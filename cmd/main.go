/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"net/http"
	"strings"

	"github.com/golang/glog"

	"github.com/julienschmidt/httprouter"
	"k8s.io/api/core/v1"
	schedulingapi "k8s.io/kubernetes/pkg/scheduler/api"
)

var httpAddr, apiPrefix, prioritiesPrefix string

func init() {
	flag.StringVar(&apiPrefix, "api-prefix", "/my_scheduler_extension", "The api prefix path, e.g. /scheduler_extension")
	flag.StringVar(&prioritiesPrefix, "priorities-prefix", "/my_new_priorities", "The priorities prefix path, e.g. /a_new_priorities")
	flag.StringVar(&httpAddr, "http-addr", ":80", "The ip:port address the extender endpoint binds to, if <ip> is missing it bings to localhost")
	flag.Set("logtostderr", "true")
	flag.Set("stderrthreshold", "WARNING")
	flag.Parse()
	if !strings.Contains(httpAddr, ":") {
		httpAddr = ":" + httpAddr
		glog.Warningf("the -http-addr flag value was missing a `:`, it was automatically added -> %v", httpAddr)
	}
	if !strings.HasPrefix(apiPrefix, "/") {
		apiPrefix = "/" + apiPrefix
		glog.Warningf("the -api-prefix flag value was missing a `/`, it was automatically added -> %v", apiPrefix)
	}
	if !strings.HasPrefix(prioritiesPrefix, "/") {
		prioritiesPrefix = "/" + prioritiesPrefix
		glog.Warningf("the -priorities-prefix flag value was missing a `/`, it was automatically added -> %v", prioritiesPrefix)
	}
	prioritiesPrefix = apiPrefix + prioritiesPrefix
}

// PrioritizeMethod defines the name of the priority. this name should much the one specified in the
// scheduler config file, since it is part of the URL to be called by the scheduler
type PrioritizeMethod struct {
	Name string
	Func func(pod v1.Pod, nodes []v1.Node) (*schedulingapi.HostPriorityList, error)
}

// Handler takes as input the pod and a list of nodes and returns a hostPriority list
func (p PrioritizeMethod) Handler(args schedulingapi.ExtenderArgs) (*schedulingapi.HostPriorityList, error) {
	return p.Func(*args.Pod, args.Nodes.Items)
}

// ImagePriority defines the name and method for a priotity
// for each priority we should add a PrioritizeMethod
var ImagePriority = PrioritizeMethod{
	Name: "image_score",
	Func: func(pod v1.Pod, nodes []v1.Node) (*schedulingapi.HostPriorityList, error) {
		var priorityList schedulingapi.HostPriorityList
		priorityList = make([]schedulingapi.HostPriority, len(nodes))
		for i, node := range nodes {
			score := nodeHasImage(pod, node.Status.Images, node.Name)
			priorityList[i] = schedulingapi.HostPriority{
				Host:  node.Name,
				Score: int(score),
			}
			glog.V(6).Infof("node %v has priority score of %v for pod %v\n", node.Name, score, pod.Name)
		}
		return &priorityList, nil
	},
}

// we return the count of found container images of the pod on the node
func nodeHasImage(pod v1.Pod, nodeImages []v1.ContainerImage, nodeName string) uint32 {
	if len(nodeImages) == 0 {
		return 0
	}
	var count uint32
	for _, ctnr := range pod.Spec.Containers {
		var shouldBreak bool
		for _, img := range nodeImages {
			if shouldBreak {
				break
			}
			for _, imgName := range img.Names {
				if strings.Contains(imgName, ctnr.Image) {
					// we use the heuristic approach of `strings.Contains` since the missing tag `latest` in the pod's container may be added in the node image
					count++
					glog.V(6).Infof("nodeImage %v matches container Image %v on node %v\n", imgName, ctnr.Image, nodeName)
					shouldBreak = true
					break
				}
			}
		}
	}
	return count
}

// making sure the request has a body
func checkRequestBody(w http.ResponseWriter, r *http.Request) bool {
	if r.Body == nil {
		http.Error(w, "the request is empty, expecting a pod and a list of nodes!", 400)
		return false
	}
	return true
}

// PrioritizeRoute returns an http handle
func PrioritizeRoute(priorityMethod PrioritizeMethod) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		if !checkRequestBody(w, r) {
			glog.Warning("received empty request!")
			return
		}
		var buf bytes.Buffer
		body := io.TeeReader(r.Body, &buf)
		glog.V(8).Infof("detailed info: %v  ExtenderArgs = %v\n", priorityMethod.Name, buf.String())

		var extenderArgs schedulingapi.ExtenderArgs
		var hostPriorityList *schedulingapi.HostPriorityList

		if err := json.NewDecoder(body).Decode(&extenderArgs); err != nil {
			panic(err)
		}

		if list, err := priorityMethod.Handler(extenderArgs); err != nil {
			panic(err)
		} else {
			hostPriorityList = list
		}

		if resultBody, err := json.Marshal(hostPriorityList); err != nil {
			panic(err)
		} else {
			glog.V(4).Infof("priorityMethod %v, hostPriorityList = %v\n ", priorityMethod.Name, string(resultBody))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(resultBody)
		}
	}
}

// AddPrioritizeFunc adding the route path to the router
func AddPrioritizeFunc(router *httprouter.Router, priorityMethod PrioritizeMethod) {
	path := prioritiesPrefix + "/" + priorityMethod.Name
	router.POST(path, PrioritizeRoute(priorityMethod))
	glog.V(2).Infof("added priority method: %v at path: %v\n", priorityMethod.Name, path)
}

func main() {

	router := httprouter.New()

	priorities := []PrioritizeMethod{ImagePriority}
	for _, p := range priorities {
		AddPrioritizeFunc(router, p)
	}

	glog.V(0).Infof("scheduler extender http server started on the address %v\n", httpAddr)
	if err := http.ListenAndServe(httpAddr, router); err != nil {
		glog.Fatal(err)
	}
}
