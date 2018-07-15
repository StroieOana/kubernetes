package main

import (
	"fmt"
	"time"
	"k8s.io/kubernetes/pkg/scheduler/algorithm/reschedule_algorithm"
	schedulercache "k8s.io/kubernetes/pkg/scheduler/cache"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	resource_api "k8s.io/kubernetes/pkg/api/v1/resource"
	schedulertesting "k8s.io/kubernetes/pkg/scheduler/testing"
)

func main() {
	fmt.Println("hello world")
	//runClusterAutoscalerExample()
	runDefaultExample()
}

func runClusterAutoscalerExample() {
	/*
	2 nodes: N1 (2CPUs, 4GB) and N2 (4CPUs, 4GB)
		N1 name : disertatie
		N2 name : dissertation
	2 types of pods with limit requests:
		P1 1.5CPU and 1GB
		P2 0.75CPU and 1GB

	 */

	pod1_1 := newResourcePod(schedulercache.Resource{MilliCPU: 1500, Memory: 1000})
	pod1_1.Spec.NodeName = "Node1 (1500, 3500)"
	pod1_1.ObjectMeta.UID = "pod1_1"
	pod1_2 := newResourcePod(schedulercache.Resource{MilliCPU: 1500, Memory: 1000})

	pod2_1 := newResourcePod(schedulercache.Resource{MilliCPU: 750, Memory: 1000})
	pod2_1.Spec.NodeName = "Node2 (3000, 2000)"
	pod2_1.ObjectMeta.UID = "pod2_1"
	pod2_2 := newResourcePod(schedulercache.Resource{MilliCPU: 750, Memory: 1000})
	//pod2_2.Spec.NodeName = "Node2 (3000, 2000)"
	pod2_2.Spec.NodeName = "Node1 (1500, 3500)"
	//pod2_2.ObjectMeta.UID = "pod2_2"
	pod2_2.ObjectMeta.UID = "pod1_2"

	node1 := v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "Node1 (1500, 3500)", },
		Status: v1.NodeStatus{Allocatable: makeAllocatableResources(1500, 3500)}}

	node2 := v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "Node2 (3000, 2000)", },
		Status: v1.NodeStatus{Allocatable: makeAllocatableResources(3000, 2000)}}

	cache := schedulercache.New(time.Duration(0), wait.NeverStop)

	err := cache.AddNode(&node1)
	fmt.Printf("err 1 %v", err)
	err = cache.AddNode(&node2)
	fmt.Printf("err 2 %v", err)

	err = cache.AddPod(pod1_1)
	fmt.Printf("err 3 %v", err)
	//cache.AddPod(pod1_2)
	err = cache.AddPod(pod2_1)
	fmt.Printf("err 4 %v", err)
	err = cache.AddPod(pod2_2)
	fmt.Printf("err 5 %v", err)
	rescheduler := reschedule_algorithm.NewClusterAutoscalerRescheduleAlgorithm(cache)
	nodeLister := schedulertesting.FakeNodeLister([]*v1.Node{&node1, &node2})

	assignments, _ := rescheduler.Schedule(pod1_2, nodeLister)
	for _, assignment := range(assignments) {
		fmt.Printf("Node %s - pod %s (%v %v)", assignment.SelectedMachine,
			assignment.Pod.ObjectMeta.UID,
			resource_api.GetResourceRequest(assignment.Pod, v1.ResourceCPU),
			resource_api.GetResourceRequest(assignment.Pod, v1.ResourceMemory))
		fmt.Println()

	}
	fmt.Println("Reschedule ended")
}


func runDefaultExample() {
	/*
	2 nodes: N1 (2CPUs, 4GB) and N2 (4CPUs, 4GB)
		N1 name : disertatie
		N2 name : dissertation
	2 types of pods with limit requests:
		P1 0.5CPU and 1GB
		P2 2CPU and 1GB

	 P1, P1, P2
	 */

	pod1_1 := newResourcePod(schedulercache.Resource{MilliCPU: 500, Memory: 1000})
	pod1_1.Spec.NodeName = "Node1 (2000, 4000)"
	pod1_1.ObjectMeta.UID = "pod1_1"
	pod1_2 := newResourcePod(schedulercache.Resource{MilliCPU: 500, Memory: 1000})
	pod1_2.Spec.NodeName = "Node2 (4000, 4000)"
	pod1_2.ObjectMeta.UID = "pod1_2"

	pod2_1 := newResourcePod(schedulercache.Resource{MilliCPU: 2000, Memory: 1000})
	pod2_1.ObjectMeta.UID = "pod2_1"

	node1 := v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "Node1 (2000, 4000)", },
		Status: v1.NodeStatus{Allocatable: makeAllocatableResources(2000, 4000)}}

	node2 := v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "Node2 (4000, 4000)", },
		Status: v1.NodeStatus{Allocatable: makeAllocatableResources(4000, 4000)}}

	cache := schedulercache.New(time.Duration(0), wait.NeverStop)

	err := cache.AddNode(&node1)
	fmt.Printf("err 1 %v", err)
	err = cache.AddNode(&node2)
	fmt.Printf("err 2 %v", err)

	err = cache.AddPod(pod1_1)
	fmt.Printf("err 3 %v", err)
	//cache.AddPod(pod1_2)
	err = cache.AddPod(pod1_2)
	fmt.Printf("err 5 %v", err)
	rescheduler := reschedule_algorithm.NewDefaultRescheduleAlgorithm(cache)
	nodeLister := schedulertesting.FakeNodeLister([]*v1.Node{&node1, &node2})

	assignments, _ := rescheduler.Schedule(pod2_1, nodeLister)
	for _, assignment := range(assignments) {
		fmt.Printf("Node %s - pod %s (%v %v)", assignment.SelectedMachine,
			assignment.Pod.ObjectMeta.UID,
			resource_api.GetResourceRequest(assignment.Pod, v1.ResourceCPU),
			resource_api.GetResourceRequest(assignment.Pod, v1.ResourceMemory))
		fmt.Println()

	}
	fmt.Println("Reschedule ended")
}

func makeResources(milliCPU, memory int64) v1.NodeResources {
	return v1.NodeResources{
		Capacity: v1.ResourceList{
			v1.ResourceCPU:              *resource.NewMilliQuantity(milliCPU, resource.DecimalSI),
			v1.ResourceMemory:           *resource.NewQuantity(memory, resource.BinarySI),
			//v1.ResourcePods:             *resource.NewQuantity(pods, resource.DecimalSI),
		},
	}
}
func makeAllocatableResources(milliCPU, memory int64) v1.ResourceList {
	return v1.ResourceList{
		v1.ResourceCPU:              *resource.NewMilliQuantity(milliCPU, resource.DecimalSI),
		v1.ResourceMemory:           *resource.NewQuantity(memory, resource.BinarySI),
	}
}

func newResourcePod(usage ...schedulercache.Resource) *v1.Pod {
	containers := []v1.Container{}
	for _, req := range usage {
		containers = append(containers, v1.Container{
			Resources: v1.ResourceRequirements{Requests: req.ResourceList()},
		})
	}
	return &v1.Pod{
		Spec: v1.PodSpec{
			Containers: containers,
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}
}

