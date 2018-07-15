package reschedule_algorithm

import (
	"fmt"
	apiv1 "k8s.io/api/core/v1"
	schedulercache "k8s.io/kubernetes/pkg/scheduler/cache"
	resource_api "k8s.io/kubernetes/pkg/api/v1/resource"
	"github.com/golang/glog"
	"math"
)




func podFitsNode(pod *apiv1.Pod, nodeInfo *schedulercache.NodeInfo) bool {
	podCpu := resource_api.GetResourceRequest(pod, apiv1.ResourceCPU)
	podMemory:= resource_api.GetResourceRequest(pod, apiv1.ResourceMemory)

	nodeCPURequested := nodeInfo.RequestedResource().MilliCPU
	nodeMemoryRequested := nodeInfo.RequestedResource().Memory

	nodeCPUAllocatable := nodeInfo.AllocatableResource().MilliCPU
	nodeMemoryAllocatable := nodeInfo.AllocatableResource().Memory

	fits := nodeCPURequested + podCpu <= nodeCPUAllocatable && nodeMemoryRequested + podMemory <= nodeMemoryAllocatable

	fmt.Printf("\t[Oana][Rescheduler] \tPod %v(%v, %v) fits on node %v (%v/%v %v/%v): %v",
		pod.Name, podCpu, podMemory, nodeInfo.Node().Name,
		nodeCPURequested, nodeCPUAllocatable, nodeMemoryRequested, nodeMemoryAllocatable, fits)
	glog.V(4).Infof("[Oana][Rescheduler] \tPod %v(%v, %v) fits on node %v (%v/%v %v/%v): %v",
		pod.Name, podCpu, podMemory, nodeInfo.Node().Name,
		nodeCPURequested, nodeCPUAllocatable, nodeMemoryRequested, nodeMemoryAllocatable, fits)

	return fits
}


func getPodOnNodeImbalanceScore(pod *apiv1.Pod, nodeInfo *schedulercache.NodeInfo) float64 {
	// How much the pod improves the capacity imbalance
	// = - (imbalance_after - imbalance_before)
	// imbalance = |allocated_cpu_percent- allocated_mem_percent|
	requestedCPU := resource_api.GetResourceRequest(pod, apiv1.ResourceCPU)
	requestedMem := resource_api.GetResourceRequest(pod, apiv1.ResourceMemory)

	// TODO  diff between allocatable and capacyti
	freeCPU := nodeInfo.AllocatableResource().MilliCPU - nodeInfo.RequestedResource().MilliCPU
	capacityCPU := nodeInfo.AllocatableResource().MilliCPU

	freeMemory := nodeInfo.AllocatableResource().Memory - nodeInfo.RequestedResource().Memory
	capacityMem := nodeInfo.AllocatableResource().Memory

	imbalanceBefore := math.Abs(fractionOfCapacity(freeCPU, capacityCPU) - fractionOfCapacity(freeMemory, capacityMem))
	imbalanceAfter := math.Abs(fractionOfCapacity(freeCPU - requestedCPU, capacityCPU) - fractionOfCapacity(freeMemory - requestedMem, capacityMem))

	score := - (imbalanceAfter - imbalanceBefore)
	fmt.Printf("\t[Oana][Rescheduler] \tPod %v(%v %v) on node %s has imbalance score %v\n",
		pod.Name,
		resource_api.GetResourceRequest(pod, apiv1.ResourceCPU),
		resource_api.GetResourceRequest(pod, apiv1.ResourceMemory),
		nodeInfo.Node().Name,
		score,
	)
	glog.V(4).Infof("[Oana][Rescheduler] \tPod %v(%v %v) on node %s has imbalance score %v\n",
		pod.Name,
		resource_api.GetResourceRequest(pod, apiv1.ResourceCPU),
		resource_api.GetResourceRequest(pod, apiv1.ResourceMemory),
		nodeInfo.Node().Name,
		score,
	)

	return score
}

func fractionOfCapacity(requested, capacity int64) float64 {
	if capacity == 0 {
		// TODO we should not be in this situation
		return 1
	}
	return float64(requested) / float64(capacity)
}

// TODO - make this score in the same order as the imbalance
func getPodOnNodeFreenessScore(pod *apiv1.Pod, nodeInfo *schedulercache.NodeInfo) float64 {
	// How much the pod adds load to node - we prefer low loaded nodes on default flow
	// = cpu percent free after + mem percent free after
	requestedCPU := resource_api.GetResourceRequest(pod, apiv1.ResourceCPU)
	requestedMem := resource_api.GetResourceRequest(pod, apiv1.ResourceMemory)

	// TODO  diff between allocatable and capacyti
	freeCPU := nodeInfo.AllocatableResource().MilliCPU - nodeInfo.RequestedResource().MilliCPU
	capacityCPU := nodeInfo.AllocatableResource().MilliCPU

	freeMemory := nodeInfo.AllocatableResource().Memory - nodeInfo.RequestedResource().Memory
	capacityMem := nodeInfo.AllocatableResource().Memory

	score := (fractionOfCapacity(freeCPU - requestedCPU, capacityCPU) + fractionOfCapacity(freeMemory - requestedMem, capacityMem))/2

	fmt.Printf("\t[Oana][Rescheduler] \tPod %v(%v %v) on node %s has freeness score %v\n",
		pod.Name,
		resource_api.GetResourceRequest(pod, apiv1.ResourceCPU),
		resource_api.GetResourceRequest(pod, apiv1.ResourceMemory),
		nodeInfo.Node().Name,
		score,
	)
	glog.V(4).Infof("[Oana][Rescheduler] \tPod %v(%v %v) on node %s has score %v\n",
		pod.Name,
		resource_api.GetResourceRequest(pod, apiv1.ResourceCPU),
		resource_api.GetResourceRequest(pod, apiv1.ResourceMemory),
		nodeInfo.Node().Name,
		score,
	)

	return score
}