package reschedule_algorithm

import (
	"fmt"
	apiv1 "k8s.io/api/core/v1"
	schedulercache "k8s.io/kubernetes/pkg/scheduler/cache"
	"sort"
	"math"
	resource_api "k8s.io/kubernetes/pkg/api/v1/resource"
	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/scheduler/algorithm"
	"k8s.io/kubernetes/pkg/scheduler/core"
)

// TODO improve (group by size)
// TODO create alg that searches all states and returns the best, and test this against it

type RescheduleAlgorithm interface {
	Reschedule(pod *apiv1.Pod, existingNodes []*schedulercache.NodeInfo) []*schedulercache.NodeInfo
}

type rescheduler struct {
	cache schedulercache.Cache
	cachedNodeInfoMap        map[string]*schedulercache.NodeInfo
}

// podInfo contains Pod and score ...
type podInfo struct {
	// based on how much pod will reduce capacity imbalance for a node (bigger is better in this case)
	score float64
	pod   *apiv1.Pod
}

func NewRescheduleAlgorithmWithCache(cache schedulercache.Cache) algorithm.RescheduleAlgorithm {
	return &rescheduler{
		cache: cache, cachedNodeInfoMap: make(map[string]*schedulercache.NodeInfo),
	}
}


func NewRescheduleAlgorithm() algorithm.RescheduleAlgorithm {
	return &rescheduler{}
}


func (rescheduler *rescheduler) Schedule(pod *apiv1.Pod, nodeLister algorithm.NodeLister) ([]*algorithm.PodNodeAssignment, error) {
	// TODO compute and return diff
	//if err := core.podPassesBasicChecks(pod, g.pvcLister); err != nil {
	//	return []*algorithm.PodNodeAssignment{}, err
	//}

	nodes, err := nodeLister.List()
	fmt.Printf("Nr of listed nodes %d\n", len(nodes))
	if err != nil {
		return []*algorithm.PodNodeAssignment{}, err
	}
	if len(nodes) == 0 {
		return []*algorithm.PodNodeAssignment{}, core.ErrNoNodesAvailable
	}
	// Used for all fit and priority funcs.
	err = rescheduler.cache.UpdateNodeNameToInfoMap(rescheduler.cachedNodeInfoMap)
	if err != nil {
		fmt.Println("Error while updating mapping")
		return []*algorithm.PodNodeAssignment{}, err
	}

	nodeInfos := make([]*schedulercache.NodeInfo, len(nodes))
	for i := range(nodes) {
		nodeName := nodes[i].Name
		nodeInfos[i] = rescheduler.cachedNodeInfoMap[nodeName]
	}

	newNodeInfos := rescheduler.Reschedule(pod, nodeInfos)

	// TODO diff

	assignments := make([]*algorithm.PodNodeAssignment, 0)
	for _, nodeInfo := range(newNodeInfos) {
		for _, pod := range(nodeInfo.Pods()) {
			assignments = append(assignments, &algorithm.PodNodeAssignment{Pod: pod, SelectedMachine: nodeInfo.Node().Name})
		}
	}
	return assignments, nil
}

// TODO find a way of getting the current mapping
func (rescheduler *rescheduler) Reschedule(pod *apiv1.Pod, existingNodes []*schedulercache.NodeInfo) []*schedulercache.NodeInfo {
	// TODO check if each node already has its pods on them
	// Create list with all pods
	pods := make([]*apiv1.Pod, 0)
	for _, nodeInfo := range(existingNodes) {
		pods = append(pods, nodeInfo.Pods()...)
	}
	// Also add the pod to be scheduled
	pods = append(pods, pod)
	glog.V(4).Infof("Nr of pods to reschedule %d\n", len(pods))
	fmt.Printf("Nr of pods to reschedule %d\n", len(pods))

	// Create a list with empty nodes from the list of existing nodes
	nodes := make([]*schedulercache.NodeInfo, 0)
	for _, nodeInfo := range(existingNodes) {
		//newNodeInfo := schedulercache.NewNodeInfo(nodeInfo.Pods()...)
		newNodeInfo := schedulercache.NewNodeInfo()
		newNodeInfo.SetNode(nodeInfo.Node())
		nodes = append(nodes, newNodeInfo)
	}
	fmt.Printf("Nr of nodes %d\n", len(nodes))

	// Taken from https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/estimator/binpacking_estimator.go#L60
	// nodeWithPod function returns NodeInfo, which is a copy of nodeInfo argument with an additional pod scheduled on it.
	nodeWithPod := func(nodeInfo *schedulercache.NodeInfo, pod *apiv1.Pod) *schedulercache.NodeInfo {
		podsOnNode := nodeInfo.Pods()
		podsOnNode = append(podsOnNode, pod)
		newNodeInfo := schedulercache.NewNodeInfo(podsOnNode...)
		newNodeInfo.SetNode(nodeInfo.Node())
		return newNodeInfo
	}

	// TODO early quit if not enough resources
	// TODO sort the nodes based on some heuristic (based on this we will fill those nodes first)
	// TODO group pods based on their dimension!! (otherwise bigger pods will probably not be scheduled as expected)
		// also sort nodes based on dimension to not fragment smaller nodes?
		// Maybe a better logic is needed on how to choose the first node
	// TODO try iterating over pods first - bigger -> smaller - and score nodes based on this

	for i, nodeInfo := range(nodes) {
		if len(pods) == 0 {
			fmt.Println("No more pods to schedule")
			break
		}

		fmt.Printf("Trying to fill node: %s\n", nodeInfo.Node().Name)
		podInfos := rescheduler.score(pods, nodeInfo)
		for len(podInfos) > 0 {
			// Order scored pods
			sort.Slice(podInfos, func(i, j int) bool { return podInfos[i].score > podInfos[j].score })

			chosenPod := podInfos[0].pod
			fmt.Printf("Chose pod with resources (%v %v) and best score: %v\n",
				resource_api.GetResourceRequest(chosenPod, apiv1.ResourceCPU),
				resource_api.GetResourceRequest(chosenPod, apiv1.ResourceMemory),
				podInfos[0].score,
			)

			// Schedule pod with best score
			nodeInfo = nodeWithPod(nodeInfo, chosenPod)
			nodes[i] = nodeInfo

			fmt.Printf("After scheduled pod, node : requested resources (%v, %v) allocatable resources (%v %v)",
				nodeInfo.RequestedResource().MilliCPU, nodeInfo.RequestedResource().Memory,
				nodeInfo.AllocatableResource().MilliCPU, nodeInfo.AllocatableResource().Memory,
			)

			pods = removePodFromList(chosenPod, pods)

			// Recalculate score for pods now that the node resources have changed
			podInfos = rescheduler.score(pods, nodeInfo)
		}
		fmt.Println()
	}
	// Return the new scheduling (nodes with pods)
	return nodes
}

func removePodFromList(pod *apiv1.Pod, pods []*apiv1.Pod) []*apiv1.Pod {
	index := -1
	for i, p := range(pods) {
		if p == pod {
			index = i
			break
		}
	}
	newPodsList := append(pods[:index], pods[index+1:]...)
	return newPodsList
}

func (rescheduler *rescheduler) score(pods []*apiv1.Pod, node *schedulercache.NodeInfo) []*podInfo {
	fmt.Println("\tScoring pods:")
	podInfos := make([]*podInfo, 0)
	for _, pod := range pods {
		// We drop pods that can't be scheduled on the node.
		if ! podFitsNode(pod, node) {
			fmt.Println("\tPod does not fit node, Can't schedule it on this node")
			continue
		}
		podInfos = append(podInfos, &podInfo{
			score: rescheduler.getPodScore(pod, node),
			pod:   pod,
		})
	}
	return podInfos
}

func podFitsNode(pod *apiv1.Pod, nodeInfo *schedulercache.NodeInfo) bool {
	podCpu := resource_api.GetResourceRequest(pod, apiv1.ResourceCPU)
	podMemory:= resource_api.GetResourceRequest(pod, apiv1.ResourceMemory)

	nodeCPURequested := nodeInfo.RequestedResource().MilliCPU
	nodeMemoryRequested := nodeInfo.RequestedResource().Memory

	nodeCPUAllocatable := nodeInfo.AllocatableResource().MilliCPU
	nodeMemoryAllocatable := nodeInfo.AllocatableResource().Memory

	fits := nodeCPURequested + podCpu <= nodeCPUAllocatable && nodeMemoryRequested + podMemory <= nodeMemoryAllocatable

	fmt.Printf("\tPod(%v, %v) fits on node (%v/%v %v/%v): %v",
		podCpu, podMemory, nodeCPURequested, nodeCPUAllocatable, nodeMemoryRequested, nodeMemoryAllocatable, fits)

	return fits
}

func (rescheduler *rescheduler) getPodScore(pod *apiv1.Pod, nodeInfo *schedulercache.NodeInfo) float64 {
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
	fmt.Printf("\tPod (%v %v) on node %s has score %v\n",
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
