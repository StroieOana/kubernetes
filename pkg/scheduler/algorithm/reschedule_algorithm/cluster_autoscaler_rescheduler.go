package reschedule_algorithm

import (
	"fmt"
	apiv1 "k8s.io/api/core/v1"
	schedulercache "k8s.io/kubernetes/pkg/scheduler/cache"
	"sort"
	resource_api "k8s.io/kubernetes/pkg/api/v1/resource"
	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/scheduler/algorithm"
	"k8s.io/kubernetes/pkg/scheduler/core"
)

// TODO improve (group by size)
// TODO create alg that searches all states and returns the best, and test this against it
// TODO group pods based on their dimension!! (otherwise bigger pods will probably not be scheduled as expected)
	// also sort nodes based on dimension to not fragment smaller nodes?
	// !!! Maybe a better logic is needed on how to choose the first node
	// TODO try iterating over pods first - bigger -> smaller - and score nodes based on this

type clusterAutoscalerRescheduler struct {
	cache schedulercache.Cache
	cachedNodeInfoMap        map[string]*schedulercache.NodeInfo
}

// podScore contains Pod and score ...
type podScore struct {
	// based on how much pod will reduce capacity imbalance for a node (bigger value is better in this case)
	score float64
	pod   *apiv1.Pod
}

func NewClusterAutoscalerRescheduleAlgorithm(cache schedulercache.Cache) algorithm.RescheduleAlgorithm {
	return &clusterAutoscalerRescheduler{
		cache: cache, cachedNodeInfoMap: make(map[string]*schedulercache.NodeInfo),
	}
}

func (rescheduler *clusterAutoscalerRescheduler) Schedule(pod *apiv1.Pod, nodeLister algorithm.NodeLister) ([]*algorithm.PodNodeAssignment, error) {
	nodes, err := nodeLister.List()
	fmt.Printf("Nr of listed nodes %d\n", len(nodes))
	glog.V(4).Infof("[Oana][Rescheduler] Nr of listed nodes%d\n", len(nodes))
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
		glog.V(4).Infof("[Oana][Rescheduler] Error while updating mapping %v", err)
		return []*algorithm.PodNodeAssignment{}, err
	}

	// Get nodeinfos from cache
	nodeInfos := make([]*schedulercache.NodeInfo, len(nodes))
	// Used to make diff
	podToNodeBefore := make(map[*apiv1.Pod]string)
	for i := range(nodes) {
		nodeName := nodes[i].Name
		nodeInfo := rescheduler.cachedNodeInfoMap[nodeName]
		nodeInfos[i] = nodeInfo
		for _, nodePod := range(nodeInfo.Pods()) {
			podToNodeBefore[nodePod] = nodeInfo.Node().Name
		}
	}

	newNodeInfos, err := rescheduler.Reschedule(pod, nodeInfos)
	if err != nil {
		fmt.Println("Error while rescheduling")
		glog.V(4).Infof("[Oana][Rescheduler] Error while rescheduling %v", err)
		return []*algorithm.PodNodeAssignment{}, err
	}

	assignments := make([]*algorithm.PodNodeAssignment, 0)
	for _, nodeInfo := range(newNodeInfos) {
		for _, pod := range(nodeInfo.Pods()) {
			beforeNode, ok := podToNodeBefore[pod]
			// We want to reassign pods that now should be scheduled on a different machine that they were running on
			// or if the pod was not assigned yet
			if !ok || nodeInfo.Node().Name != beforeNode {
				assignments = append(assignments, &algorithm.PodNodeAssignment{Pod: pod, SelectedMachine: nodeInfo.Node().Name})
			}
		}
	}
	return assignments, nil
}

func (rescheduler *clusterAutoscalerRescheduler) Reschedule(pod *apiv1.Pod, existingNodes []*schedulercache.NodeInfo) ([]*schedulercache.NodeInfo, error) {
	// Create list with all pods
	pods := make([]*apiv1.Pod, 0)
	for _, nodeInfo := range(existingNodes) {
		for _, nodePod := range(nodeInfo.Pods()) {
			if nodePod.Namespace == pod.Namespace{
				// We only reschedule pods in the same namespace
				pods = append(pods, nodePod)
			}
		}
	}
	// Also add the pod to be scheduled
	pods = append(pods, pod)
	fmt.Printf("Nr of pods to reschedule %d\n", len(pods))
	glog.V(4).Infof("[Oana][Rescheduler] Nr of pods to reschedule %d\n", len(pods))

	// Create a list with empty nodes from the list of existing nodes
	nodes := make([]*schedulercache.NodeInfo, 0)
	for _, nodeInfo := range(existingNodes) {
		//newNodeInfo := schedulercache.NewNodeInfo(nodeInfo.Pods()...)
		newNodeInfo := schedulercache.NewNodeInfo()
		newNodeInfo.SetNode(nodeInfo.Node())
		nodes = append(nodes, newNodeInfo)
	}
	fmt.Printf("Nr of nodes %d\n", len(nodes))
	glog.V(4).Infof("[Oana][Rescheduler] Nr of nodes %d\n", len(nodes))

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
	for i, nodeInfo := range(nodes) {
		if len(pods) == 0 {
			fmt.Println("No more pods to schedule")
			glog.V(4).Infof("[Oana][Rescheduler] No more pods to schedule")
			break
		}

		fmt.Printf("[Oana][Rescheduler] Trying to fill node: %s", nodeInfo.Node().Name)
		glog.V(4).Infof("[Oana][Rescheduler] Trying to fill node: %s", nodeInfo.Node().Name)
		podInfos := rescheduler.score(pods, nodeInfo)
		for len(podInfos) > 0 {
			// Order scored pods
			// TODO - do not score - iterate and find the biggest
			sort.Slice(podInfos, func(i, j int) bool { return podInfos[i].score > podInfos[j].score })

			chosenPod := podInfos[0].pod
			fmt.Printf("[Oana][Rescheduler] Chose pod with resources (%v %v) and best score: %v\n",
				resource_api.GetResourceRequest(chosenPod, apiv1.ResourceCPU),
				resource_api.GetResourceRequest(chosenPod, apiv1.ResourceMemory),
				podInfos[0].score,
			)

			glog.V(4).Infof("[Oana][Rescheduler] Chose pod with resources (%v %v) and best score: %v\n",
				resource_api.GetResourceRequest(chosenPod, apiv1.ResourceCPU),
				resource_api.GetResourceRequest(chosenPod, apiv1.ResourceMemory),
				podInfos[0].score)

			// Schedule pod with best score
			nodeInfo = nodeWithPod(nodeInfo, chosenPod)
			nodes[i] = nodeInfo

			fmt.Printf("[Oana][Rescheduler] After scheduled pod, node : requested resources (%v, %v) allocatable resources (%v %v)\n",
				nodeInfo.RequestedResource().MilliCPU, nodeInfo.RequestedResource().Memory,
				nodeInfo.AllocatableResource().MilliCPU, nodeInfo.AllocatableResource().Memory,
			)
			glog.V(4).Infof("[Oana][Rescheduler] After scheduled pod, node : requested resources (%v, %v) allocatable resources (%v %v)",
				nodeInfo.RequestedResource().MilliCPU, nodeInfo.RequestedResource().Memory,
				nodeInfo.AllocatableResource().MilliCPU, nodeInfo.AllocatableResource().Memory)

			pods = removePodFromList(chosenPod, pods)

			// Recalculate score for pods now that the node resources have changed
			podInfos = rescheduler.score(pods, nodeInfo)
		}
		fmt.Println()
	}
	// If we still have pods to schedule it means we didn't find a scheduling that includes all. error
	if len(pods) > 0 {
		return []*schedulercache.NodeInfo{}, fmt.Errorf("Couldn't find a mapping.")
	}

	// Return the new scheduling (nodes with pods)
	return nodes, nil
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

func (rescheduler *clusterAutoscalerRescheduler) score(pods []*apiv1.Pod, node *schedulercache.NodeInfo) []*podScore {
	fmt.Println("[Oana][Rescheduler] \tScoring pods:")
	glog.V(4).Infof("[Oana][Rescheduler] \tScoring pods:")
	podInfos := make([]*podScore, 0)
	// TODO parallelize
	for _, pod := range pods {
		// We drop pods that can't be scheduled on the node.
		if ! podFitsNode(pod, node) {
			fmt.Println("\t[Oana][Rescheduler] \tPod does not fit node, Can't schedule it on this node")
			glog.V(4).Infof("[Oana][Rescheduler] \tPod does not fit node, Can't schedule it on this node")
			continue
		}
		podInfos = append(podInfos, &podScore{
			score: getPodOnNodeImbalanceScore(pod, node),
			pod:   pod,
		})
	}
	return podInfos
}

