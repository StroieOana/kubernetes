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
	clientset "k8s.io/client-go/kubernetes"
)

type defaultRescheduler struct {
	cache schedulercache.Cache
	cachedNodeInfoMap        map[string]*schedulercache.NodeInfo
	client clientset.Interface
}

// nodeScore contains node and score ...
type nodeInfoScore struct {
	// based on how much pod will reduce capacity imbalance for a node (bigger value is better in this case)
	score float64
	nodeInfo   *schedulercache.NodeInfo
}

func NewDefaultRescheduleAlgorithm(client clientset.Interface, cache schedulercache.Cache) algorithm.RescheduleAlgorithm {
	return &defaultRescheduler{
		cache: cache, cachedNodeInfoMap: make(map[string]*schedulercache.NodeInfo), client: client,
	}
}

func (rescheduler *defaultRescheduler) GetClient()  clientset.Interface {
	return rescheduler.client
}

func (rescheduler *defaultRescheduler) Schedule(pod *apiv1.Pod, nodeLister algorithm.NodeLister) ([]*algorithm.PodNodeAssignment, error) {
	nodes, err := nodeLister.List()
	//fmt.Printf("Nr of listed nodes %d\n", len(nodes))
	glog.Infof("[Oana][Rescheduler] Nr of listed nodes%d\n", len(nodes))
	if err != nil {
		return []*algorithm.PodNodeAssignment{}, err
	}
	if len(nodes) == 0 {
		return []*algorithm.PodNodeAssignment{}, core.ErrNoNodesAvailable
	}

	// Used for all fit and priority funcs.
	err = rescheduler.cache.UpdateNodeNameToInfoMap(rescheduler.cachedNodeInfoMap)
	if err != nil {
		//fmt.Println("Error while updating mapping")
		glog.Infof("[Oana][Rescheduler] Error while updating mapping %v", err)
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
		//fmt.Println("Error while rescheduling")
		glog.Infof("[Oana][Rescheduler] Error while rescheduling %v", err)
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


func (rescheduler *defaultRescheduler) Reschedule(pod *apiv1.Pod, existingNodes []*schedulercache.NodeInfo) ([]*schedulercache.NodeInfo, error) {
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
	//fmt.Printf("Nr of pods to reschedule %d\n", len(pods))
	glog.Infof("[Oana][Rescheduler] Nr of pods to reschedule %d\n", len(pods))

	// Create a list with empty nodes from the list of existing nodes
	nodesInfos := make([]*schedulercache.NodeInfo, 0)
	for _, nodeInfo := range(existingNodes) {
		podsDifferentNamespace := make([]*apiv1.Pod, 0)
		for _, p := range(nodeInfo.Pods()) {
			// The pods that are not in the same namespace will not be reschedule.
			// We need to leave them on the node (to be taken into consideration when checking resources)
			if p.Namespace != pod.Namespace {
				podsDifferentNamespace = append(podsDifferentNamespace, p)
			}
		}
		newNodeInfo := schedulercache.NewNodeInfo(podsDifferentNamespace...)
		newNodeInfo.SetNode(nodeInfo.Node())
		nodesInfos = append(nodesInfos, newNodeInfo)
	}
	//fmt.Printf("Nr of nodes %d\n", len(nodesInfos))
	glog.Infof("[Oana][Rescheduler] Nr of nodes %d\n", len(nodesInfos))

	// Taken from https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/estimator/binpacking_estimator.go#L60
	// nodeWithPod function returns NodeInfo, which is a copy of nodeInfo argument with an additional pod scheduled on it.
	nodeWithPod := func(nodeInfo *schedulercache.NodeInfo, pod *apiv1.Pod) *schedulercache.NodeInfo {
		podsOnNode := nodeInfo.Pods()
		podsOnNode = append(podsOnNode, pod)
		newNodeInfo := schedulercache.NewNodeInfo(podsOnNode...)
		newNodeInfo.SetNode(nodeInfo.Node())
		return newNodeInfo
	}

	// Sort pods by dimension (multiply resources) - we want to schedule biggest pods first
	sort.Slice(pods, func(i, j int) bool { return multiplyPodResourceReq(pods[i]) > multiplyPodResourceReq(pods[j])})
	for _, pod := range(pods) {
		//fmt.Printf("[Oana][Rescheduler] Trying to reschdule pod: %s (%v, %v)", pod.ObjectMeta.UID,
		//	resource_api.GetResourceRequest(pod, apiv1.ResourceCPU),
		//	resource_api.GetResourceRequest(pod, apiv1.ResourceMemory))
		nodeInfoScores := rescheduler.score(pod, nodesInfos)
		if len(nodeInfoScores) == 0 {
			// Pod does not fit on any node
			return []*schedulercache.NodeInfo{}, fmt.Errorf("Pod does not fit on any node")
		}
		biggestScore := nodeInfoScores[0].score
		biggestScoreNodeInfo := nodeInfoScores[0].nodeInfo
		for _, nodeInfoScore := range(nodeInfoScores){
			if nodeInfoScore.score > biggestScore {
				biggestScore = nodeInfoScore.score
				biggestScoreNodeInfo = nodeInfoScore.nodeInfo
			}
		}
		// Update the node info with the new pod
		for i, nodeInfo := range nodesInfos {
			if nodeInfo == biggestScoreNodeInfo {
				nodeInfo := nodeWithPod(nodeInfo, pod)
				nodesInfos[i] = nodeInfo
			}
		}
	}

	// Return the new scheduling (nodes with pods)
	return nodesInfos, nil
}

func (rescheduler *defaultRescheduler) score(pod *apiv1.Pod, nodeInfos []*schedulercache.NodeInfo) []*nodeInfoScore {
	//fmt.Println("[Oana][Rescheduler] \tScoring nodes:")
	glog.Infof("[Oana][Rescheduler] \tScoring nodes:")
	nodeInfoScores := make([]*nodeInfoScore, 0)
	// TODO parallelize
	for _, nodeInfo := range nodeInfos {
		// We drop pods that can't be scheduled on the node.
		if ! podFitsNode(pod, nodeInfo) {
			continue
		}
		// TODO - nicer way of calculating this. Maybe more generic like the priorities
		// It is more important to keep the nodes low loaded than perfectly balanced
		nodeScore := getPodOnNodeImbalanceScore(pod, nodeInfo) + 2*getPodOnNodeFreenessScore(pod, nodeInfo)
		// Nodes that have no resources allocated yet are preferred
		if noPodsOnNode(nodeInfo) {
			// TODO prettier way
			nodeScore += 1
		}
		nodeInfoScores = append(nodeInfoScores, &nodeInfoScore{
			score: nodeScore,
			nodeInfo:   nodeInfo,
		})
	}
	return nodeInfoScores
}

func multiplyPodResourceReq(pod *apiv1.Pod) int64 {
	return resource_api.GetResourceRequest(pod, apiv1.ResourceCPU) *resource_api.GetResourceRequest(pod, apiv1.ResourceMemory)
}

func noPodsOnNode(nodeInfo *schedulercache.NodeInfo) bool {
	return len(nodeInfo.Pods()) == 0
}
