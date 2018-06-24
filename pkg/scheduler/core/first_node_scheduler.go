package core

import (
	"sync"

	"k8s.io/api/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/kubernetes/pkg/scheduler/algorithm"
	schedulercache "k8s.io/kubernetes/pkg/scheduler/cache"
	"k8s.io/kubernetes/pkg/scheduler/volumebinder"
	"github.com/golang/glog"
	//utillog "k8s.io/kubernetes/pkg/kubelet/kubeletconfig/util/log"
	"k8s.io/kubernetes/pkg/scheduler/core/equivalence"
)

type firstNodeScheduler struct {
	cache                    schedulercache.Cache
	equivalenceCache         *equivalence.Cache
	schedulingQueue          SchedulingQueue
	predicates               map[string]algorithm.FitPredicate
	priorityMetaProducer     algorithm.PriorityMetadataProducer
	predicateMetaProducer    algorithm.PredicateMetadataProducer
	prioritizers             []algorithm.PriorityConfig
	extenders                []algorithm.SchedulerExtender
	lastNodeIndexLock        sync.Mutex
	lastNodeIndex            uint64
	alwaysCheckAllPredicates bool
	cachedNodeInfoMap        map[string]*schedulercache.NodeInfo
	volumeBinder             *volumebinder.VolumeBinder
	pvcLister                corelisters.PersistentVolumeClaimLister
}


// Schedule schedules the given pod to the first node from the node list.
func (g *firstNodeScheduler) Schedule(pod *v1.Pod, nodeLister algorithm.NodeLister) (string, error) {
	if err := podPassesBasicChecks(pod, g.pvcLister); err != nil {
		return "", err
	}

	nodes, err := nodeLister.List()
	if err != nil {
		return "", err
	}
	if len(nodes) == 0 {
		return "", ErrNoNodesAvailable
	}

	//fmt.Printf("[OANA] Available nodes: %v. Schedling on %s", nodes, nodes[0].Name)
	//utillog.Infof("attempting to download ConfigMap with UID %q", uid)
	glog.V(4).Infof("[OANA] Available nodes: %v. Schedling on %s", nodes, nodes[0].Name)

	return nodes[0].Name, nil
}


// preempt finds nodes with pods that can be preempted to make room for "pod" to
// schedule. For now we don't have an implementation for this.
func (g *firstNodeScheduler) Preempt(pod *v1.Pod, nodeLister algorithm.NodeLister, scheduleErr error) (*v1.Node, []*v1.Pod, []*v1.Pod, error) {
	return nil, nil, nil, nil
}

// Prioritizers returns a slice containing all the scheduler's priority
// functions and their config. It is exposed for testing only.
// Returning empty list for now
func (g *firstNodeScheduler) Prioritizers() []algorithm.PriorityConfig {
	return []algorithm.PriorityConfig{}
}

// Predicates returns a map containing all the scheduler's predicate
// functions. It is exposed for testing only.
// Returning empty list for now
func (g *firstNodeScheduler) Predicates() map[string]algorithm.FitPredicate {
	return map[string]algorithm.FitPredicate{}
}

// NewFirstNodeScheduler creates a firstNodeScheduler object.
// TODO(oanas): no need for all params
func NewFirstNodeScheduler(
	cache schedulercache.Cache,
	eCache *equivalence.Cache,
	podQueue SchedulingQueue,
	predicates map[string]algorithm.FitPredicate,
	predicateMetaProducer algorithm.PredicateMetadataProducer,
	prioritizers []algorithm.PriorityConfig,
	priorityMetaProducer algorithm.PriorityMetadataProducer,
	extenders []algorithm.SchedulerExtender,
	volumeBinder *volumebinder.VolumeBinder,
	pvcLister corelisters.PersistentVolumeClaimLister,
	alwaysCheckAllPredicates bool) algorithm.ScheduleAlgorithm {
		return &firstNodeScheduler{
			cache:                    cache,
			equivalenceCache:         eCache,
			schedulingQueue:          podQueue,
			predicates:               predicates,
			predicateMetaProducer:    predicateMetaProducer,
			prioritizers:             prioritizers,
			priorityMetaProducer:     priorityMetaProducer,
			extenders:                extenders,
			cachedNodeInfoMap:        make(map[string]*schedulercache.NodeInfo),
			volumeBinder:             volumeBinder,
			pvcLister:                pvcLister,
			alwaysCheckAllPredicates: alwaysCheckAllPredicates,
		}
}
