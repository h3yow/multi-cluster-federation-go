package federation

import (
	"fmt"
	"sort"
)

// placementInput 是一次放置计算的只读快照。
type placementInput struct {
	sites     []*Site
	workloads []*Workload
	existing  []*Instance
}

// siteUsage 跟踪规划过程中的站点占用。
type siteUsage struct {
	used      int
	workloads map[string]bool   // 站点上已有的负载名
	groups    map[string]string // mutexGroup -> 占用它的负载名
}

func sizeOf(w *Workload) int {
	if w.Size <= 0 {
		return 1
	}
	return w.Size
}

// eligible 判断站点是否可承载该负载（可用性 + 标签约束）。
func eligible(s *Site, w *Workload) bool {
	if s.State != SiteActive {
		return false
	}
	for k, v := range w.RequiredLabels {
		if s.Labels[k] != v {
			return false
		}
	}
	return true
}

// mutexConflict 判断把负载 w 放到站点上是否违反互斥关系：
// 同一互斥组内，不同负载的实例不得共存于同一站点。
func mutexConflict(u *siteUsage, w *Workload) bool {
	if w.MutexGroup == "" {
		return false
	}
	owner, taken := u.groups[w.MutexGroup]
	return taken && owner != w.Name
}

// planPlacement 计算目标放置方案：返回全部实例的目标位置与拒绝项。
// 纯函数：输入排序后按确定性规则计算，同样输入必然得到同样输出。
// 规则：
//  1. 已存在且仍合法的放置优先保留（保证重复协调结论稳定）；
//  2. 超出期望副本数或负载已删除的实例被移除；
//  3. 缺口按站点名升序 first-fit 放置，满足容量、标签与互斥约束；
//  4. 无法放置时按主导原因给出 Rejection。
func planPlacement(in placementInput) (targets []*Instance, rejections []Rejection) {
	sites := append([]*Site(nil), in.sites...)
	sort.Slice(sites, func(i, j int) bool { return sites[i].Name < sites[j].Name })
	workloads := append([]*Workload(nil), in.workloads...)
	sort.Slice(workloads, func(i, j int) bool { return workloads[i].Name < workloads[j].Name })
	existing := append([]*Instance(nil), in.existing...)
	sort.Slice(existing, func(i, j int) bool { return existing[i].ID < existing[j].ID })

	wlByName := map[string]*Workload{}
	for _, w := range workloads {
		wlByName[w.Name] = w
	}
	siteByName := map[string]*Site{}
	usage := map[string]*siteUsage{}
	for _, s := range sites {
		siteByName[s.Name] = s
		usage[s.Name] = &siteUsage{workloads: map[string]bool{}, groups: map[string]string{}}
	}

	keptCount := map[string]int{}
	usedIDs := map[string]bool{}

	// 第 1、2 步：保留仍合法的既有实例。
	for _, inst := range existing {
		w, ok := wlByName[inst.WorkloadName]
		if !ok {
			continue // 负载已删除
		}
		if keptCount[inst.WorkloadName] >= w.Replicas {
			continue // 超出期望副本数（缩容）
		}
		s, ok := siteByName[inst.SiteName]
		if !ok || !eligible(s, w) {
			continue // 站点消失、故障、排空或标签不再满足
		}
		u := usage[inst.SiteName]
		if u.used+sizeOf(w) > s.Capacity || mutexConflict(u, w) {
			continue // 站点容量或互斥关系已不允许保留
		}
		// 保留该实例。
		u.used += sizeOf(w)
		u.workloads[w.Name] = true
		if w.MutexGroup != "" {
			u.groups[w.MutexGroup] = w.Name
		}
		keptCount[w.Name]++
		usedIDs[inst.ID] = true
		targets = append(targets, inst)
	}

	// 第 3 步：为每个负载补齐缺口。
	for _, w := range workloads {
		need := w.Replicas - keptCount[w.Name]
		for i := 0; i < need; i++ {
			inst, rej := placeOne(w, sites, usage, usedIDs)
			if rej != nil {
				rejections = append(rejections, *rej)
				continue
			}
			targets = append(targets, inst)
		}
	}
	return targets, rejections
}

// placeOne 为负载 w 放置一个新实例；失败时返回带主导原因的 Rejection。
func placeOne(w *Workload, sites []*Site, usage map[string]*siteUsage, usedIDs map[string]bool) (*Instance, *Rejection) {
	sawEligible := false
	sawMutexBlock := false
	sawActive := false
	for _, s := range sites {
		if s.State == SiteActive {
			sawActive = true
		}
		if !eligible(s, w) {
			continue
		}
		sawEligible = true
		u := usage[s.Name]
		if mutexConflict(u, w) {
			sawMutexBlock = true
			continue
		}
		if u.used+sizeOf(w) > s.Capacity {
			continue
		}
		// 命中：分配确定性 ID 并占用资源。
		id := nextID(w.Name, usedIDs)
		usedIDs[id] = true
		u.used += sizeOf(w)
		u.workloads[w.Name] = true
		if w.MutexGroup != "" {
			u.groups[w.MutexGroup] = w.Name
		}
		return &Instance{ID: id, WorkloadName: w.Name, SiteName: s.Name}, nil
	}
	// 失败：按主导原因归类。
	switch {
	case !sawActive:
		return nil, &Rejection{WorkloadName: w.Name, Reason: RejectNoEligibleSite,
			Detail: "没有处于 Active 状态的站点"}
	case !sawEligible:
		return nil, &Rejection{WorkloadName: w.Name, Reason: RejectLabelMismatch,
			Detail: fmt.Sprintf("没有可用站点满足标签约束 %v", w.RequiredLabels)}
	case sawMutexBlock:
		return nil, &Rejection{WorkloadName: w.Name, Reason: RejectMutexConflict,
			Detail: fmt.Sprintf("互斥组 %q 内已有其他负载占用全部候选站点", w.MutexGroup)}
	default:
		return nil, &Rejection{WorkloadName: w.Name, Reason: RejectInsufficientCapacity,
			Detail: "候选站点剩余容量不足"}
	}
}

// nextID 生成确定性的实例 ID：<workload>-<最小可用序号>。
func nextID(workload string, used map[string]bool) string {
	for i := 0; ; i++ {
		id := fmt.Sprintf("%s-%d", workload, i)
		if !used[id] {
			return id
		}
	}
}
