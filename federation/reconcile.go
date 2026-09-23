package federation

import (
	"fmt"
	"sort"
)

// instanceID 由负载名与序号确定性派生实例 ID。
// 这是幂等的关键：同一期望状态重复协调、故障重建、恢复后都不会产生重复实例。
func instanceID(workload string, ordinal int) string {
	return fmt.Sprintf("%s-%d", workload, ordinal)
}

// Reconcile 对状态执行一轮协调，使其收敛到期望状态。
// 幂等：同一 (State, Desired) 重复执行结论不变；可安全重试。
// 返回本轮变更说明（用于日志与测试断言）。
//
// 协调顺序（每步都基于前一步的结果，保证结论可预期）：
//  1. 删除不再期望的负载的全部实例；
//  2. 对每个期望负载，移除多余序号（缩容）与失效实例
//     （所在站点 Failed/Draining/不存在，或标签/互斥不再满足）；
//  3. 对缺口按确定性规则补放；放不下的记录可区分的拒绝原因。
func Reconcile(st *State) []string {
	var log []string

	// 1. 删除不再期望的负载。
	for id, inst := range st.Instances {
		if _, ok := st.Desired[inst.Workload]; !ok {
			delete(st.Instances, id)
			log = append(log, fmt.Sprintf("remove %s: workload %q no longer desired", id, inst.Workload))
		}
	}

	// 2c. 站点容量被调小导致超配时，按实例 ID 降序逐出直至不超载（确定性）。
	{
		used := map[string]int{}
		for _, inst := range st.Instances {
			used[inst.Site] += inst.Size
		}
		ids := make([]string, 0, len(st.Instances))
		for id := range st.Instances {
			ids = append(ids, id)
		}
		sort.Sort(sort.Reverse(sort.StringSlice(ids)))
		for _, id := range ids {
			inst := st.Instances[id]
			site, ok := st.Sites[inst.Site]
			if !ok {
				continue // 已在 2b 处理
			}
			if used[inst.Site] > site.Capacity {
				delete(st.Instances, id)
				used[inst.Site] -= inst.Size
				log = append(log, fmt.Sprintf("evict %s from %s: site over capacity (%d > %d)", id, inst.Site, used[inst.Site]+inst.Size, site.Capacity))
			}
		}
	}

	// 按负载名排序处理，保证日志与结论确定。
	wlNames := make([]string, 0, len(st.Desired))
	for n := range st.Desired {
		wlNames = append(wlNames, n)
	}
	sort.Strings(wlNames)

	rejections := make([]Rejection, 0)

	for _, wl := range wlNames {
		spec := st.Desired[wl]

		// 2a. 缩容：序号 >= Replicas 的实例移除（序号大者先删，确定性）。
		for ord := spec.Replicas; ; ord++ {
			id := instanceID(wl, ord)
			inst, ok := st.Instances[id]
			if !ok {
				break
			}
			delete(st.Instances, id)
			log = append(log, fmt.Sprintf("remove %s: scale down %q to %d replicas", id, wl, spec.Replicas))
			_ = inst
		}

		// 2b. 失效实例：站点不可用/排空/不存在，或约束不再满足。
		for ord := 0; ord < spec.Replicas; ord++ {
			id := instanceID(wl, ord)
			inst, ok := st.Instances[id]
			if !ok {
				continue
			}
			if why := invalidReason(st, spec, inst); why != "" {
				delete(st.Instances, id)
				log = append(log, fmt.Sprintf("evict %s from %s: %s", id, inst.Site, why))
			}
		}

		// 3. 补放缺口。
		view := newPlacementView(st.Instances)
		for ord := 0; ord < spec.Replicas; ord++ {
			id := instanceID(wl, ord)
			if _, ok := st.Instances[id]; ok {
				continue
			}
			site, rej := chooseSite(spec, ord, st.Sites, view)
			if site == "" {
				rejections = append(rejections, rej)
				log = append(log, fmt.Sprintf("reject %s: %s (%s)", id, rej.Reason, rej.Detail))
				continue
			}
			inst := &Instance{
				ID:       id,
				Workload: wl,
				Ordinal:  ord,
				Site:     site,
				Size:     spec.Size,
				MutexKey: spec.MutexKey,
			}
			st.Instances[id] = inst
			view.occupy(site, spec.Size, spec.MutexKey)
			log = append(log, fmt.Sprintf("place %s on %s", id, site))
		}
	}

	// 拒绝记录整体替换：反映本轮协调后的当前缺口，而非历史累积。
	st.Rejections = rejections
	return log
}

// invalidReason 判断已放置实例是否失效；返回空串表示仍然有效。
func invalidReason(st *State, spec *WorkloadSpec, inst *Instance) string {
	site, ok := st.Sites[inst.Site]
	if !ok {
		return "site no longer exists"
	}
	switch site.State {
	case SiteFailed:
		return "site failed"
	case SiteDraining:
		return "site draining"
	}
	if !matchSelector(spec.Selector, site.Labels) {
		return "site labels no longer match selector"
	}
	// 互斥：同站点上是否存在更早序号的同 MutexKey 实例（保留序号小者，结论确定）。
	if inst.MutexKey != "" {
		for _, other := range st.Instances {
			if other.ID == inst.ID || other.Site != inst.Site || other.MutexKey != inst.MutexKey {
				continue
			}
			if other.ID < inst.ID {
				return fmt.Sprintf("mutex conflict with %s on %s", other.ID, inst.Site)
			}
		}
	}
	return ""
}
