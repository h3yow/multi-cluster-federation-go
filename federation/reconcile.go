package federation

import (
	"fmt"
	"sort"
)

// instanceID 由负载名与序号确定性派生实例 ID。
// 这是幂等的关键：同一期望状态重复协调、故障重建、恢复后都不会产生重复实例。
// 声明调整（Size/MutexKey/Selector 变化）不改变实例 ID，只触发重新评估。
func instanceID(workload string, ordinal int) string {
	return fmt.Sprintf("%s-%d", workload, ordinal)
}

// Reconcile 对状态执行一轮协调，使其收敛到期望状态。
// 幂等：同一 (State, Desired) 重复执行结论不变；可安全重试。
// 返回本轮变更说明（用于日志与测试断言）。
//
// 协调顺序（每步都基于前一步的结果，保证结论可预期）：
//  1. 删除不再期望的负载的全部实例与状态；
//  2. 对每个期望负载：缩容（高序号先删）→ 把最新声明同步到存量实例
//     （Size/MutexKey 原地更新，身份不变）→ 逐出失效实例
//     （站点 Failed/Draining/不存在、标签不再匹配、互斥冲突）；
//  3. 全局容量检查：声明调大导致站点超配时，按实例 ID 降序逐出直至不超载
//     （落盘状态任何时刻不超配，不为放新规格临时超配）；
//  4. 对缺口按确定性规则补放；放不下的记录可区分的拒绝原因；
//  5. 重算每个负载的收敛状态（含本轮迁移记录）。
func Reconcile(st *State) []string {
	var log []string

	// evicted 记录本轮被逐出的实例及其原站点，用于识别"迁走"（逐出后改放他站）。
	evicted := map[string]string{}

	evict := func(id, why string) {
		inst := st.Instances[id]
		evicted[id] = inst.Site
		delete(st.Instances, id)
		log = append(log, fmt.Sprintf("evict %s from %s: %s", id, inst.Site, why))
	}

	// 1. 删除不再期望的负载。
	for id, inst := range st.Instances {
		if _, ok := st.Desired[inst.Workload]; !ok {
			delete(st.Instances, id)
			log = append(log, fmt.Sprintf("remove %s: workload %q no longer desired", id, inst.Workload))
		}
	}
	for wl := range st.Status {
		if _, ok := st.Desired[wl]; !ok {
			delete(st.Status, wl)
		}
	}

	// 按负载名排序处理，保证日志与结论确定。
	wlNames := make([]string, 0, len(st.Desired))
	for n := range st.Desired {
		wlNames = append(wlNames, n)
	}
	sort.Strings(wlNames)

	// 2. 逐负载：缩容 → 同步最新声明 → 逐出失效实例。
	for _, wl := range wlNames {
		spec := st.Desired[wl]

		// 2a. 缩容：序号 >= Replicas 的实例移除（序号小者先遇到即停，确定性）。
		for ord := spec.Replicas; ; ord++ {
			id := instanceID(wl, ord)
			if _, ok := st.Instances[id]; !ok {
				break
			}
			delete(st.Instances, id)
			log = append(log, fmt.Sprintf("remove %s: scale down %q to %d replicas", id, wl, spec.Replicas))
		}

		// 2b. 把最新声明同步到存量实例：Size/MutexKey 原地更新，实例身份不变。
		// 仍满足新声明的实例留在原站点；同步后不再满足的由 2c/3 逐出。
		for ord := 0; ord < spec.Replicas; ord++ {
			id := instanceID(wl, ord)
			inst, ok := st.Instances[id]
			if !ok {
				continue
			}
			if inst.Size != spec.Size {
				log = append(log, fmt.Sprintf("resize %s on %s: size %d -> %d", id, inst.Site, inst.Size, spec.Size))
				inst.Size = spec.Size
			}
			if inst.MutexKey != spec.MutexKey {
				log = append(log, fmt.Sprintf("rekey %s on %s: mutexKey %q -> %q", id, inst.Site, inst.MutexKey, spec.MutexKey))
				inst.MutexKey = spec.MutexKey
			}
		}

		// 2c. 失效实例：站点不可用/排空/不存在，标签不再匹配，或互斥冲突。
		for ord := 0; ord < spec.Replicas; ord++ {
			id := instanceID(wl, ord)
			inst, ok := st.Instances[id]
			if !ok {
				continue
			}
			if why := invalidReason(st, spec, inst); why != "" {
				evict(id, why)
			}
		}
	}

	// 3. 容量强一致：声明调大或站点容量调小导致超配时，
	// 按实例 ID 降序逐出直至不超载（确定性；被逐出者在第 4 步尝试改放他站）。
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
			inst, ok := st.Instances[id]
			if !ok {
				continue // 本轮已被逐出
			}
			site, ok := st.Sites[inst.Site]
			if !ok {
				continue // 已在 2c 处理
			}
			if used[inst.Site] > site.Capacity {
				evict(id, fmt.Sprintf("site over capacity (%d > %d)", used[inst.Site], site.Capacity))
				used[inst.Site] -= inst.Size
			}
		}
	}

	// 4. 补放缺口。
	rejections := make([]Rejection, 0)
	migrated := map[string][]string{} // workload -> 本轮迁移记录
	for _, wl := range wlNames {
		spec := st.Desired[wl]
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
			if from, ok := evicted[id]; ok {
				migrated[wl] = append(migrated[wl], fmt.Sprintf("%s: %s->%s", id, from, site))
				log = append(log, fmt.Sprintf("migrate %s: %s -> %s", id, from, site))
			} else {
				log = append(log, fmt.Sprintf("place %s on %s", id, site))
			}
		}
	}

	// 拒绝记录整体替换：反映本轮协调后的当前缺口，而非历史累积。
	st.Rejections = rejections

	// 5. 重算收敛状态：每个负载是否按最新声明收敛、本轮迁走了哪些实例。
	pending := map[string]int{}
	for _, r := range rejections {
		pending[r.Workload]++
	}
	for _, wl := range wlNames {
		spec := st.Desired[wl]
		placed := 0
		for _, inst := range st.Instances {
			if inst.Workload == wl {
				placed++
			}
		}
		st.Status[wl] = &WorkloadStatus{
			Desired:   spec.Replicas,
			Placed:    placed,
			Pending:   pending[wl],
			Converged: placed == spec.Replicas && pending[wl] == 0,
			Migrated:  migrated[wl],
		}
	}
	return log
}

// invalidReason 判断已放置实例是否失效；返回空串表示仍然有效。
// 调用前实例的 Size/MutexKey 已同步为最新声明，因此互斥按最新范围判定。
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
	// 互斥：同站点上是否存在 ID 更小的同 MutexKey 实例（保留 ID 小者，结论确定）。
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
