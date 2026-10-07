package federation

import (
	"fmt"
	"sort"
	"strings"
)

// placementView 是调度视角：站点已用容量与互斥占用。
// 关键约束：视角只计入“已确认保留或已新放”的实例；按最新声明评估存量时，
// 本轮即将腾出的占用不会被计入，从而既不会误判超配，也不会把可行实例误迁。
// 随每次确认保留/试放置递增，保证同一轮协调内容量绝不超配。
type placementView struct {
	used  map[string]int               // site -> 已用容量
	mutex map[string]map[string]string // site -> mutexKey -> 属主实例 ID
}

// newPlacementView 从实例集合构建调度视角（传 nil 得到空视角）。
func newPlacementView(instances map[string]*Instance) *placementView {
	v := &placementView{
		used:  map[string]int{},
		mutex: map[string]map[string]string{},
	}
	for _, inst := range instances {
		v.occupy(inst)
	}
	return v
}

// occupy 把一个实例（按其当前 Size/MutexKey）计入视角。
func (v *placementView) occupy(inst *Instance) {
	v.used[inst.Site] += inst.Size
	if inst.MutexKey != "" {
		if v.mutex[inst.Site] == nil {
			v.mutex[inst.Site] = map[string]string{}
		}
		v.mutex[inst.Site][inst.MutexKey] = inst.ID
	}
}

// free 返回站点剩余容量。
func (v *placementView) free(site *Site) int {
	return site.Capacity - v.used[site.Name]
}

// mutexBlocked 判断 selfID 以 mutexKey 落在该站点时是否被其他实例挡住。
// 属主是自己（如原地更新 Size）不算冲突。
func (v *placementView) mutexBlocked(site, mutexKey, selfID string) bool {
	if mutexKey == "" {
		return false
	}
	owner := v.mutex[site][mutexKey]
	return owner != "" && owner != selfID
}

// matchSelector 判断站点标签是否满足选择器（站点须包含全部键值）。
func matchSelector(selector, labels map[string]string) bool {
	for k, want := range selector {
		if labels[k] != want {
			return false
		}
	}
	return true
}

// chooseSite 为 spec 的第 ordinal 个实例选择站点。
// 返回选中站点名；无法放置时返回拒绝原因与逐项判定依据。
// 选择规则确定性：可行站点中按 (剩余容量降序, 名称升序) 取首个，
// 保证同一期望状态重复协调结论稳定。
func chooseSite(spec *WorkloadSpec, ordinal int, sites map[string]*Site, v *placementView) (string, Rejection) {
	rej := Rejection{Workload: spec.Name, Ordinal: ordinal}

	names := make([]string, 0, len(sites))
	for n := range sites {
		names = append(names, n)
	}
	sort.Strings(names)

	type candidate struct {
		name string
		free int
	}
	var cands []candidate
	var labelFailed, capFailed, mutexFailed, inactive []string

	for _, n := range names {
		s := sites[n]
		if s.State != SiteActive {
			inactive = append(inactive, fmt.Sprintf("%s(state=%s)", n, s.State))
			continue
		}
		if !matchSelector(spec.Selector, s.Labels) {
			labelFailed = append(labelFailed, n)
			continue
		}
		if v.mutexBlocked(n, spec.MutexKey, "") {
			mutexFailed = append(mutexFailed, n)
			continue
		}
		free := s.Capacity - v.used[n]
		if free < spec.Size {
			capFailed = append(capFailed, fmt.Sprintf("%s(free=%d,need=%d)", n, free, spec.Size))
			continue
		}
		cands = append(cands, candidate{name: n, free: free})
	}

	if len(cands) > 0 {
		// 剩余容量降序，容量相同按名称升序：完全确定。
		sort.Slice(cands, func(i, j int) bool {
			if cands[i].free != cands[j].free {
				return cands[i].free > cands[j].free
			}
			return cands[i].name < cands[j].name
		})
		return cands[0].name, Rejection{}
	}

	// 无可行站点：按主导原因归类，并附逐项依据。
	var parts []string
	if len(inactive) > 0 {
		parts = append(parts, "不可用站点: "+strings.Join(inactive, ","))
	}
	if len(labelFailed) > 0 {
		parts = append(parts, "标签不匹配: "+strings.Join(labelFailed, ","))
	}
	if len(mutexFailed) > 0 {
		parts = append(parts, "互斥冲突: "+strings.Join(mutexFailed, ","))
	}
	if len(capFailed) > 0 {
		parts = append(parts, "容量不足: "+strings.Join(capFailed, ","))
	}
	rej.Detail = strings.Join(parts, "; ")

	switch {
	case len(sites) == 0 || (len(labelFailed) == 0 && len(capFailed) == 0 && len(mutexFailed) == 0):
		// 没有任何可调度（Active）站点。
		rej.Reason = ReasonNoEligibleSite
	case len(labelFailed) == 0 && len(capFailed) == 0 && len(mutexFailed) > 0:
		// 标签全过，仅互斥挡住。
		rej.Reason = ReasonMutexConflict
	case len(labelFailed) == 0 && len(capFailed) > 0:
		// 标签全过，存在容量不足（可能同时有互斥冲突，容量不足为主因）。
		rej.Reason = ReasonInsufficientCapacity
	default:
		// 所有 Active 站点均标签不匹配。
		rej.Reason = ReasonLabelMismatch
	}
	return "", rej
}
