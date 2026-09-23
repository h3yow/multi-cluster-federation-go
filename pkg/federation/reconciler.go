package federation

import (
	"sort"
	"sync"
)

// Reconciler 把期望状态收敛到实际状态。
// Reconcile 是幂等的：同一状态重复协调结论稳定，可安全重试与并发调用。
type Reconciler struct {
	store *Store
	mu    sync.Mutex // 串行化协调，避免并发协调互相覆盖
}

// NewReconciler 基于给定存储创建协调器。
func NewReconciler(store *Store) *Reconciler {
	return &Reconciler{store: store}
}

// Reconcile 执行一次协调：快照 -> 规划 -> 整体替换实例集 -> 落盘 -> 报告。
// 报告中的 Placed/Removed/Kept/Rejected 即本次协调的可解释结论。
func (r *Reconciler) Reconcile() (*Report, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	sites, workloads, existing := r.store.snapshot()
	targets, rejections := planPlacement(placementInput{
		sites:     sites,
		workloads: workloads,
		existing:  existing,
	})

	// 与现状做差分，生成可解释的报告。
	existingByID := map[string]*Instance{}
	for _, inst := range existing {
		existingByID[inst.ID] = inst
	}
	targetByID := map[string]*Instance{}
	for _, inst := range targets {
		targetByID[inst.ID] = inst
	}
	report := &Report{Rejected: rejections}
	for _, inst := range targets {
		if _, ok := existingByID[inst.ID]; ok {
			report.Kept = append(report.Kept, *inst)
		} else {
			report.Placed = append(report.Placed, *inst)
		}
	}
	for _, inst := range existing {
		if _, ok := targetByID[inst.ID]; !ok {
			report.Removed = append(report.Removed, *inst)
		}
	}
	sort.Slice(report.Placed, func(i, j int) bool { return report.Placed[i].ID < report.Placed[j].ID })
	sort.Slice(report.Kept, func(i, j int) bool { return report.Kept[i].ID < report.Kept[j].ID })
	sort.Slice(report.Removed, func(i, j int) bool { return report.Removed[i].ID < report.Removed[j].ID })
	report.Stable = len(report.Placed) == 0 && len(report.Removed) == 0

	// 整体替换实例集并落盘；重载后再次协调应得到同样结论。
	r.store.applyInstances(targets)
	if err := r.store.Save(); err != nil {
		return nil, err
	}
	return report, nil
}

// ---- 期望状态与站点状态的变更 API（变更后需调用 Reconcile 生效）----

// UpsertSite 新增或更新站点。
func (r *Reconciler) UpsertSite(s Site) {
	r.store.mu.Lock()
	defer r.store.mu.Unlock()
	r.store.Sites[s.Name] = &s
}

// SetSiteState 设置站点状态（Active/Draining/Failed），模拟故障、排空与恢复。
func (r *Reconciler) SetSiteState(name string, state SiteState) {
	r.store.mu.Lock()
	defer r.store.mu.Unlock()
	if s, ok := r.store.Sites[name]; ok {
		s.State = state
	}
}

// UpsertWorkload 声明或更新期望负载（扩容、缩容、更新均由此表达）。
func (r *Reconciler) UpsertWorkload(w Workload) {
	r.store.mu.Lock()
	defer r.store.mu.Unlock()
	r.store.Workloads[w.Name] = &w
}

// DeleteWorkload 删除期望负载，其全部实例将在下次协调时移除。
func (r *Reconciler) DeleteWorkload(name string) {
	r.store.mu.Lock()
	defer r.store.mu.Unlock()
	delete(r.store.Workloads, name)
}

// Save 持久化当前状态。
func (r *Reconciler) Save() error { return r.store.Save() }

// Instances 返回当前实际实例快照（按 ID 排序）。
func (r *Reconciler) Instances() []Instance {
	_, _, instances := r.store.snapshot()
	out := make([]Instance, 0, len(instances))
	for _, inst := range instances {
		out = append(out, *inst)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
