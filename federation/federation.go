package federation

import "sync"

// Federation 是编排组件的门面。
// 所有变更与协调在同一把互斥锁下串行执行，且每轮协调后整体落盘，
// 因此并发提交期望状态/站点事件与周期协调交织时不会产生重复或丢失实例，
// 任意操作都可安全重试（幂等）。
type Federation struct {
	mu    sync.Mutex
	store *Store
	state *State
}

// Open 从 store 加载状态并创建 Federation。
// 加载后先做一次协调，使落盘状态与期望状态在重启后重新收敛。
func Open(store *Store) (*Federation, error) {
	st, err := store.Load()
	if err != nil {
		return nil, err
	}
	f := &Federation{store: store, state: st}
	if _, err := f.Reconcile(); err != nil {
		return nil, err
	}
	return f, nil
}

// Reconcile 执行一轮协调并落盘，返回本轮变更说明。
func (f *Federation) Reconcile() ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	log := Reconcile(f.state)
	if err := f.store.Save(f.state); err != nil {
		return nil, err
	}
	return log, nil
}

// mutate 在锁内修改状态、协调并落盘。所有写操作的统一入口。
func (f *Federation) mutate(fn func(st *State)) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f.state)
	log := Reconcile(f.state)
	if err := f.store.Save(f.state); err != nil {
		return nil, err
	}
	return log, nil
}

// UpsertSite 注册或更新站点（容量、标签、状态）。
func (f *Federation) UpsertSite(s Site) ([]string, error) {
	return f.mutate(func(st *State) {
		c := s
		st.Sites[s.Name] = &c
	})
}

// SetSiteState 变更站点可用性（Active/Draining/Failed），用于故障与恢复。
func (f *Federation) SetSiteState(name string, s SiteState) ([]string, error) {
	return f.mutate(func(st *State) {
		if site, ok := st.Sites[name]; ok {
			site.State = s
		}
	})
}

// Apply 声明或更新一个期望负载（扩容/缩容/更新通过修改 Spec 字段表达）。
func (f *Federation) Apply(spec WorkloadSpec) ([]string, error) {
	return f.mutate(func(st *State) {
		c := spec
		st.Desired[spec.Name] = &c
	})
}

// Delete 删除一个期望负载，其实例随之回收。
func (f *Federation) Delete(workload string) ([]string, error) {
	return f.mutate(func(st *State) {
		delete(st.Desired, workload)
	})
}

// Snapshot 返回当前状态的深拷贝，调用方可安全读取。
func (f *Federation) Snapshot() *State {
	f.mu.Lock()
	defer f.mu.Unlock()
	return deepCopy(f.state)
}

func deepCopy(st *State) *State {
	out := NewState()
	for k, v := range st.Sites {
		c := *v
		labels := map[string]string{}
		for lk, lv := range v.Labels {
			labels[lk] = lv
		}
		c.Labels = labels
		out.Sites[k] = &c
	}
	for k, v := range st.Desired {
		c := *v
		sel := map[string]string{}
		for sk, sv := range v.Selector {
			sel[sk] = sv
		}
		c.Selector = sel
		out.Desired[k] = &c
	}
	for k, v := range st.Instances {
		c := *v
		out.Instances[k] = &c
	}
	out.Rejections = append([]Rejection(nil), st.Rejections...)
	for k, v := range st.Status {
		c := *v
		c.Migrated = append([]string(nil), v.Migrated...)
		out.Status[k] = &c
	}
	return out
}
