package federation

import (
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

// --- 测试辅助 ---

func newFed(t *testing.T) *Federation {
	t.Helper()
	f, err := Open(NewStore(filepath.Join(t.TempDir(), "state.json")))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return f
}

func mustApply(t *testing.T, f *Federation, spec WorkloadSpec) {
	t.Helper()
	log, err := f.Apply(spec)
	if err != nil {
		t.Fatalf("apply %s: %v", spec.Name, err)
	}
	logDecision(t, fmt.Sprintf("apply %+v", spec), log)
}

func logDecision(t *testing.T, what string, log []string) {
	t.Helper()
	t.Logf("== %s", what)
	for _, l := range log {
		t.Logf("   %s", l)
	}
}

// countOn 统计站点上的实例数。
func countOn(st *State, site string) int {
	n := 0
	for _, inst := range st.Instances {
		if inst.Site == site {
			n++
		}
	}
	return n
}

// usedOn 统计站点已用容量。
func usedOn(st *State, site string) int {
	n := 0
	for _, inst := range st.Instances {
		if inst.Site == site {
			n += inst.Size
		}
	}
	return n
}

// assertCapacity 断言所有站点均未超容量。
func assertCapacity(t *testing.T, st *State) {
	t.Helper()
	for name, site := range st.Sites {
		if used := usedOn(st, name); used > site.Capacity {
			t.Errorf("site %s over capacity: used=%d cap=%d", name, used, site.Capacity)
		}
	}
}

// assertNoDup 断言实例 ID 无重复（map 天然去重，此处断言语义总数）。
func assertTotal(t *testing.T, st *State, want int) {
	t.Helper()
	if got := len(st.Instances); got != want {
		t.Errorf("total instances = %d, want %d", got, want)
	}
}

func rejectReasons(st *State) map[RejectReason]int {
	out := map[RejectReason]int{}
	for _, r := range st.Rejections {
		out[r.Reason]++
	}
	return out
}

// instanceMap 返回 id@site 的有序列表，便于比较两次协调结论。
func instanceMap(st *State) []string {
	var out []string
	for id, inst := range st.Instances {
		out = append(out, id+"@"+inst.Site)
	}
	sort.Strings(out)
	return out
}

// --- 用例 ---

// TestCapacityInsufficient 容量不足：拒绝原因可区分，且不得超量放置。
func TestCapacityInsufficient(t *testing.T) {
	f := newFed(t)
	mustUpsert := func(s Site) {
		log, _ := f.UpsertSite(s)
		logDecision(t, "upsert "+s.Name, log)
	}
	mustUpsert(Site{Name: "edge-a", Capacity: 3, State: SiteActive})
	mustUpsert(Site{Name: "edge-b", Capacity: 2, State: SiteActive})

	// 3 个实例 x size 2 = 6 > 总容量 5，只能放下 2 个。
	mustApply(t, f, WorkloadSpec{Name: "app", Replicas: 3, Size: 2})

	st := f.Snapshot()
	t.Logf("desired: 3x3; placed=%d rejections=%v", len(st.Instances), st.Rejections)
	assertCapacity(t, st)
	assertTotal(t, st, 2) // 只能放下 2 个
	if got := rejectReasons(st)[ReasonInsufficientCapacity]; got != 1 {
		t.Fatalf("want 1 InsufficientCapacity rejection, got %d (%v)", got, st.Rejections)
	}

	// 重复协调：结论（实例集合 + 拒绝记录）必须稳定。
	before := st
	log, _ := f.Reconcile()
	logDecision(t, "reconcile again", log)
	after := f.Snapshot()
	if fmt.Sprint(instanceMap(before)) != fmt.Sprint(instanceMap(after)) {
		t.Fatalf("second reconcile changed instances:\n%v\n%v", instanceMap(before), instanceMap(after))
	}
	if fmt.Sprint(before.Rejections) != fmt.Sprint(after.Rejections) {
		t.Fatalf("second reconcile changed rejections:\n%v\n%v", before.Rejections, after.Rejections)
	}
}

// TestLabelMismatch 标签约束不满足：ReasonLabelMismatch。
func TestLabelMismatch(t *testing.T) {
	f := newFed(t)
	f.UpsertSite(Site{Name: "edge-a", Capacity: 10, State: SiteActive, Labels: map[string]string{"region": "east"}})

	mustApply(t, f, WorkloadSpec{Name: "gpu-job", Replicas: 1, Size: 1,
		Selector: map[string]string{"gpu": "true"}})

	st := f.Snapshot()
	t.Logf("desired selector gpu=true; sites only have region=east; rejections=%v", st.Rejections)
	assertTotal(t, st, 0)
	if got := rejectReasons(st)[ReasonLabelMismatch]; got != 1 {
		t.Fatalf("want LabelMismatch, got %v", st.Rejections)
	}
}

// TestMutexConflict 互斥：同站点不允许两个相同 MutexKey 实例。
func TestMutexConflict(t *testing.T) {
	f := newFed(t)
	f.UpsertSite(Site{Name: "edge-a", Capacity: 10, State: SiteActive})

	mustApply(t, f, WorkloadSpec{Name: "db", Replicas: 2, Size: 1, MutexKey: "db-leader"})

	st := f.Snapshot()
	t.Logf("desired 2 replicas mutexKey=db-leader, single site; rejections=%v", st.Rejections)
	assertTotal(t, st, 1)
	if got := rejectReasons(st)[ReasonMutexConflict]; got != 1 {
		t.Fatalf("want MutexConflict, got %v", st.Rejections)
	}

	// 增加一个站点后应收敛到 2 个实例。
	log, _ := f.UpsertSite(Site{Name: "edge-b", Capacity: 10, State: SiteActive})
	logDecision(t, "add edge-b", log)
	st = f.Snapshot()
	assertTotal(t, st, 2)
	if countOn(st, "edge-a") != 1 || countOn(st, "edge-b") != 1 {
		t.Errorf("mutex instances should spread across sites: %v", st.Instances)
	}
}

// TestNoEligibleSite 全部站点不可用：ReasonNoEligibleSite。
func TestNoEligibleSite(t *testing.T) {
	f := newFed(t)
	f.UpsertSite(Site{Name: "edge-a", Capacity: 10, State: SiteDraining})
	mustApply(t, f, WorkloadSpec{Name: "app", Replicas: 1, Size: 1})
	st := f.Snapshot()
	t.Logf("only site draining; rejections=%v", st.Rejections)
	if got := rejectReasons(st)[ReasonNoEligibleSite]; got != 1 {
		t.Fatalf("want NoEligibleSite, got %v", st.Rejections)
	}
}

// TestFailoverAndRecovery 故障迁移与恢复：重建不超容量，恢复后不重复。
func TestFailoverAndRecovery(t *testing.T) {
	f := newFed(t)
	f.UpsertSite(Site{Name: "edge-a", Capacity: 3, State: SiteActive})
	f.UpsertSite(Site{Name: "edge-b", Capacity: 3, State: SiteActive})
	mustApply(t, f, WorkloadSpec{Name: "app", Replicas: 3, Size: 1})

	st := f.Snapshot()
	t.Logf("initial: 3 replicas on 2 sites: a=%d b=%d", countOn(st, "edge-a"), countOn(st, "edge-b"))
	assertTotal(t, st, 3)

	// edge-a 故障：其上实例需在 edge-b 重建，且 edge-b 容量 3 不得超出。
	log, _ := f.SetSiteState("edge-a", SiteFailed)
	logDecision(t, "edge-a failed", log)
	st = f.Snapshot()
	assertCapacity(t, st)
	if got := countOn(st, "edge-b"); got > 3 {
		t.Fatalf("edge-b over capacity after failover: %d", got)
	}
	// edge-b 容量 3，最多容纳 3 个；总数应为 3 或给出缺口解释。
	if len(st.Instances)+len(st.Rejections) != 3 {
		t.Fatalf("instances(%d) + rejections(%d) != desired 3", len(st.Instances), len(st.Rejections))
	}
	t.Logf("after failover: placed=%d gap=%v", len(st.Instances), st.Rejections)

	// edge-a 恢复：不得出现重复实例，总数仍为期望值。
	log, _ = f.SetSiteState("edge-a", SiteActive)
	logDecision(t, "edge-a recovered", log)
	st = f.Snapshot()
	assertTotal(t, st, 3)
	assertCapacity(t, st)
	if len(st.Rejections) != 0 {
		t.Fatalf("after recovery want no gap, got %v", st.Rejections)
	}
}

// TestDrain 排空：存量迁出，新实例不再落入。
func TestDrain(t *testing.T) {
	f := newFed(t)
	f.UpsertSite(Site{Name: "edge-a", Capacity: 5, State: SiteActive})
	f.UpsertSite(Site{Name: "edge-b", Capacity: 5, State: SiteActive})
	mustApply(t, f, WorkloadSpec{Name: "app", Replicas: 4, Size: 1})

	log, _ := f.SetSiteState("edge-a", SiteDraining)
	logDecision(t, "drain edge-a", log)
	st := f.Snapshot()
	if got := countOn(st, "edge-a"); got != 0 {
		t.Fatalf("draining site still hosts %d instances", got)
	}
	assertTotal(t, st, 4)
	assertCapacity(t, st)
}

// TestConcurrentReconcile 期望变更与周期协调并发：不重复、不丢失、可重试。
func TestConcurrentReconcile(t *testing.T) {
	f := newFed(t)
	for i := 0; i < 5; i++ {
		f.UpsertSite(Site{Name: fmt.Sprintf("edge-%d", i), Capacity: 10, State: SiteActive})
	}

	var wg sync.WaitGroup
	// 并发写期望状态：扩缩容与更新交织。
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("wl-%d", i%4)
			for j := 1; j <= 5; j++ {
				f.Apply(WorkloadSpec{Name: name, Replicas: j, Size: 1})
			}
			f.Delete(name)
			f.Apply(WorkloadSpec{Name: name, Replicas: 2, Size: 1})
		}(i)
	}
	// 并发周期协调与站点事件。
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				f.Reconcile()
				f.SetSiteState(fmt.Sprintf("edge-%d", i), SiteFailed)
				f.SetSiteState(fmt.Sprintf("edge-%d", i), SiteActive)
			}
		}(i)
	}
	wg.Wait()

	// 最终收敛：每个负载 2 副本，共 4 个负载 = 8 实例（全部站点恢复后无缺口）。
	log, _ := f.Reconcile()
	logDecision(t, "final reconcile", log)
	st := f.Snapshot()
	assertCapacity(t, st)
	assertTotal(t, st, 8)
	if len(st.Rejections) != 0 {
		t.Fatalf("unexpected gap after convergence: %v", st.Rejections)
	}
	// 再协调一次应为无操作（可安全重试，不放大副作用）。
	log, _ = f.Reconcile()
	if len(log) != 0 {
		t.Fatalf("retry reconcile not a no-op: %v", log)
	}
}

// TestPersistence 落盘后重新加载得到一致结论。
func TestPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	store := NewStore(path)

	f1, err := Open(store)
	if err != nil {
		t.Fatal(err)
	}
	f1.UpsertSite(Site{Name: "edge-a", Capacity: 2, State: SiteActive})
	mustApply(t, f1, WorkloadSpec{Name: "app", Replicas: 3, Size: 1})
	before := f1.Snapshot()
	t.Logf("before reload: placed=%d rejections=%v", len(before.Instances), before.Rejections)

	// 模拟重启：重新加载，Open 内部先协调一轮。
	f2, err := Open(store)
	if err != nil {
		t.Fatal(err)
	}
	after := f2.Snapshot()
	t.Logf("after reload: placed=%d rejections=%v", len(after.Instances), after.Rejections)

	if len(before.Instances) != len(after.Instances) {
		t.Fatalf("reload changed instances: %d -> %d", len(before.Instances), len(after.Instances))
	}
	for id, inst := range before.Instances {
		got, ok := after.Instances[id]
		if !ok || *got != *inst {
			t.Fatalf("instance %s changed after reload: %+v -> %+v", id, inst, got)
		}
	}
	if len(before.Rejections) != len(after.Rejections) {
		t.Fatalf("reload changed rejections: %v -> %v", before.Rejections, after.Rejections)
	}
}

// TestDeterministicPlacement 同一期望状态重复协调放置结论稳定。
func TestDeterministicPlacement(t *testing.T) {
	build := func() *State {
		f := newFed(t)
		f.UpsertSite(Site{Name: "edge-a", Capacity: 2, State: SiteActive})
		f.UpsertSite(Site{Name: "edge-b", Capacity: 5, State: SiteActive})
		f.UpsertSite(Site{Name: "edge-c", Capacity: 5, State: SiteActive})
		mustApply(t, f, WorkloadSpec{Name: "app", Replicas: 4, Size: 2})
		return f.Snapshot()
	}
	s1, s2 := build(), build()
	keys := func(st *State) []string {
		var out []string
		for id, inst := range st.Instances {
			out = append(out, id+"@"+inst.Site)
		}
		sort.Strings(out)
		return out
	}
	k1, k2 := keys(s1), keys(s2)
	t.Logf("run1=%v run2=%v", k1, k2)
	if fmt.Sprint(k1) != fmt.Sprint(k2) {
		t.Fatalf("placement not deterministic:\n%v\n%v", k1, k2)
	}
}
