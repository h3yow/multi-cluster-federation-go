package federation

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// --- 声明调整（更新）路径的测试 ---
// 重点关注：调大每实例容量单位（Size）与调整互斥范围（MutexKey）。

// siteOf 返回实例所在站点；不存在时返回空串。
func siteOf(st *State, id string) string {
	if inst, ok := st.Instances[id]; ok {
		return inst.Site
	}
	return ""
}

// logStatus 打印每个负载的收敛状态，便于在测试日志中核对本轮调整效果。
func logStatus(t *testing.T, st *State) {
	t.Helper()
	for name, s := range st.Status {
		t.Logf("status %s: desired=%d placed=%d pending=%d converged=%v migrated=%v",
			name, s.Desired, s.Placed, s.Pending, s.Converged, s.Migrated)
	}
}

// TestResizeExactFit 调大 Size 且各站点容量刚好够：
// 存量实例原地更新、不迁移，身份不变，收敛状态可见。
func TestResizeExactFit(t *testing.T) {
	f := newFed(t)
	f.UpsertSite(Site{Name: "edge-a", Capacity: 4, State: SiteActive})
	f.UpsertSite(Site{Name: "edge-b", Capacity: 4, State: SiteActive})
	mustApply(t, f, WorkloadSpec{Name: "app", Replicas: 2, Size: 1})

	st := f.Snapshot()
	t.Logf("期望: 2 实例 x size1; 实际 app-0@%s app-1@%s", siteOf(st, "app-0"), siteOf(st, "app-1"))
	before := instanceMap(st)

	// 调大到 size=4：两站点各 4 容量刚好够，实例应原地 resize 不挪动。
	mustApply(t, f, WorkloadSpec{Name: "app", Replicas: 2, Size: 4})
	st = f.Snapshot()
	logStatus(t, st)
	t.Logf("resize 后: app-0@%s(size=%d) app-1@%s(size=%d)",
		siteOf(st, "app-0"), st.Instances["app-0"].Size,
		siteOf(st, "app-1"), st.Instances["app-1"].Size)

	assertCapacity(t, st)
	assertTotal(t, st, 2)
	if got := instanceMap(st); fmt.Sprint(got) != fmt.Sprint(before) {
		t.Fatalf("容量刚好够时实例不应迁移: before=%v after=%v", before, got)
	}
	for _, id := range []string{"app-0", "app-1"} {
		if st.Instances[id].Size != 4 {
			t.Errorf("%s size = %d, want 4（应原地按最新声明更新）", id, st.Instances[id].Size)
		}
	}
	if !st.Status["app"].Converged {
		t.Errorf("status should be converged: %+v", st.Status["app"])
	}
	if len(st.Status["app"].Migrated) != 0 {
		t.Errorf("no migration expected, got %v", st.Status["app"].Migrated)
	}

	// 同一新声明反复协调：结论稳定，应为无操作。
	log, _ := f.Reconcile()
	logDecision(t, "reconcile after resize", log)
	if len(log) != 0 {
		t.Fatalf("reconcile after converged resize not a no-op: %v", log)
	}
}

// TestResizeMigrateOverflow 调大 Size 后原站点放不下全部实例：
// 放得下的留在原站点，放不下的迁到可行站点，身份不变、不超配。
func TestResizeMigrateOverflow(t *testing.T) {
	f := newFed(t)
	f.UpsertSite(Site{Name: "edge-a", Capacity: 6, State: SiteActive})
	f.UpsertSite(Site{Name: "edge-b", Capacity: 4, State: SiteActive})
	// 2 x size2 都落在 edge-a（剩余容量优先）。
	mustApply(t, f, WorkloadSpec{Name: "app", Replicas: 2, Size: 2})
	st := f.Snapshot()
	t.Logf("期望: 2 实例 x size2 同站; 实际 app-0@%s app-1@%s", siteOf(st, "app-0"), siteOf(st, "app-1"))
	if siteOf(st, "app-0") != "edge-a" || siteOf(st, "app-1") != "edge-a" {
		t.Fatalf("precondition: both instances should start on edge-a, got %v", instanceMap(st))
	}

	// 调大到 size=4：edge-a 放不下两个（8>6），app-1 应迁到 edge-b（恰好放满 4/4）。
	mustApply(t, f, WorkloadSpec{Name: "app", Replicas: 2, Size: 4})
	st = f.Snapshot()
	logStatus(t, st)
	t.Logf("resize 后: app-0@%s app-1@%s used(a)=%d used(b)=%d",
		siteOf(st, "app-0"), siteOf(st, "app-1"), usedOn(st, "edge-a"), usedOn(st, "edge-b"))

	assertCapacity(t, st)
	assertTotal(t, st, 2) // 实例数量不凭空变化
	if siteOf(st, "app-0") != "edge-a" {
		t.Errorf("app-0 仍满足新声明，应留在 edge-a, got %s", siteOf(st, "app-0"))
	}
	if siteOf(st, "app-1") != "edge-b" {
		t.Errorf("app-1 放不小应迁到 edge-b, got %s", siteOf(st, "app-1"))
	}
	if st.Instances["app-1"].Size != 4 {
		t.Errorf("app-1 size = %d, want 4（迁移后按最新声明）", st.Instances["app-1"].Size)
	}
	mig := st.Status["app"].Migrated
	if len(mig) != 1 || mig[0] != "app-1: edge-a->edge-b" {
		t.Errorf("status.Migrated = %v, want [app-1: edge-a->edge-b]", mig)
	}
	if !st.Status["app"].Converged {
		t.Errorf("should converge after migration: %+v", st.Status["app"])
	}

	// 重复协调：摆放结论稳定，不再来回挪。
	log, _ := f.Reconcile()
	logDecision(t, "reconcile again", log)
	if len(log) != 0 {
		t.Fatalf("not stable after migration: %v", log)
	}
}

// TestResizeRejectInsufficient 调大 Size 后全局差一点放不下：
// 给出 InsufficientCapacity 缺口，实例总数守恒（placed+pending==desired），不超配。
func TestResizeRejectInsufficient(t *testing.T) {
	f := newFed(t)
	f.UpsertSite(Site{Name: "edge-a", Capacity: 6, State: SiteActive})
	f.UpsertSite(Site{Name: "edge-b", Capacity: 3, State: SiteActive}) // 比上一个用例少 1，放不下 size4
	mustApply(t, f, WorkloadSpec{Name: "app", Replicas: 2, Size: 2})

	mustApply(t, f, WorkloadSpec{Name: "app", Replicas: 2, Size: 4})
	st := f.Snapshot()
	logStatus(t, st)
	t.Logf("rejections=%v", st.Rejections)

	assertCapacity(t, st)
	// app-0 留在 edge-a；app-1 既迁不走也放不下，进入缺口。
	assertTotal(t, st, 1)
	if siteOf(st, "app-0") != "edge-a" {
		t.Errorf("app-0 应留在 edge-a, got %s", siteOf(st, "app-0"))
	}
	if len(st.Instances)+len(st.Rejections) != 2 {
		t.Fatalf("placed(%d)+rejected(%d) != desired 2", len(st.Instances), len(st.Rejections))
	}
	if got := rejectReasons(st)[ReasonInsufficientCapacity]; got != 1 {
		t.Fatalf("want 1 InsufficientCapacity, got %v", st.Rejections)
	}
	s := st.Status["app"]
	if s.Converged || s.Pending != 1 || s.Placed != 1 {
		t.Errorf("status = %+v, want placed=1 pending=1 converged=false", s)
	}
	if len(s.Migrated) != 0 {
		t.Errorf("app-1 未成功迁移，Migrated 应为空, got %v", s.Migrated)
	}

	// 重复协调：缺口结论稳定，不反复逐出/重放。
	before := instanceMap(st)
	log, _ := f.Reconcile()
	logDecision(t, "reconcile again", log)
	after := f.Snapshot()
	if fmt.Sprint(instanceMap(after)) != fmt.Sprint(before) {
		t.Fatalf("placement not stable: %v -> %v", before, instanceMap(after))
	}
	if fmt.Sprint(before) != fmt.Sprint(instanceMap(after)) ||
		fmt.Sprint(st.Rejections) != fmt.Sprint(after.Rejections) {
		t.Fatalf("rejections not stable: %v -> %v", st.Rejections, after.Rejections)
	}

	// 扩容 edge-b 后缺口应被补上，app-1 以原身份落到 edge-b。
	log, _ = f.UpsertSite(Site{Name: "edge-b", Capacity: 4, State: SiteActive})
	logDecision(t, "grow edge-b to 4", log)
	st = f.Snapshot()
	logStatus(t, st)
	assertTotal(t, st, 2)
	if siteOf(st, "app-1") != "edge-b" {
		t.Errorf("app-1 应补放到 edge-b, got %s", siteOf(st, "app-1"))
	}
	if !st.Status["app"].Converged {
		t.Errorf("should converge after capacity added: %+v", st.Status["app"])
	}
}

// TestMutexKeyUpdate 调整互斥范围：原本同站的实例按新 MutexKey 冲突，必须分开。
func TestMutexKeyUpdate(t *testing.T) {
	f := newFed(t)
	f.UpsertSite(Site{Name: "edge-a", Capacity: 10, State: SiteActive})
	f.UpsertSite(Site{Name: "edge-b", Capacity: 1, State: SiteActive})
	// 无互斥：两个实例都落在容量更大的 edge-a。
	mustApply(t, f, WorkloadSpec{Name: "db", Replicas: 2, Size: 1})
	st := f.Snapshot()
	t.Logf("期望: 2 实例同站 edge-a; 实际 db-0@%s db-1@%s", siteOf(st, "db-0"), siteOf(st, "db-1"))
	if siteOf(st, "db-0") != "edge-a" || siteOf(st, "db-1") != "edge-a" {
		t.Fatalf("precondition: both on edge-a, got %v", instanceMap(st))
	}

	// 加上互斥声明：同站的 db-1 与 db-0 冲突，db-1 应迁到 edge-b。
	mustApply(t, f, WorkloadSpec{Name: "db", Replicas: 2, Size: 1, MutexKey: "leader"})
	st = f.Snapshot()
	logStatus(t, st)
	t.Logf("rekey 后: db-0@%s(key=%s) db-1@%s(key=%s)",
		siteOf(st, "db-0"), st.Instances["db-0"].MutexKey,
		siteOf(st, "db-1"), st.Instances["db-1"].MutexKey)

	assertTotal(t, st, 2)
	if countOn(st, "edge-a") != 1 || countOn(st, "edge-b") != 1 {
		t.Errorf("互斥生效后两实例必须分开: %v", instanceMap(st))
	}
	if siteOf(st, "db-0") != "edge-a" {
		t.Errorf("db-0（ID 小者）应留在 edge-a, got %s", siteOf(st, "db-0"))
	}
	for _, id := range []string{"db-0", "db-1"} {
		if st.Instances[id].MutexKey != "leader" {
			t.Errorf("%s mutexKey = %q, want leader（应按最新声明同步）", id, st.Instances[id].MutexKey)
		}
	}
	if !st.Status["db"].Converged {
		t.Errorf("should converge: %+v", st.Status["db"])
	}

	// 同一声明反复协调：互斥结论稳定，不来回挪。
	log, _ := f.Reconcile()
	logDecision(t, "reconcile again", log)
	if len(log) != 0 {
		t.Fatalf("not stable after mutex update: %v", log)
	}
}

// TestMutexKeyUpdateConflict 两个负载调整出相同 MutexKey：同站冲突者必须分开，
// 无处可去时给出 MutexConflict 缺口。
func TestMutexKeyUpdateConflict(t *testing.T) {
	f := newFed(t)
	f.UpsertSite(Site{Name: "edge-a", Capacity: 10, State: SiteActive})
	mustApply(t, f, WorkloadSpec{Name: "svc-a", Replicas: 1, Size: 1, MutexKey: "x"})
	mustApply(t, f, WorkloadSpec{Name: "svc-b", Replicas: 1, Size: 1, MutexKey: "y"})
	st := f.Snapshot()
	t.Logf("调整前: svc-a-0@%s svc-b-0@%s（不同 key 可同站）", siteOf(st, "svc-a-0"), siteOf(st, "svc-b-0"))
	assertTotal(t, st, 2)

	// svc-b 改用与 svc-a 相同的 MutexKey：单站点放不下两个同 key 实例。
	mustApply(t, f, WorkloadSpec{Name: "svc-b", Replicas: 1, Size: 1, MutexKey: "x"})
	st = f.Snapshot()
	logStatus(t, st)
	t.Logf("rejections=%v", st.Rejections)

	assertTotal(t, st, 1) // ID 小的 svc-a-0 保留
	if siteOf(st, "svc-a-0") != "edge-a" {
		t.Errorf("svc-a-0 应保留在 edge-a, got %s", siteOf(st, "svc-a-0"))
	}
	if got := rejectReasons(st)[ReasonMutexConflict]; got != 1 {
		t.Fatalf("want 1 MutexConflict, got %v", st.Rejections)
	}
	if st.Status["svc-b"].Converged {
		t.Errorf("svc-b should not converge: %+v", st.Status["svc-b"])
	}

	// 增加站点后 svc-b-0 以原身份补放，两负载都收敛。
	log, _ := f.UpsertSite(Site{Name: "edge-b", Capacity: 10, State: SiteActive})
	logDecision(t, "add edge-b", log)
	st = f.Snapshot()
	logStatus(t, st)
	assertTotal(t, st, 2)
	if countOn(st, "edge-a") != 1 || countOn(st, "edge-b") != 1 {
		t.Errorf("同 key 实例必须分站: %v", instanceMap(st))
	}
}

// TestUpdatePersistence 声明调整落盘后重新加载，结论与调整前一致。
func TestUpdatePersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	store := NewStore(path)

	f1, err := Open(store)
	if err != nil {
		t.Fatal(err)
	}
	f1.UpsertSite(Site{Name: "edge-a", Capacity: 6, State: SiteActive})
	f1.UpsertSite(Site{Name: "edge-b", Capacity: 4, State: SiteActive})
	mustApply(t, f1, WorkloadSpec{Name: "app", Replicas: 2, Size: 2})
	// 调整声明：触发 app-1 迁移。
	mustApply(t, f1, WorkloadSpec{Name: "app", Replicas: 2, Size: 4})
	before := f1.Snapshot()
	logStatus(t, before)
	t.Logf("reload 前: %v migrated=%v", instanceMap(before), before.Status["app"].Migrated)

	// 模拟重启：重新加载，Open 内部先协调一轮。
	f2, err := Open(store)
	if err != nil {
		t.Fatal(err)
	}
	after := f2.Snapshot()
	t.Logf("reload 后: %v", instanceMap(after))

	if fmt.Sprint(instanceMap(before)) != fmt.Sprint(instanceMap(after)) {
		t.Fatalf("reload changed placement: %v -> %v", instanceMap(before), instanceMap(after))
	}
	for id, inst := range before.Instances {
		got, ok := after.Instances[id]
		if !ok || *got != *inst {
			t.Fatalf("instance %s changed after reload: %+v -> %+v", id, inst, got)
		}
	}
	if !after.Status["app"].Converged || after.Status["app"].Placed != 2 {
		t.Errorf("status after reload = %+v, want converged placed=2", after.Status["app"])
	}
}

// TestUpdateConcurrent 声明调整与扩缩容、删除、站点故障/恢复、周期协调并发：
// 不重复、不丢失、可重试，最终按最新声明收敛。
func TestUpdateConcurrent(t *testing.T) {
	f := newFed(t)
	for i := 0; i < 4; i++ {
		f.UpsertSite(Site{Name: fmt.Sprintf("edge-%d", i), Capacity: 12, State: SiteActive})
	}

	var wg sync.WaitGroup
	// 并发写期望状态：Size/MutexKey 调整与扩缩容交织。
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("wl-%d", i%3)
			for j := 1; j <= 4; j++ {
				f.Apply(WorkloadSpec{Name: name, Replicas: 2, Size: j})
				f.Apply(WorkloadSpec{Name: name, Replicas: 2, Size: j, MutexKey: "k"})
				f.Apply(WorkloadSpec{Name: name, Replicas: 2, Size: j})
			}
		}(i)
	}
	// 并发周期协调与站点事件。
	for i := 0; i < 3; i++ {
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

	// 最终声明：3 负载 x 2 副本 x size2，无互斥。
	for i := 0; i < 3; i++ {
		mustApply(t, f, WorkloadSpec{Name: fmt.Sprintf("wl-%d", i), Replicas: 2, Size: 2})
	}
	st := f.Snapshot()
	logStatus(t, st)
	assertCapacity(t, st)
	assertTotal(t, st, 6)
	for name, s := range st.Status {
		if !s.Converged {
			t.Errorf("%s not converged: %+v", name, s)
		}
	}
	if len(st.Rejections) != 0 {
		t.Fatalf("unexpected gap after convergence: %v", st.Rejections)
	}
	// 重试为无操作：并发+重试不放大副作用。
	log, _ := f.Reconcile()
	if len(log) != 0 {
		t.Fatalf("retry reconcile not a no-op: %v", log)
	}
}
