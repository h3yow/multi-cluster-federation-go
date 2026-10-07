package federation

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

// --- 更新语义专用辅助 ---

// logRound 打印一轮协调的期望状态、实例摆放、迁移与缺口。
func logRound(t *testing.T, tag string, f *Federation, spec WorkloadSpec, log []string) {
	t.Helper()
	st := f.Snapshot()
	t.Logf("== [%s] desired name=%s replicas=%d size=%d mutex=%q gen=%d",
		tag, spec.Name, spec.Replicas, spec.Size, spec.MutexKey, st.Generation[spec.Name])
	for _, l := range log {
		t.Logf("    %s", l)
	}
	for _, m := range st.Statuses[spec.Name].Migrations {
		t.Logf("    MIGRATION %s %s -> %s (%s)", m.InstanceID, m.From, m.To, m.Reason)
	}
	for _, r := range st.Rejections {
		if r.Workload == spec.Name {
			t.Logf("    REJECT ordinal=%d reason=%s detail=%s", r.Ordinal, r.Reason, r.Detail)
		}
	}
	t.Logf("    placement=%v", instanceMap(st))
}

func idsOf(st *State) []string {
	out := make([]string, 0, len(st.Instances))
	for id := range st.Instances {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func siteOf(st *State, id string) string {
	if inst, ok := st.Instances[id]; ok {
		return inst.Site
	}
	return "<missing>"
}

// TestUpdateSizeFitsExactly 调大每实例容量，站点容量【刚好够】：
// 全部实例原地按新规格保留、不搬家、不超配、状态收敛。
func TestUpdateSizeFitsExactly(t *testing.T) {
	f := newFed(t)
	f.UpsertSite(Site{Name: "edge-a", Capacity: 4, State: SiteActive})
	f.UpsertSite(Site{Name: "edge-b", Capacity: 4, State: SiteActive})

	mustApply(t, f, WorkloadSpec{Name: "app", Replicas: 4, Size: 1})
	before := f.Snapshot()
	if usedOn(before, "edge-a") != 2 || usedOn(before, "edge-b") != 2 {
		t.Fatalf("initial placement unexpected: a=%d b=%d", usedOn(before, "edge-a"), usedOn(before, "edge-b"))
	}
	beforeSites := instanceMap(before)

	// 1 -> 2：每站点 2 个实例 x 2 = 4，恰好等于容量。
	log, _ := f.Apply(WorkloadSpec{Name: "app", Replicas: 4, Size: 2})
	logRound(t, "grow size 1->2 (exact fit)", f, WorkloadSpec{Name: "app", Replicas: 4, Size: 2}, log)

	st := f.Snapshot()
	assertTotal(t, st, 4) // 数量不凭空变化
	assertCapacity(t, st)
	if got := usedOn(st, "edge-a"); got != 4 {
		t.Errorf("edge-a used=%d want 4", got)
	}
	if got := usedOn(st, "edge-b"); got != 4 {
		t.Errorf("edge-b used=%d want 4", got)
	}
	// 身份与站点都不变。
	if fmt.Sprint(instanceMap(st)) != fmt.Sprint(beforeSites) {
		t.Fatalf("instances moved despite exact fit:\n%v\n%v", beforeSites, instanceMap(st))
	}
	for _, inst := range st.Instances {
		if inst.Size != 2 {
			t.Errorf("%s size=%d want 2", inst.ID, inst.Size)
		}
	}
	status := st.Statuses["app"]
	if !status.Converged || status.Rejected != 0 || len(status.Migrations) != 0 {
		t.Fatalf("status not converged-in-place: %+v", status)
	}
	if status.DesiredGeneration != 2 || status.ObservedGeneration != 2 {
		t.Fatalf("generation: desired=%d observed=%d want 2/2", status.DesiredGeneration, status.ObservedGeneration)
	}

	// 同样的声明重复 Apply：不递增代次、不产生任何副作用。
	log, _ = f.Apply(WorkloadSpec{Name: "app", Replicas: 4, Size: 2})
	if len(log) != 0 {
		t.Fatalf("identical re-apply must be no-op, got: %v", log)
	}
	// 再协调一轮仍为无操作，摆放稳定、不再迁移。
	log, _ = f.Reconcile()
	logDecision(t, "reconcile again after exact-fit update", log)
	if len(log) != 0 {
		t.Fatalf("repeat reconcile must be no-op, got: %v", log)
	}
}

// TestUpdateSizeTooBig 调大后容量【差一点放不下】：放不下的实例给出可解释缺口，
// 其余实例原地保留，数量不凭空变化，反复协调结论稳定。
func TestUpdateSizeTooBig(t *testing.T) {
	f := newFed(t)
	f.UpsertSite(Site{Name: "edge-a", Capacity: 6, State: SiteActive})
	f.UpsertSite(Site{Name: "edge-b", Capacity: 5, State: SiteActive})

	mustApply(t, f, WorkloadSpec{Name: "app", Replicas: 4, Size: 1})
	before := f.Snapshot()
	t.Logf("initial placement: %v", instanceMap(before))
	// 初始（按剩余容量均衡）：a=app-0,app-1,app-3(used3)，b=app-2(used1)。

	// 1 -> 3：总需求 12，总容量 11（差 1 个单位放不下第 4 个）。
	// 确定性结果：app-0/app-1 留 a（3+3=6 恰好放满），app-2 留 b（3<=5），
	// app-3 在 a(剩0)/b(剩2) 都放不下 -> 恰好 1 个可解释缺口。
	log, _ := f.Apply(WorkloadSpec{Name: "app", Replicas: 4, Size: 3})
	logRound(t, "grow size 1->3 (short by one)", f, WorkloadSpec{Name: "app", Replicas: 4, Size: 3}, log)

	st := f.Snapshot()
	assertCapacity(t, st)
	assertTotal(t, st, 3) // 仅 3 个放下，不凭空多/少
	if got := rejectReasons(st)[ReasonInsufficientCapacity]; got != 1 {
		t.Fatalf("want 1 InsufficientCapacity, got %v", st.Rejections)
	}
	var gap Rejection
	for _, r := range st.Rejections {
		if r.Workload == "app" {
			gap = r
		}
	}
	if gap.Ordinal != 3 {
		t.Fatalf("gap ordinal=%d want 3 (app-3 is the one that does not fit)", gap.Ordinal)
	}
	// 身份核对：留下的正是 app-0/app-1 在 a、app-2 在 b；app-3 是缺口。
	want := map[string]string{"app-0": "edge-a", "app-1": "edge-a", "app-2": "edge-b"}
	for id, site := range want {
		if got := siteOf(st, id); got != site {
			t.Errorf("%s site=%s want %s", id, got, site)
		}
	}
	if _, ok := st.Instances["app-3"]; ok {
		t.Errorf("app-3 must be a gap, not placed")
	}
	status := st.Statuses["app"]
	if status.Converged || status.Rejected != 1 {
		t.Fatalf("status should show one gap: %+v", status)
	}
	if status.ObservedGeneration != status.DesiredGeneration-1 {
		t.Fatalf("observed gen should trail desired by 1: %+v", status)
	}

	// 反复协调：摆放与缺口结论必须稳定，不产生迁移抖动。
	first := instanceMap(st)
	firstRej := fmt.Sprint(st.Rejections)
	log, _ = f.Reconcile()
	logDecision(t, "reconcile again while still short", log)
	after := f.Snapshot()
	if fmt.Sprint(instanceMap(after)) != fmt.Sprint(first) {
		t.Fatalf("placement churn under shortage:\n%v\n%v", first, instanceMap(after))
	}
	if fmt.Sprint(after.Rejections) != firstRej {
		t.Fatalf("rejection churn under shortage:\n%s\n%s", firstRej, fmt.Sprint(after.Rejections))
	}
	if len(after.Statuses["app"].Migrations) != 0 {
		t.Fatalf("repeat reconcile must not migrate: %+v", after.Statuses["app"].Migrations)
	}

	// 扩容 edge-b 后，缺口在下一轮被补上，且身份仍是 app-3，总数回到 4。
	log, _ = f.UpsertSite(Site{Name: "edge-b", Capacity: 7, State: SiteActive})
	logDecision(t, "grow edge-b to 7", log)
	st = f.Snapshot()
	assertTotal(t, st, 4)
	if got := siteOf(st, "app-3"); got != "edge-b" {
		t.Errorf("app-3 should now be placed on edge-b, got %s", got)
	}
	if !st.Statuses["app"].Converged {
		t.Fatalf("should converge after capacity grows: %+v", st.Statuses["app"])
	}
}

// TestUpdateSizeTriggersMigration 调大后原站点放不下但别的站点放得下：
// 该实例带着原身份迁走，满足的实例留在原站点。
func TestUpdateSizeTriggersMigration(t *testing.T) {
	f := newFed(t)
	// 容量拓扑刻意不对称：c 最大，a 次之，b 最小但恰好能接一个新规格实例。
	f.UpsertSite(Site{Name: "edge-a", Capacity: 5, State: SiteActive})
	f.UpsertSite(Site{Name: "edge-b", Capacity: 4, State: SiteActive})
	f.UpsertSite(Site{Name: "edge-c", Capacity: 7, State: SiteActive})

	mustApply(t, f, WorkloadSpec{Name: "app", Replicas: 3, Size: 1})
	// 初始（剩余容量优先）：c=app-0,app-1(used2)，a=app-2(used1)，b 空着。
	if got := countOn(f.Snapshot(), "edge-c"); got != 2 {
		t.Fatalf("initial: expect 2 replicas on edge-c, got %d", got)
	}

	// 1 -> 4：c 上 app-0 占 4 后只剩 3，放不下 app-1；a 被 app-2 占满剩 1；
	// 空着的 b(4) 恰好接得住 app-1 -> app-1 带着原身份 c -> b 迁移。
	log, _ := f.Apply(WorkloadSpec{Name: "app", Replicas: 3, Size: 4})
	logRound(t, "grow size 1->4 (migrate app-1 to b)", f, WorkloadSpec{Name: "app", Replicas: 3, Size: 4}, log)

	st := f.Snapshot()
	assertTotal(t, st, 3)
	assertCapacity(t, st)
	if siteOf(st, "app-0") != "edge-c" || siteOf(st, "app-2") != "edge-a" {
		t.Fatalf("fitting instances should stay put: %v", instanceMap(st))
	}
	if got := siteOf(st, "app-1"); got != "edge-b" {
		t.Fatalf("app-1 should migrate to edge-b, got %s", got)
	}
	migs := st.Statuses["app"].Migrations
	if len(migs) != 1 || migs[0].InstanceID != "app-1" || migs[0].From != "edge-c" || migs[0].To != "edge-b" {
		t.Fatalf("want single migration app-1 c->b, got %+v", migs)
	}
	if !st.Statuses["app"].Converged {
		t.Fatalf("should converge after migration: %+v", st.Statuses["app"])
	}

	// 再协调：无操作、不回迁。
	log, _ = f.Reconcile()
	if len(log) != 0 {
		t.Fatalf("post-migration reconcile must be no-op, got %v", log)
	}
	again := f.Snapshot()
	if len(again.Statuses["app"].Migrations) != 0 {
		t.Fatalf("migrations list must reset after convergence, got %+v", again.Statuses["app"].Migrations)
	}
}

// TestUpdateMutexKey 运行中增加互斥范围：原本同站的两个实例必须分开，后序号迁出。
func TestUpdateMutexKey(t *testing.T) {
	f := newFed(t)
	f.UpsertSite(Site{Name: "edge-a", Capacity: 10, State: SiteActive})

	mustApply(t, f, WorkloadSpec{Name: "db", Replicas: 2, Size: 1}) // 无互斥、单站：两个都在 edge-a
	if countOn(f.Snapshot(), "edge-a") != 2 {
		t.Fatalf("initial: both replicas should be on edge-a")
	}
	// 增加一个站点：存量实例没有理由搬家，仍都在 a。
	log, _ := f.UpsertSite(Site{Name: "edge-b", Capacity: 10, State: SiteActive})
	logDecision(t, "add edge-b (no movement yet)", log)
	if countOn(f.Snapshot(), "edge-a") != 2 {
		t.Fatalf("adding a site must not move healthy instances")
	}

	// 运行中加互斥范围：原本同站的两个实例必须分开，后序号 db-1 迁出。
	log, _ = f.Apply(WorkloadSpec{Name: "db", Replicas: 2, Size: 1, MutexKey: "leader"})
	logRound(t, "add mutexKey=leader", f, WorkloadSpec{Name: "db", Replicas: 2, Size: 1, MutexKey: "leader"}, log)

	st := f.Snapshot()
	assertTotal(t, st, 2)
	if countOn(st, "edge-a") != 1 || countOn(st, "edge-b") != 1 {
		t.Fatalf("mutex replicas must spread 1/1, got %v", instanceMap(st))
	}
	// 身份不丢：app... db-0 留 a，db-1 迁 b。
	if siteOf(st, "db-0") != "edge-a" || siteOf(st, "db-1") != "edge-b" {
		t.Fatalf("identity-preserving spread expected, got %v", instanceMap(st))
	}
	migs := st.Statuses["db"].Migrations
	if len(migs) != 1 || migs[0].InstanceID != "db-1" || migs[0].To != "edge-b" {
		t.Fatalf("want db-1 migrated to edge-b, got %+v", migs)
	}

	// 反复协调稳定，不再来回挪。
	first := instanceMap(st)
	log, _ = f.Reconcile()
	if len(log) != 0 {
		t.Fatalf("mutex reconcile must be no-op once spread, got %v", log)
	}
	if fmt.Sprint(instanceMap(f.Snapshot())) != fmt.Sprint(first) {
		t.Fatalf("mutex placement churn: %v", instanceMap(f.Snapshot()))
	}

	// 去掉互斥范围也不强制回迁（满足新声明即可，最小扰动），两个实例都仍合法。
	log, _ = f.Apply(WorkloadSpec{Name: "db", Replicas: 2, Size: 1})
	logRound(t, "remove mutexKey", f, WorkloadSpec{Name: "db", Replicas: 2, Size: 1}, log)
	st = f.Snapshot()
	assertTotal(t, st, 2)
	if len(st.Statuses["db"].Migrations) != 0 {
		t.Fatalf("removing mutex should not force migration, got %+v", st.Statuses["db"].Migrations)
	}
}

// TestUpdateMutexKeyNoRoom 增加互斥但只有一个站点：后序号必须给出 MutexConflict 缺口；
// 新增站点后自动按原身份补上。
func TestUpdateMutexKeyNoRoom(t *testing.T) {
	f := newFed(t)
	f.UpsertSite(Site{Name: "edge-a", Capacity: 10, State: SiteActive})
	mustApply(t, f, WorkloadSpec{Name: "db", Replicas: 2, Size: 1})

	log, _ := f.Apply(WorkloadSpec{Name: "db", Replicas: 2, Size: 1, MutexKey: "leader"})
	logRound(t, "add mutex with single site", f, WorkloadSpec{Name: "db", Replicas: 2, Size: 1, MutexKey: "leader"}, log)

	st := f.Snapshot()
	assertTotal(t, st, 1)
	if siteOf(st, "db-0") != "edge-a" {
		t.Fatalf("db-0 should remain on edge-a, got %v", instanceMap(st))
	}
	if got := rejectReasons(st)[ReasonMutexConflict]; got != 1 {
		t.Fatalf("want 1 MutexConflict, got %v", st.Rejections)
	}
	if st.Rejections[0].Ordinal != 1 {
		t.Fatalf("gap must be db-1, got ordinal %d", st.Rejections[0].Ordinal)
	}
	if st.Statuses["db"].Converged {
		t.Fatalf("must not be converged while mutex blocks db-1")
	}

	// 新增站点：缺口按原身份 db-1 补上。
	log, _ = f.UpsertSite(Site{Name: "edge-b", Capacity: 10, State: SiteActive})
	logDecision(t, "add edge-b", log)
	st = f.Snapshot()
	assertTotal(t, st, 2)
	if siteOf(st, "db-1") != "edge-b" {
		t.Fatalf("db-1 should be rebuilt on edge-b, got %s", siteOf(st, "db-1"))
	}
	if !st.Statuses["db"].Converged {
		t.Fatalf("should converge once a second site exists: %+v", st.Statuses["db"])
	}
}

// TestUpdateSizeCrossWorkload 调大一个负载的 Size 时，跨负载的容量占用必须强一致，
// 不能因为只看本负载视角而把共享站点超配。
func TestUpdateSizeCrossWorkload(t *testing.T) {
	f := newFed(t)
	f.UpsertSite(Site{Name: "edge-a", Capacity: 4, State: SiteActive})
	f.UpsertSite(Site{Name: "edge-b", Capacity: 4, State: SiteActive})
	mustApply(t, f, WorkloadSpec{Name: "wx", Replicas: 2, Size: 1})
	mustApply(t, f, WorkloadSpec{Name: "wy", Replicas: 2, Size: 1})
	before := f.Snapshot()
	t.Logf("initial placement: %v", instanceMap(before))
	if usedOn(before, "edge-a") != 2 || usedOn(before, "edge-b") != 2 {
		t.Fatalf("initial usage want a=2 b=2, got a=%d b=%d", usedOn(before, "edge-a"), usedOn(before, "edge-b"))
	}

	// wx 1->3（跨负载）：两站点上 wx 与 wy 的新规格各占 3+1=4，恰好放满，
	// 全局视角必须把 wx 与 wy 的占用一起算，谁也不能超配。
	log, _ := f.Apply(WorkloadSpec{Name: "wx", Replicas: 2, Size: 3})
	logRound(t, "grow wx size 1->3 (cross-workload exact fit)", f, WorkloadSpec{Name: "wx", Replicas: 2, Size: 3}, log)

	st := f.Snapshot()
	assertCapacity(t, st)
	assertTotal(t, st, 4) // 两负载共 4 个，一个不丢
	if usedOn(st, "edge-a") != 4 || usedOn(st, "edge-b") != 4 {
		t.Fatalf("after update both sites used=4, got a=%d b=%d", usedOn(st, "edge-a"), usedOn(st, "edge-b"))
	}
	// 每个站点上必须恰好一个 wx(size3) 与一个 wy(size1)。
	var wxOnA, wyOnA int
	for _, inst := range st.Instances {
		if inst.Site == "edge-a" {
			if inst.Workload == "wx" {
				wxOnA++
			} else {
				wyOnA++
			}
		}
	}
	if wxOnA != 1 || wyOnA != 1 {
		t.Fatalf("edge-a should host 1 wx + 1 wy, got %d wx + %d wy", wxOnA, wyOnA)
	}
	// 两个负载都应收敛（总容量 8 >= wx 6 + wy 2）。
	if !st.Statuses["wx"].Converged || !st.Statuses["wy"].Converged {
		t.Fatalf("both should converge: wx=%+v wy=%+v", st.Statuses["wx"], st.Statuses["wy"])
	}
}

// TestUpdateConcurrent 声明调整与扩缩容、删除、站点故障/恢复、周期协调并发：
// 不重复、不丢实例、不超配、无残留缺口（最终状态全部可收敛）。
func TestUpdateConcurrent(t *testing.T) {
	f := newFed(t)
	for i := 0; i < 4; i++ {
		f.UpsertSite(Site{Name: fmt.Sprintf("edge-%d", i), Capacity: 12, State: SiteActive})
	}
	for i := 0; i < 4; i++ {
		f.Apply(WorkloadSpec{Name: fmt.Sprintf("wl-%d", i), Replicas: 3, Size: 1})
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("wl-%d", i%4)
			// 反复在 size 1/2/3 之间调整声明。
			sizes := []int{2, 1, 3, 1}
			for j, sz := range sizes {
				f.Apply(WorkloadSpec{Name: name, Replicas: 3, Size: sz})
				_ = j
			}
			f.Apply(WorkloadSpec{Name: name, Replicas: 2, Size: 2})
		}(i)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 8; j++ {
				f.Reconcile()
				f.SetSiteState(fmt.Sprintf("edge-%d", i), SiteFailed)
				f.SetSiteState(fmt.Sprintf("edge-%d", i), SiteActive)
				f.Apply(WorkloadSpec{Name: fmt.Sprintf("wl-%d", i), Replicas: 2, Size: 2})
			}
		}(i)
	}
	wg.Wait()

	// 恢复全部站点并声明统一终态：4 负载 x 2 副本 x size2 = 16，容量 48，必收敛。
	for i := 0; i < 4; i++ {
		f.SetSiteState(fmt.Sprintf("edge-%d", i), SiteActive)
		f.Apply(WorkloadSpec{Name: fmt.Sprintf("wl-%d", i), Replicas: 2, Size: 2})
	}
	log, _ := f.Reconcile()
	logDecision(t, "final reconcile after concurrent updates", log)
	st := f.Snapshot()
	assertCapacity(t, st)
	assertTotal(t, st, 8)
	// 按最新声明校验：互斥为空；每个实例 Size=2；数量守恒。
	for _, inst := range st.Instances {
		if inst.Size != 2 {
			t.Errorf("%s size=%d want 2", inst.ID, inst.Size)
		}
	}
	for name, status := range st.Statuses {
		if !status.Converged {
			t.Errorf("%s not converged: %+v", name, status)
		}
	}
	if len(st.Rejections) != 0 {
		t.Fatalf("unexpected final gap: %v", st.Rejections)
	}
	// 重试无副作用。
	log, _ = f.Reconcile()
	if len(log) != 0 {
		t.Fatalf("retry reconcile not a no-op: %v", log)
	}
}

// TestPersistenceAfterUpdate 声明调整并迁移后落盘，重新加载结论一致。
func TestPersistenceAfterUpdate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	store := NewStore(path)

	f1, err := Open(store)
	if err != nil {
		t.Fatal(err)
	}
	f1.UpsertSite(Site{Name: "edge-a", Capacity: 5, State: SiteActive})
	f1.UpsertSite(Site{Name: "edge-b", Capacity: 6, State: SiteActive})
	f1.Apply(WorkloadSpec{Name: "app", Replicas: 3, Size: 1})
	log, _ := f1.Apply(WorkloadSpec{Name: "app", Replicas: 3, Size: 3})
	logDecision(t, "update size before restart", log)
	before := f1.Snapshot()
	t.Logf("before reload: placement=%v migrations=%d", instanceMap(before), len(before.Statuses["app"].Migrations))

	f2, err := Open(store) // 重启：加载并先协调一轮
	if err != nil {
		t.Fatal(err)
	}
	after := f2.Snapshot()
	t.Logf("after reload: placement=%v converged=%v", instanceMap(after), after.Statuses["app"].Converged)

	// 摆放、Size、身份完全一致。
	if fmt.Sprint(instanceMap(before)) != fmt.Sprint(instanceMap(after)) {
		t.Fatalf("placement changed after reload:\n%v\n%v", instanceMap(before), instanceMap(after))
	}
	for id, inst := range before.Instances {
		got := after.Instances[id]
		if got == nil || got.Site != inst.Site || got.Size != inst.Size || got.MutexKey != inst.MutexKey {
			t.Fatalf("instance %s changed after reload: %+v -> %+v", id, inst, got)
		}
	}
	if !after.Statuses["app"].Converged {
		t.Fatalf("after reload should still be converged: %+v", after.Statuses["app"])
	}

	// 兼容旧版本落盘文件：缺少 Generation/Statuses 的状态加载后不 panic，
	// 且能按新逻辑正常协调、补齐收敛状态。
	legacyPath := filepath.Join(t.TempDir(), "legacy.json")
	legacy := `{"sites":{"edge-a":{"name":"edge-a","capacity":4,"state":"Active"}},"desired":{"app":{"name":"app","replicas":1,"size":1}},"instances":{"app-0":{"id":"app-0","workload":"app","ordinal":0,"site":"edge-a","size":1}}}`
	if err := os.WriteFile(legacyPath, []byte(legacy), 0o644); err != nil {
		t.Fatal(err)
	}
	f3, err := Open(NewStore(legacyPath))
	if err != nil {
		t.Fatalf("open legacy state: %v", err)
	}
	st3 := f3.Snapshot()
	if got := siteOf(st3, "app-0"); got != "edge-a" {
		t.Fatalf("legacy app-0 site=%s want edge-a", got)
	}
	if st3.Statuses["app"] == nil || !st3.Statuses["app"].Converged {
		t.Fatalf("legacy state should converge: %+v", st3.Statuses["app"])
	}
}
