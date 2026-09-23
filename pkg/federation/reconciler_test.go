package federation

import (
	"fmt"
	"sync"
	"testing"
)

// newTestReconciler 构建 3 站点拓扑，每站点容量 2。
func newTestReconciler(t *testing.T, path string) *Reconciler {
	t.Helper()
	r := NewReconciler(NewStore(path))
	for _, name := range []string{"edge-a", "edge-b", "edge-c"} {
		r.UpsertSite(Site{Name: name, Capacity: 2, State: SiteActive})
	}
	return r
}

func countBySite(insts []Instance) map[string]int {
	m := map[string]int{}
	for _, i := range insts {
		m[i.SiteName]++
	}
	return m
}

func TestSiteFailureAndRecovery(t *testing.T) {
	r := newTestReconciler(t, "")
	r.UpsertWorkload(Workload{Name: "web", Replicas: 3})
	rep, err := r.Reconcile()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("初始协调: placed=%d kept=%d removed=%d rejected=%d",
		len(rep.Placed), len(rep.Kept), len(rep.Removed), len(rep.Rejected))
	t.Logf("分布: %v", countBySite(r.Instances()))

	// 站点故障：实例应在其余站点重建，总数仍为期望值。
	r.SetSiteState("edge-a", SiteFailed)
	rep, _ = r.Reconcile()
	t.Logf("edge-a 故障后: placed=%d removed=%d rejected=%d 分布=%v",
		len(rep.Placed), len(rep.Removed), len(rep.Rejected), countBySite(r.Instances()))
	insts := r.Instances()
	if len(insts) != 3 {
		t.Fatalf("故障迁移后期望 3 实例, 实际 %d", len(insts))
	}
	for _, i := range insts {
		if i.SiteName == "edge-a" {
			t.Fatalf("实例 %s 仍位于故障站点", i.ID)
		}
	}
	// edge-b/edge-c 容量各 2, 3 实例可全部容纳, 不应有拒绝。
	if len(rep.Rejected) != 0 {
		t.Fatalf("容量足够时不应有拒绝: %v", rep.Rejected)
	}

	// 站点恢复：不得出现重复实例，总数保持期望值。
	r.SetSiteState("edge-a", SiteActive)
	rep, _ = r.Reconcile()
	t.Logf("edge-a 恢复后: placed=%d removed=%d stable=%v 分布=%v",
		len(rep.Placed), len(rep.Removed), rep.Stable, countBySite(r.Instances()))
	if got := len(r.Instances()); got != 3 {
		t.Fatalf("恢复后实例总数应为 3, 实际 %d (不得出现重复实例)", got)
	}
	if !rep.Stable {
		t.Fatalf("恢复后既有放置仍合法, 不应发生变更: %+v", rep)
	}
}

func TestSiteFailureInsufficientCapacity(t *testing.T) {
	r := newTestReconciler(t, "")
	r.UpsertWorkload(Workload{Name: "web", Replicas: 6}) // 占满 3 站点
	if _, err := r.Reconcile(); err != nil {
		t.Fatal(err)
	}
	r.SetSiteState("edge-a", SiteFailed)
	rep, _ := r.Reconcile()
	t.Logf("故障后: placed=%d rejected=%d", len(rep.Placed), len(rep.Rejected))
	for _, rej := range rep.Rejected {
		t.Logf("可解释缺口: %s reason=%s", rej.WorkloadName, rej.Reason)
	}
	// 剩余容量 4, 期望 6 -> 缺口 2, 原因须为容量不足。
	if len(rep.Rejected) != 2 || rep.Rejected[0].Reason != RejectInsufficientCapacity {
		t.Fatalf("期望 2 条容量不足拒绝, 实际 %v", rep.Rejected)
	}
	if got := len(r.Instances()); got != 4 {
		t.Fatalf("实际实例应为 4, 实际 %d", got)
	}
}

func TestSiteDraining(t *testing.T) {
	r := newTestReconciler(t, "")
	r.UpsertWorkload(Workload{Name: "web", Replicas: 2})
	if _, err := r.Reconcile(); err != nil {
		t.Fatal(err)
	}
	r.SetSiteState("edge-a", SiteDraining)
	rep, _ := r.Reconcile()
	t.Logf("edge-a 排空: removed=%d placed=%d 分布=%v",
		len(rep.Removed), len(rep.Placed), countBySite(r.Instances()))
	for _, i := range r.Instances() {
		if i.SiteName == "edge-a" {
			t.Fatalf("排空站点不应再承载实例: %s", i.ID)
		}
	}
}

func TestScaleAndDeleteInterleaved(t *testing.T) {
	r := newTestReconciler(t, "")
	r.UpsertWorkload(Workload{Name: "web", Replicas: 2})
	r.UpsertWorkload(Workload{Name: "api", Replicas: 2})
	if _, err := r.Reconcile(); err != nil {
		t.Fatal(err)
	}
	// 扩容 web、缩容 api、删除 api 前再变更、新增 worker，交织后协调。
	r.UpsertWorkload(Workload{Name: "web", Replicas: 4})
	r.UpsertWorkload(Workload{Name: "api", Replicas: 1})
	r.UpsertWorkload(Workload{Name: "worker", Replicas: 1})
	r.DeleteWorkload("api")
	rep, err := r.Reconcile()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("交织变更后: placed=%d removed=%d kept=%d rejected=%v",
		len(rep.Placed), len(rep.Removed), len(rep.Kept), rep.Rejected)
	insts := r.Instances()
	// 期望: web 4 + worker 1 = 5, 总容量 6, 无拒绝。
	if len(insts) != 5 {
		t.Fatalf("期望 5 实例, 实际 %d: %v", len(insts), insts)
	}
	seen := map[string]bool{}
	webCount := 0
	for _, i := range insts {
		if seen[i.ID] {
			t.Fatalf("出现重复实例 ID: %s", i.ID)
		}
		seen[i.ID] = true
		if i.WorkloadName == "web" {
			webCount++
		}
		if i.WorkloadName == "api" {
			t.Fatalf("已删除负载仍有实例: %s", i.ID)
		}
	}
	if webCount != 4 {
		t.Fatalf("web 期望 4 实例, 实际 %d", webCount)
	}
	// 再次协调应稳定（幂等，可安全重试）。
	rep2, _ := r.Reconcile()
	if !rep2.Stable {
		t.Fatalf("重复协调应稳定, 实际 placed=%d removed=%d", len(rep2.Placed), len(rep2.Removed))
	}
	t.Log("重复协调结论稳定, 无副作用放大")
}

func TestConcurrentReconcile(t *testing.T) {
	r := newTestReconciler(t, "")
	r.UpsertWorkload(Workload{Name: "web", Replicas: 3})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// 并发地变更期望状态并触发协调。
			r.UpsertWorkload(Workload{Name: fmt.Sprintf("w%d", i%3), Replicas: 1})
			if _, err := r.Reconcile(); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	// 收敛后再次协调必须稳定。
	rep, err := r.Reconcile()
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Stable {
		t.Fatalf("并发协调收敛后应稳定: placed=%d removed=%d", len(rep.Placed), len(rep.Removed))
	}
	insts := r.Instances()
	seen := map[string]bool{}
	for _, i := range insts {
		if seen[i.ID] {
			t.Fatalf("并发协调产生重复实例: %s", i.ID)
		}
		seen[i.ID] = true
	}
	t.Logf("并发协调收敛: 实例数=%d, 无重复无丢失", len(insts))
}

func TestPersistenceReloadConsistency(t *testing.T) {
	path := t.TempDir() + "/store.json"
	r := newTestReconciler(t, path)
	r.UpsertWorkload(Workload{Name: "web", Replicas: 3})
	if _, err := r.Reconcile(); err != nil {
		t.Fatal(err)
	}
	before := r.Instances()

	// 重新加载后协调，结论必须一致（稳定、无变更）。
	st, err := LoadStore(path)
	if err != nil {
		t.Fatal(err)
	}
	r2 := NewReconciler(st)
	rep, err := r2.Reconcile()
	if err != nil {
		t.Fatal(err)
	}
	after := r2.Instances()
	t.Logf("重载前实例=%v", before)
	t.Logf("重载后实例=%v stable=%v", after, rep.Stable)
	if !rep.Stable {
		t.Fatalf("落盘重载后协调应稳定: %+v", rep)
	}
	if len(before) != len(after) {
		t.Fatalf("重载前后实例数不一致: %d vs %d", len(before), len(after))
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("重载前后实例不一致: %v vs %v", before[i], after[i])
		}
	}
}
