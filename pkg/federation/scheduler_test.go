package federation

import (
	"reflect"
	"testing"
)

func TestPlanInsufficientCapacity(t *testing.T) {
	in := placementInput{
		sites: []*Site{
			{Name: "edge-a", Capacity: 2, State: SiteActive},
			{Name: "edge-b", Capacity: 1, State: SiteActive},
		},
		workloads: []*Workload{{Name: "web", Replicas: 5, Size: 1}},
	}
	targets, rejections := planPlacement(in)
	t.Logf("期望: 5 副本, 总容量 3 -> 放置 %d, 拒绝 %d", len(targets), len(rejections))
	if len(targets) != 3 {
		t.Fatalf("期望放置 3 个实例, 实际 %d", len(targets))
	}
	if len(rejections) != 2 {
		t.Fatalf("期望 2 条拒绝, 实际 %d", len(rejections))
	}
	for _, r := range rejections {
		t.Logf("拒绝: workload=%s reason=%s detail=%s", r.WorkloadName, r.Reason, r.Detail)
		if r.Reason != RejectInsufficientCapacity {
			t.Fatalf("期望原因 %s, 实际 %s", RejectInsufficientCapacity, r.Reason)
		}
	}
}

func TestPlanLabelMismatch(t *testing.T) {
	in := placementInput{
		sites: []*Site{
			{Name: "edge-a", Capacity: 4, State: SiteActive, Labels: map[string]string{"region": "east"}},
		},
		workloads: []*Workload{{Name: "gpu-job", Replicas: 1, RequiredLabels: map[string]string{"gpu": "true"}}},
	}
	targets, rejections := planPlacement(in)
	t.Logf("期望: 标签 gpu=true 无站点满足 -> 拒绝")
	if len(targets) != 0 || len(rejections) != 1 {
		t.Fatalf("期望 0 放置 1 拒绝, 实际 %d/%d", len(targets), len(rejections))
	}
	t.Logf("拒绝原因: %s (%s)", rejections[0].Reason, rejections[0].Detail)
	if rejections[0].Reason != RejectLabelMismatch {
		t.Fatalf("期望 %s, 实际 %s", RejectLabelMismatch, rejections[0].Reason)
	}
}

func TestPlanMutexConflict(t *testing.T) {
	in := placementInput{
		sites: []*Site{
			{Name: "edge-a", Capacity: 8, State: SiteActive},
		},
		workloads: []*Workload{
			{Name: "db-primary", Replicas: 1, MutexGroup: "db"},
			{Name: "db-standby", Replicas: 1, MutexGroup: "db"},
		},
	}
	targets, rejections := planPlacement(in)
	t.Logf("期望: 单站点, 互斥组 db 内两个不同负载 -> 放置 1, 拒绝 1")
	for _, inst := range targets {
		t.Logf("放置: %s -> %s", inst.ID, inst.SiteName)
	}
	if len(targets) != 1 || len(rejections) != 1 {
		t.Fatalf("期望 1 放置 1 拒绝, 实际 %d/%d", len(targets), len(rejections))
	}
	t.Logf("拒绝原因: %s (%s)", rejections[0].Reason, rejections[0].Detail)
	if rejections[0].Reason != RejectMutexConflict {
		t.Fatalf("期望 %s, 实际 %s", RejectMutexConflict, rejections[0].Reason)
	}
}

func TestPlanMutexSameWorkloadAllowed(t *testing.T) {
	in := placementInput{
		sites:     []*Site{{Name: "edge-a", Capacity: 8, State: SiteActive}},
		workloads: []*Workload{{Name: "db-primary", Replicas: 3, MutexGroup: "db"}},
	}
	targets, rejections := planPlacement(in)
	t.Logf("期望: 同一负载自身副本不受互斥限制 -> 3 全部放置")
	if len(targets) != 3 || len(rejections) != 0 {
		t.Fatalf("期望 3 放置 0 拒绝, 实际 %d/%d", len(targets), len(rejections))
	}
}

func TestPlanDeterministic(t *testing.T) {
	in := placementInput{
		sites: []*Site{
			{Name: "edge-c", Capacity: 2, State: SiteActive, Labels: map[string]string{"tier": "edge"}},
			{Name: "edge-a", Capacity: 3, State: SiteActive},
			{Name: "edge-b", Capacity: 1, State: SiteActive},
		},
		workloads: []*Workload{
			{Name: "w2", Replicas: 2},
			{Name: "w1", Replicas: 3, RequiredLabels: map[string]string{"tier": "edge"}},
		},
	}
	t1, r1 := planPlacement(in)
	t2, r2 := planPlacement(in)
	if !reflect.DeepEqual(t1, t2) || !reflect.DeepEqual(r1, r2) {
		t.Fatalf("重复规划结果不一致:\n%v %v\n%v %v", t1, r1, t2, r2)
	}
	t.Logf("确定性规划结果: %d 放置, %d 拒绝 (w1 仅 1 个站点满足标签)", len(t1), len(r1))
	if len(r1) != 1 || r1[0].Reason != RejectInsufficientCapacity {
		t.Fatalf("期望 w1 因容量不足拒绝 1 个, 实际 %v", r1)
	}
}

func TestPlanKeepsValidExisting(t *testing.T) {
	in := placementInput{
		sites:     []*Site{{Name: "edge-a", Capacity: 4, State: SiteActive}},
		workloads: []*Workload{{Name: "web", Replicas: 2}},
		existing: []*Instance{
			{ID: "web-0", WorkloadName: "web", SiteName: "edge-a"},
			{ID: "web-1", WorkloadName: "web", SiteName: "edge-a"},
		},
	}
	targets, rejections := planPlacement(in)
	if len(rejections) != 0 || len(targets) != 2 {
		t.Fatalf("期望全部保留, 实际 targets=%v rejections=%v", targets, rejections)
	}
	for _, inst := range targets {
		if inst.ID != "web-0" && inst.ID != "web-1" {
			t.Fatalf("既有实例未被保留: %v", inst)
		}
	}
	t.Log("既有合法放置全部保留, 重复协调结论稳定")
}
