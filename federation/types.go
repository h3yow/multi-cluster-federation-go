// Package federation 实现一套本地可运行的多集群工作负载联邦编排组件。
// 中心集群与边缘站点均由本地存储与内存模拟。
package federation

// SiteState 表示站点可用性状态。
type SiteState string

const (
	SiteActive   SiteState = "Active"   // 正常，可调度
	SiteDraining SiteState = "Draining" // 排空中，不再接收新实例，存量需迁出
	SiteFailed   SiteState = "Failed"   // 故障，其上实例视为丢失，需重建
)

// Site 描述一个边缘站点。
type Site struct {
	Name     string            `json:"name"`
	Labels   map[string]string `json:"labels,omitempty"`
	Capacity int               `json:"capacity"` // 可承载的容量单位总数
	State    SiteState         `json:"state"`
}

// WorkloadSpec 是声明式的期望状态。
type WorkloadSpec struct {
	Name     string            `json:"name"`
	Replicas int               `json:"replicas"`
	Size     int               `json:"size"`               // 每个实例占用的容量单位
	Selector map[string]string `json:"selector,omitempty"` // 站点标签约束：站点须包含全部键值
	// MutexKey 非空时，同一站点上不允许存在两个相同 MutexKey 的实例（互斥）。
	MutexKey string `json:"mutexKey,omitempty"`
}

// Instance 是已放置（运行中）的负载实例。
// ID 由负载名与序号确定性派生，保证重复协调结论稳定、恢复后不产生重复。
type Instance struct {
	ID       string `json:"id"`
	Workload string `json:"workload"`
	Ordinal  int    `json:"ordinal"`
	Site     string `json:"site"`
	Size     int    `json:"size"`
	MutexKey string `json:"mutexKey,omitempty"`
}

// RejectReason 区分放置失败的原因。
type RejectReason string

const (
	ReasonNone                 RejectReason = ""
	ReasonInsufficientCapacity RejectReason = "InsufficientCapacity" // 有匹配站点但容量不足
	ReasonLabelMismatch        RejectReason = "LabelMismatch"        // 无站点满足标签约束
	ReasonMutexConflict        RejectReason = "MutexConflict"        // 仅因互斥约束无法放置
	ReasonNoEligibleSite       RejectReason = "NoEligibleSite"       // 无任何可调度站点
)

// Rejection 记录一次无法放置的判定及依据。
type Rejection struct {
	Workload string       `json:"workload"`
	Ordinal  int          `json:"ordinal"`
	Reason   RejectReason `json:"reason"`
	Detail   string       `json:"detail"` // 关键判定依据（各站点逐项评估结果）
}

// Migration 记录一个实例在一轮协调中从原站点迁走的动作。
// 实例 ID 保持不变（迁移不是删除重建），因此可据此核对调整影响面。
type Migration struct {
	InstanceID string `json:"instanceID"`
	Workload   string `json:"workload"`
	Ordinal    int    `json:"ordinal"`
	From       string `json:"from"`
	To         string `json:"to"`
	// Reason 说明为什么原站点不再满足最新声明（如调大 Size 后放不下、互斥范围变更）。
	Reason string `json:"reason"`
}

// WorkloadStatus 是单个负载相对最新声明的收敛状态，每轮协调整体重算。
type WorkloadStatus struct {
	Name string `json:"name"`
	// DesiredGeneration 是最新声明的代次；ObservedGeneration 是当前状态已反映的代次。
	DesiredGeneration  int `json:"desiredGeneration"`
	ObservedGeneration int `json:"observedGeneration"`
	// Replicas 是当前已按最新声明放置的实例数；Rejected 是本轮仍欠的实例数。
	Replicas int `json:"replicas"`
	Rejected int `json:"rejected"`
	// Converged 为 true 表示期望序号全部按最新声明放置、无缺口。
	Converged bool `json:"converged"`
	// Migrations 是本轮协调中被迁走的实例（每轮替换，空表示本轮无迁移）。
	Migrations []Migration `json:"migrations,omitempty"`
}

// State 是联邦的持久化状态：站点、期望负载、已放置实例、拒绝记录。
type State struct {
	Sites      map[string]*Site         `json:"sites"`
	Desired    map[string]*WorkloadSpec `json:"desired"`
	Instances  map[string]*Instance     `json:"instances"` // key: Instance.ID
	Rejections []Rejection              `json:"rejections,omitempty"`
	// Generation 记录每个负载最新声明的代次，声明内容变化时递增。
	Generation map[string]int `json:"generation,omitempty"`
	// Statuses 记录每个负载相对最新声明的收敛状态与本轮迁移。
	Statuses map[string]*WorkloadStatus `json:"statuses,omitempty"`
}

// NewState 返回空状态。
func NewState() *State {
	return &State{
		Sites:      map[string]*Site{},
		Desired:    map[string]*WorkloadSpec{},
		Instances:  map[string]*Instance{},
		Generation: map[string]int{},
		Statuses:   map[string]*WorkloadStatus{},
	}
}
