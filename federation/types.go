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

// State 是联邦的持久化状态：站点、期望负载、已放置实例、拒绝记录。
type State struct {
	Sites      map[string]*Site         `json:"sites"`
	Desired    map[string]*WorkloadSpec `json:"desired"`
	Instances  map[string]*Instance     `json:"instances"` // key: Instance.ID
	Rejections []Rejection              `json:"rejections,omitempty"`
}

// NewState 返回空状态。
func NewState() *State {
	return &State{
		Sites:     map[string]*Site{},
		Desired:   map[string]*WorkloadSpec{},
		Instances: map[string]*Instance{},
	}
}
