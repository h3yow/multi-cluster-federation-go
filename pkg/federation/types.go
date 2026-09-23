// Package federation 提供本地可运行的多集群工作负载联邦编排组件。
package federation

// SiteState 表示站点可用性状态。
type SiteState string

const (
	SiteActive   SiteState = "Active"
	SiteDraining SiteState = "Draining"
	SiteFailed   SiteState = "Failed"
)

// Site 表示联邦中的一个边缘站点。
type Site struct {
	Name     string            `json:"name"`
	Capacity int               `json:"capacity"` // 可承载的容量单元总数
	Labels   map[string]string `json:"labels,omitempty"`
	State    SiteState         `json:"state"`
}

// Workload 是声明式的期望负载。
type Workload struct {
	Name           string            `json:"name"`
	Replicas       int               `json:"replicas"`
	Size           int               `json:"size"` // 每个实例占用的容量单元，<=0 视为 1
	RequiredLabels map[string]string `json:"requiredLabels,omitempty"`
	MutexGroup     string            `json:"mutexGroup,omitempty"` // 同组负载的实例不得同站点共存
}

// Instance 是已放置到站点上的实际实例。
type Instance struct {
	ID           string `json:"id"`
	WorkloadName string `json:"workloadName"`
	SiteName     string `json:"siteName"`
}

// RejectReason 区分放置失败原因。
type RejectReason string

const (
	RejectNone                 RejectReason = ""
	RejectInsufficientCapacity RejectReason = "InsufficientCapacity"
	RejectLabelMismatch        RejectReason = "LabelMismatch"
	RejectMutexConflict        RejectReason = "MutexConflict"
	RejectNoEligibleSite       RejectReason = "NoEligibleSite"
)

// Rejection 记录一个无法放置的实例及其原因。
type Rejection struct {
	WorkloadName string       `json:"workloadName"`
	Reason       RejectReason `json:"reason"`
	Detail       string       `json:"detail,omitempty"`
}

// Report 是一次协调的结论，落盘后重载应得到一致结果。
type Report struct {
	Placed    []Instance  `json:"placed,omitempty"`
	Removed   []Instance  `json:"removed,omitempty"`
	Kept      []Instance  `json:"kept,omitempty"`
	Rejected  []Rejection `json:"rejected,omitempty"`
	Stable    bool        `json:"stable"` // 本次协调是否未产生任何变更
}
