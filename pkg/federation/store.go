package federation

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// Store 以 JSON 文件持久化联邦状态（站点、期望负载、实际实例）。
// 所有变更方法持有互斥锁，Save 通过临时文件 + rename 保证原子性。
type Store struct {
	mu        sync.Mutex
	path      string
	Sites     map[string]*Site     `json:"sites"`
	Workloads map[string]*Workload `json:"workloads"`
	Instances map[string]*Instance `json:"instances"`
}

// NewStore 创建空存储（内存态）。
func NewStore(path string) *Store {
	return &Store{
		path:      path,
		Sites:     map[string]*Site{},
		Workloads: map[string]*Workload{},
		Instances: map[string]*Instance{},
	}
}

// snapshot 返回当前状态的深拷贝，供协调器在锁外计算。
func (s *Store) snapshot() (sites []*Site, workloads []*Workload, instances []*Instance) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range s.Sites {
		cp := *v
		sites = append(sites, &cp)
	}
	for _, v := range s.Workloads {
		cp := *v
		workloads = append(workloads, &cp)
	}
	for _, v := range s.Instances {
		cp := *v
		instances = append(instances, &cp)
	}
	return sites, workloads, instances
}

// applyInstances 以计算结果整体替换实例集合（由协调器调用）。
func (s *Store) applyInstances(instances []*Instance) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Instances = map[string]*Instance{}
	for _, inst := range instances {
		cp := *inst
		s.Instances[inst.ID] = &cp
	}
}

// Save 将状态原子写入磁盘。
func (s *Store) Save() error {
	s.mu.Lock()
	data, err := json.MarshalIndent(s, "", "  ")
	s.mu.Unlock()
	if err != nil {
		return fmt.Errorf("marshal store: %w", err)
	}
	if s.path == "" {
		return nil // 纯内存模式
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write tmp: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

// LoadStore 从磁盘加载；文件不存在时返回空存储。
func LoadStore(path string) (*Store, error) {
	data, err := os.ReadFile(filepath.Clean(path))
	if errors.Is(err, fs.ErrNotExist) {
		return NewStore(path), nil
	}
	if err != nil {
		return nil, fmt.Errorf("read store: %w", err)
	}
	st := NewStore(path)
	if err := json.Unmarshal(data, st); err != nil {
		return nil, fmt.Errorf("unmarshal store: %w", err)
	}
	st.path = path
	if st.Sites == nil {
		st.Sites = map[string]*Site{}
	}
	if st.Workloads == nil {
		st.Workloads = map[string]*Workload{}
	}
	if st.Instances == nil {
		st.Instances = map[string]*Instance{}
	}
	return st, nil
}
