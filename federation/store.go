package federation

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// Store 负责状态的持久化与加载（本地 JSON 文件模拟中心集群存储）。
type Store struct {
	path string
}

// NewStore 创建以 path 为落盘文件的存储。
func NewStore(path string) *Store {
	return &Store{path: path}
}

// Load 从磁盘加载状态；文件不存在时返回空状态。
func (s *Store) Load() (*State, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return NewState(), nil
	}
	if err != nil {
		return nil, err
	}
	st := NewState()
	if err := json.Unmarshal(data, st); err != nil {
		return nil, err
	}
	// 防御：JSON 中 null map 会导致后续写入 panic。
	if st.Sites == nil {
		st.Sites = map[string]*Site{}
	}
	if st.Desired == nil {
		st.Desired = map[string]*WorkloadSpec{}
	}
	if st.Instances == nil {
		st.Instances = map[string]*Instance{}
	}
	return st, nil
}

// Save 将状态原子写入磁盘（先写临时文件再 rename，避免半截文件）。
func (s *Store) Save(st *State) error {
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
