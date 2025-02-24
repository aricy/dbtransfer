package migration

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// Migration 定义数据迁移接口
type Migration interface {
	Run(ctx context.Context) error
	Close()
}

// TableMapping 定义表映射
type TableMapping struct {
	Name       string `yaml:"name"`
	TargetName string `yaml:"target_name,omitempty"`
}

// Checkpoint 定义断点信息
type Checkpoint struct {
	LastKey     map[string]string `json:"last_key"`
	LastUpdated time.Time         `json:"last_updated"`
	Complete    bool              `json:"complete"`
}

// MigrationStats 定义迁移统计信息
type MigrationStats struct {
	TotalRows     int64
	ProcessedRows int64
	StartTime     time.Time
	Mu            sync.Mutex
}

func (s *MigrationStats) Increment(count int) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.ProcessedRows += int64(count)
}

func (s *MigrationStats) Report() {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	duration := time.Since(s.StartTime)
	rate := float64(s.ProcessedRows) / duration.Seconds()
	logrus.Infof("进度: %d/%d 行 (%.2f%%), 速率: %.2f 行/秒",
		s.ProcessedRows, s.TotalRows,
		float64(s.ProcessedRows)/float64(s.TotalRows)*100, rate)
}
