package migration

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"dbtransfer/internal/i18n"

	"github.com/sirupsen/logrus"
)

// Migration 定义数据迁移接口
type Migration interface {
	Run(ctx context.Context) error
	Close() error
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
	TotalRows      int64
	ProcessedRows  int64
	StartTime      time.Time
	lastReportTime time.Time
	lastReportRows int64
	mu             sync.Mutex
	stopped        bool
	ticker         *time.Ticker
	tableName      string
}

// NewMigrationStats 创建新的迁移统计信息
func NewMigrationStats(totalRows int64, reportIntervalSeconds int, tableName string) *MigrationStats {
	return &MigrationStats{
		TotalRows:     totalRows,
		ProcessedRows: 0,
		StartTime:     time.Now(),
		ticker:        time.NewTicker(time.Second * time.Duration(reportIntervalSeconds)),
		tableName:     tableName,
	}
}

// Increment 增加已处理行数
func (s *MigrationStats) Increment(count int) {
	s.Lock()
	defer s.Unlock()
	s.ProcessedRows += int64(count)
}

// Lock 获取锁
func (s *MigrationStats) Lock() {
	s.mu.Lock()
}

// Unlock 释放锁
func (s *MigrationStats) Unlock() {
	s.mu.Unlock()
}

// Report 报告迁移进度
func (s *MigrationStats) Report() {
	s.Lock()
	defer s.Unlock()

	now := time.Now()
	duration := now.Sub(s.StartTime)

	// 计算实际速率 - 使用最近一段时间的处理量
	var rate float64
	if s.lastReportTime.IsZero() {
		// 第一次报告，使用从开始到现在的总时间和处理量
		// 如果是从断点继续，只考虑新处理的行数
		processedSinceStart := s.ProcessedRows - s.lastReportRows
		if duration.Seconds() > 0 {
			rate = float64(processedSinceStart) / duration.Seconds()
		}
	} else {
		// 非第一次报告，使用上次报告到现在的时间和处理量
		timeSinceLastReport := now.Sub(s.lastReportTime)
		rowsSinceLastReport := s.ProcessedRows - s.lastReportRows
		if timeSinceLastReport.Seconds() > 0 {
			rate = float64(rowsSinceLastReport) / timeSinceLastReport.Seconds()
		}
	}

	// 更新上次报告时间和行数
	s.lastReportTime = now
	s.lastReportRows = s.ProcessedRows

	var percentage float64
	if s.TotalRows > 0 {
		// 计算已处理的百分比
		percentage = float64(s.ProcessedRows) / float64(s.TotalRows) * 100

		// 计算剩余行数
		remainingRows := s.TotalRows - s.ProcessedRows

		logrus.Infof(i18n.Tr("[%s] 进度: %d/%d 行 (%.2f%%), 剩余: %d 行, 速率: %.2f 行/秒, 已用时间: %v",
			"[%s] Progress: %d/%d rows (%.2f%%), Remaining: %d rows, Rate: %.2f rows/sec, Elapsed time: %v"),
			s.tableName, s.ProcessedRows, s.TotalRows, percentage, remainingRows, rate, duration.Round(time.Second))
	} else {
		// 对于不提供总行数的数据库，显示已处理行数和速率
		logrus.Infof(i18n.Tr("[%s] 已处理: %d 行, 速率: %.2f 行/秒, 已用时间: %v",
			"[%s] Processed: %d rows, Rate: %.2f rows/sec, Elapsed time: %v"),
			s.tableName, s.ProcessedRows, rate, duration.Round(time.Second))
	}
}

// 确保目录存在
func EnsureDir(dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf(i18n.Tr("创建目录失败 %s: %v", "Failed to create directory %s: %v"), dir, err)
	}
	return nil
}

// 添加停止报告的方法
func (s *MigrationStats) StopReporting() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = true
	if s.ticker != nil {
		s.ticker.Stop()
	}
}

// 修改 StartReporting 方法
func (s *MigrationStats) StartReporting(interval time.Duration) {
	s.Lock()
	defer s.Unlock()

	// 如果已经在报告中，先停止
	if s.ticker != nil {
		s.ticker.Stop()
	}

	s.ticker = time.NewTicker(interval)
	go func() {
		for range s.ticker.C {
			s.Report()
		}
	}()
}

// ResetStartTime 重置起始时间，用于从断点继续时
func (s *MigrationStats) ResetStartTime(fromCheckpoint bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.StartTime = time.Now()
	s.lastReportTime = time.Time{} // 重置为零值

	// 如果是从断点继续，将 lastReportRows 设置为当前的 ProcessedRows
	// 这样第一次报告时，速率计算将只考虑新处理的行数
	if fromCheckpoint {
		s.lastReportRows = s.ProcessedRows
	} else {
		s.lastReportRows = 0
	}
}

// InitLogger 初始化日志系统
func InitLogger(logFile string, logLevel string) error {
	// 使用 i18n 中的格式化器
	i18n.InitLogger()

	// 如果配置了日志文件，则同时输出到文件和控制台
	if logFile != "" {
		// 创建日志目录
		if err := os.MkdirAll(filepath.Dir(logFile), 0755); err != nil {
			return fmt.Errorf(i18n.Tr("创建日志目录失败: %v", "Failed to create log directory: %v"), err)
		}

		// 创建或打开日志文件
		file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf(i18n.Tr("打开日志文件失败: %v", "Failed to open log file: %v"), err)
		}

		// 创建多输出
		mw := io.MultiWriter(os.Stdout, file)
		logrus.SetOutput(mw)
	}

	// 设置日志级别
	if level, err := logrus.ParseLevel(logLevel); err == nil {
		logrus.SetLevel(level)
	} else {
		logrus.SetLevel(logrus.InfoLevel)
	}

	return nil
}

// 添加全局限速器
var (
	globalLimiter struct {
		sync.Mutex
		processed int64
		lastTick  time.Time
		rateLimit int
	}
)

// InitGlobalLimiter 初始化全局限速器
func InitGlobalLimiter(rateLimit int) {
	globalLimiter.Lock()
	defer globalLimiter.Unlock()

	globalLimiter.rateLimit = rateLimit
	globalLimiter.lastTick = time.Now()
	globalLimiter.processed = 0
}

// EnforceGlobalRateLimit 实现全局限速
func EnforceGlobalRateLimit(ctx context.Context, count int) error {
	if globalLimiter.rateLimit <= 0 {
		return nil
	}

	globalLimiter.Lock()
	defer globalLimiter.Unlock()

	globalLimiter.processed += int64(count)
	now := time.Now()
	elapsed := now.Sub(globalLimiter.lastTick)

	currentRate := float64(globalLimiter.processed) / elapsed.Seconds()

	if currentRate > float64(globalLimiter.rateLimit) {
		idealTime := time.Duration(float64(globalLimiter.processed) / float64(globalLimiter.rateLimit) * float64(time.Second))
		waitTime := idealTime - elapsed
		if waitTime > 0 {
			logrus.Debugf(i18n.Tr("全局限流: 总处理行数 %d，当前速率 %.2f 行/秒，限制 %d 行/秒，等待 %v",
				"Global rate limiting: Total processed %d rows, current rate %.2f rows/sec, limit %d rows/sec, waiting %v"),
				globalLimiter.processed, currentRate, globalLimiter.rateLimit, waitTime)

			select {
			case <-time.After(waitTime):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	if elapsed > 10*time.Second {
		globalLimiter.processed = int64(count)
		globalLimiter.lastTick = now
	}

	return nil
}
