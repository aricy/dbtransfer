package migration

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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

// 定义日志级别的颜色代码
var levelColors = map[logrus.Level]string{
	logrus.DebugLevel: "\033[36m", // 青色
	logrus.InfoLevel:  "\033[32m", // 绿色
	logrus.WarnLevel:  "\033[33m", // 黄色
	logrus.ErrorLevel: "\033[31m", // 红色
	logrus.FatalLevel: "\033[35m", // 紫色
	logrus.PanicLevel: "\033[31m", // 红色
}

// 定义颜色重置代码
const colorReset = "\033[0m"

// 自定义格式化器
type customFormatter struct {
	logrus.TextFormatter
}

func (f *customFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	// 获取时间戳
	timestamp := entry.Time.Format("2006-01-02 15:04:05")

	// 获取日志级别（大写，固定4位宽度）
	levelText := strings.ToUpper(entry.Level.String())
	for len(levelText) < 4 {
		levelText += " "
	}
	if len(levelText) > 4 {
		levelText = levelText[:4]
	}

	// 获取颜色代码
	color := levelColors[entry.Level]

	// 构建日志消息（带颜色）
	msg := fmt.Sprintf("[%s] %s%s%s %s\n",
		timestamp,
		color,
		levelText,
		colorReset,
		entry.Message)

	return []byte(msg), nil
}

// InitLogger 初始化日志配置
func InitLogger(logFile string, logLevel string) error {
	// 设置自定义格式化器
	logrus.SetFormatter(&customFormatter{
		TextFormatter: logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: "2006-01-02 15:04:05",
		},
	})

	// 如果配置了日志文件，则同时输出到文件和控制台
	if logFile != "" {
		// 创建日志目录
		if err := os.MkdirAll(filepath.Dir(logFile), 0755); err != nil {
			return fmt.Errorf("创建日志目录失败: %v", err)
		}

		// 创建或打开日志文件
		file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf("打开日志文件失败: %v", err)
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

// 确保目录存在
func EnsureDir(dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("创建目录失败 %s: %v", dir, err)
	}
	return nil
}
