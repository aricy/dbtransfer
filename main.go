package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"

	"dbtransfer/internal/migration"
	"dbtransfer/internal/migration/cassandra"
	"dbtransfer/internal/migration/mongodb"
	"dbtransfer/internal/migration/mysql"
)

const VERSION = "0.1"

// 使用 migration 包中的接口
type Migration = migration.Migration

// 添加 loadConfig 函数
func loadConfig(filename string) (*Config, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %v", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %v", err)
	}

	return &config, nil
}

func main() {
	// 添加版本标志
	var showVersion bool
	flag.BoolVar(&showVersion, "version", false, "显示版本信息")
	configFile := flag.String("config", "config.yaml", "配置文件路径")
	migrationType := flag.String("type", "cassandra", "迁移类型: cassandra/mysql/mongodb")
	flag.Parse()

	// 如果指定了 version 标志，显示版本信息并退出
	if showVersion {
		fmt.Printf("DB Migration Tool v%s\n", VERSION)
		os.Exit(0)
	}

	// 设置日志格式
	setupLogging()

	// 加载配置
	config, err := loadConfig(*configFile)
	if err != nil {
		logrus.Fatalf("加载配置失败: %v", err)
	}

	// 根据迁移类型创建相应的迁移实例
	var migration Migration
	switch *migrationType {
	case "cassandra":
		migration, err = cassandra.NewMigration(config)
	case "mysql":
		migration, err = mysql.NewMigration(config)
	case "mongodb":
		migration, err = mongodb.NewMigration(config)
	default:
		logrus.Fatalf("不支持的迁移类型: %s", *migrationType)
	}

	if err != nil {
		logrus.Fatalf("初始化迁移失败: %v", err)
	}
	defer migration.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := migration.Run(ctx); err != nil {
		logrus.Fatalf("迁移失败: %v", err)
	}

	logrus.Info("迁移完成")
}

func setupLogging() {
	// 设置日志格式...（从原来的 main.go 移动过来）
}
