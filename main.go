package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
)

const VERSION = "0.1"

type Migration interface {
	Run(ctx context.Context) error
	Close()
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
		migration, err = NewCassandraMigration(config)
	case "mysql":
		migration, err = NewMySQLMigration(config)
	case "mongodb":
		migration, err = NewMongoDBMigration(config)
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
