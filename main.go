package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"

	"dbtransfer/internal/config"
	"dbtransfer/internal/migration"
	"dbtransfer/internal/migration/cassandra"
	"dbtransfer/internal/migration/mongodb"
	"dbtransfer/internal/migration/mysql"
	"dbtransfer/internal/migration/postgresql"
)

const VERSION = "0.1"

// 使用 migration 包中的接口
type Migration = migration.Migration

// 添加 loadConfig 函数
func loadConfig(filename string) (*config.Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %v", err)
	}

	var cfg config.Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %v", err)
	}

	// 设置默认值
	cfg.Migration.SetDefaults()

	// 创建必要的目录
	if err := migration.EnsureDir(cfg.Migration.CheckpointDir); err != nil {
		return nil, err
	}
	if cfg.Migration.LogFile != "" {
		if err := migration.EnsureDir(filepath.Dir(cfg.Migration.LogFile)); err != nil {
			return nil, err
		}
	}

	return &cfg, nil
}

// 生成配置文件模板
func generateConfigTemplate(dbType, outputFile string) error {
	var template string

	switch dbType {
	case "mysql":
		template = `source:
  type: mysql
  hosts:
    - "localhost:3306"
  database: "source_db"
  username: "root"
  password: "password"
  tables:
    - name: "table1"
      target_name: "table1_new"
    - name: "table2"

destination:
  type: mysql
  hosts:
    - "localhost:3306"
  database: "dest_db"
  username: "root"
  password: "password"

migration:
  batch_size: 1000
  workers: 4
  rate_limit: 10000
  timeout: 30
  checkpoint_dir: "./data/checkpoints"
  log_file: "./logs/migration.log"
  log_level: "info"
  progress_interval: 10
 `
	case "postgresql":
		template = `source:
  type: postgresql
  hosts:
    - "localhost:5432"
  database: "source_db"
  schema: "public"
  username: "postgres"
  password: "password"
  tables:
    - name: "table1"
      target_name: "table1_new"
    - name: "table2"

destination:
  type: postgresql
  hosts:
    - "localhost:5432"
  database: "dest_db"
  schema: "public"
  username: "postgres"
  password: "password"

migration:
  batch_size: 1000
  workers: 4
  rate_limit: 10000
  timeout: 30
  checkpoint_dir: "./data/checkpoints"
  log_file: "./logs/migration.log"
  log_level: "info"
  progress_interval: 10
 `
	case "mongodb":
		template = `source:
  type: mongodb
  hosts:
    - "localhost:27017"
  database: "source_db"
  username: "admin"
  password: "password"
  tables:
    - name: "collection1"
      target_name: "collection1_new"
    - name: "collection2"

destination:
  type: mongodb
  hosts:
    - "localhost:27017"
  database: "dest_db"
  username: "admin"
  password: "password"

migration:
  batch_size: 500
  workers: 4
  rate_limit: 2000
  timeout: 30
  checkpoint_dir: "./data/checkpoints"
  log_file: "./logs/migration.log"
  log_level: "info"
  progress_interval: 10
 `
	case "cassandra":
		template = `source:
  type: cassandra
  hosts:
    - "localhost:9042"
  keyspace: "source_keyspace"
  username: "cassandra"
  password: "cassandra"
  tables:
    - name: "table1"
      target_name: "table1_new"
    - name: "table2"

destination:
  type: cassandra
  hosts:
    - "localhost:9042"
  keyspace: "dest_keyspace"
  username: "cassandra"
  password: "cassandra"

migration:
  batch_size: 1000
  workers: 8
  rate_limit: 5000
  timeout: 30
  checkpoint_dir: "./data/checkpoints"
  log_file: "./logs/migration.log"
  log_level: "info"
  progress_interval: 10
 `
	default:
		return fmt.Errorf("不支持的数据库类型: %s", dbType)
	}

	// 确保目录存在
	dir := filepath.Dir(outputFile)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("创建目录失败: %v", err)
		}
	}

	// 写入文件
	if err := os.WriteFile(outputFile, []byte(template), 0644); err != nil {
		return fmt.Errorf("写入配置模板失败: %v", err)
	}

	fmt.Printf("配置模板已生成: %s\n", outputFile)
	return nil
}

func main() {
	// 添加版本标志
	var showVersion bool
	flag.BoolVar(&showVersion, "version", false, "显示版本信息")
	configFile := flag.String("config", "config.yaml", "配置文件路径")
	migrationType := flag.String("type", "cassandra", "迁移类型: cassandra/mysql/mongodb/postgresql")
	generateTemplate := flag.Bool("generate-template", false, "生成配置文件模板")
	templateOutput := flag.String("template-output", "config.yaml", "配置模板输出路径")
	flag.Parse()

	// 如果指定了 version 标志，显示版本信息并退出
	if showVersion {
		fmt.Printf("DB Migration Tool v%s\n", VERSION)
		os.Exit(0)
	}

	// 如果指定了生成模板标志
	if *generateTemplate {
		if err := generateConfigTemplate(*migrationType, *templateOutput); err != nil {
			logrus.Fatalf("生成配置模板失败: %v", err)
		}
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
	case "postgresql":
		migration, err = postgresql.NewMigration(config)
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
