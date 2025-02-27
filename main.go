package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"

	"dbtransfer/internal/config"
	"dbtransfer/internal/i18n"
	"dbtransfer/internal/migration"
	"dbtransfer/internal/migration/cassandra"
	"dbtransfer/internal/migration/mongodb"
	"dbtransfer/internal/migration/mysql"
	"dbtransfer/internal/migration/postgresql"
)

const VERSION = "0.2"

// 使用 migration 包中的接口
type Migration = migration.Migration

// 添加 loadConfig 函数
func loadConfig(filename string) (*config.Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf(i18n.Tr("读取配置文件失败: %v", "Failed to read config file: %v"), err)
	}

	var cfg config.Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf(i18n.Tr("解析配置文件失败: %v", "Failed to parse config file: %v"), err)
	}

	// 设置默认值
	cfg.Migration.SetDefaults()

	// 创建必要的目录
	if err := migration.EnsureDir(cfg.Migration.CheckpointDir); err != nil {
		return nil, err
	}
	if cfg.Migration.LogFile != "" {
		if err := migration.EnsureDir(strings.TrimSuffix(cfg.Migration.LogFile, filepath.Ext(cfg.Migration.LogFile))); err != nil {
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
  language: "zh"
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
  language: "zh"
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
  language: "zh"
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
  language: "zh"
 `
	default:
		return fmt.Errorf(i18n.Tr("不支持的数据库类型: %s", "Unsupported database type: %s"), dbType)
	}

	// 确保目录存在
	dir := strings.TrimSuffix(outputFile, filepath.Ext(outputFile))
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf(i18n.Tr("创建目录失败: %v", "Failed to create directory: %v"), err)
		}
	}

	// 写入文件
	if err := os.WriteFile(outputFile, []byte(template), 0644); err != nil {
		return fmt.Errorf(i18n.Tr("写入配置模板失败: %v", "Failed to write config template: %v"), err)
	}

	fmt.Printf(i18n.Tr("配置模板已生成: %s\n", "Config template generated: %s\n"), outputFile)
	return nil
}

func main() {
	// 添加版本标志
	var showVersion bool
	flag.BoolVar(&showVersion, "version", false, i18n.Tr("显示版本信息", "Show version information"))
	configFile := flag.String("config", "config.yaml", i18n.Tr("配置文件路径", "Path to config file"))
	migrationType := flag.String("type", "", i18n.Tr("迁移类型 (mysql, postgresql, mongodb, cassandra)", "Migration type (mysql, postgresql, mongodb, cassandra)"))
	generateTemplate := flag.Bool("generate-template", false, i18n.Tr("生成配置文件模板", "Generate config file template"))
	templateOutput := flag.String("template-output", "", i18n.Tr("配置模板输出路径", "Output path for config template"))
	language := flag.String("language", "", i18n.Tr("界面语言 (zh: 中文, en: 英文)", "Interface language (zh: Chinese, en: English)"))
	flag.Parse()

	// 如果指定了 version 标志，显示版本信息并退出
	if showVersion {
		fmt.Printf(i18n.Tr("数据库迁移工具 v%s\n", "DB Migration Tool v%s\n"), VERSION)
		os.Exit(0)
	}

	// 如果指定了生成模板标志
	if *generateTemplate {
		if err := generateConfigTemplate(*migrationType, *templateOutput); err != nil {
			logrus.Fatalf(i18n.Tr("生成配置模板失败: %v", "Failed to generate config template: %v"), err)
		}
		return
	}

	// 设置日志格式
	setupLogging()

	// 如果命令行指定了语言，优先使用命令行指定的语言
	if *language != "" {
		i18n.SetLanguage(*language)
		logrus.Infof(i18n.Tr("使用命令行指定的语言: %s", "Using language specified by command line: %s"), *language)
	}

	// 加载配置
	cfg, err := loadConfig(*configFile)
	if err != nil {
		logrus.Fatalf(i18n.Tr("加载配置失败: %v", "Failed to load config: %v"), err)
	}

	// 初始化国际化 - 只有当命令行没有指定语言时，才使用配置文件中的语言设置
	if *language == "" && cfg.Migration.Language != "" {
		i18n.SetLanguage(cfg.Migration.Language)
		logrus.Infof(i18n.Tr("使用配置文件指定的语言: %s", "Using language specified in config file: %s"), cfg.Migration.Language)
	}

	// 显示最终使用的语言
	lang := i18n.GetLanguage()
	var langSource string
	if *language != "" {
		langSource = i18n.Tr("命令行参数", "command line")
	} else if cfg.Migration.Language != "" {
		langSource = i18n.Tr("配置文件", "configuration file")
	} else {
		langSource = i18n.Tr("系统环境自动检测", "system environment auto-detection")
	}
	logrus.Infof(i18n.Tr("使用语言: %s (来源: %s)", "Using language: %s (source: %s)"), lang, langSource)

	// 设置默认值
	cfg.Migration.SetDefaults()

	// 创建迁移实例
	var m Migration
	if *migrationType != "" {
		// 使用命令行指定的类型
		m, err = createMigration(*migrationType, cfg)
	} else if cfg.Source.Type != "" {
		// 使用配置文件中指定的类型
		m, err = createMigration(cfg.Source.Type, cfg)
	} else {
		// 尝试自动检测
		m, err = autoDetectMigration(cfg)
	}

	if err != nil {
		logrus.Fatalf(i18n.Tr("创建迁移实例失败: %v", "Failed to create migration instance: %v"), err)
	}
	defer m.Close()

	// 创建上下文，支持取消
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 处理信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logrus.Info(i18n.Tr("收到中断信号，正在优雅退出...", "Received interrupt signal, gracefully shutting down..."))
		cancel()
		// 给一些时间完成当前批次
		time.Sleep(2 * time.Second)
		os.Exit(0)
	}()

	// 执行迁移
	startTime := time.Now()
	logrus.Info(i18n.Tr("开始数据迁移...", "Starting data migration..."))
	if err := m.Run(ctx); err != nil {
		logrus.Fatalf(i18n.Tr("迁移失败: %v", "Migration failed: %v"), err)
	}
	duration := time.Since(startTime)
	logrus.Infof(i18n.Tr("迁移完成，总耗时: %v", "Migration completed, total time: %v"), duration)
}

func setupLogging() {
	// 设置日志格式
	i18n.InitLogger()
}

// 根据类型创建迁移实例
func createMigration(migrationType string, cfg *config.Config) (migration.Migration, error) {
	switch migrationType {
	case "mysql":
		return mysql.NewMigration(cfg)
	case "postgresql":
		return postgresql.NewMigration(cfg)
	case "mongodb":
		return mongodb.NewMigration(cfg)
	case "cassandra":
		return cassandra.NewMigration(cfg)
	default:
		return nil, fmt.Errorf(i18n.Tr("不支持的迁移类型: %s", "Unsupported migration type: %s"), migrationType)
	}
}

// 自动检测迁移类型
func autoDetectMigration(cfg *config.Config) (migration.Migration, error) {
	// 根据源数据库连接信息尝试检测类型
	host := ""
	if len(cfg.Source.Hosts) > 0 {
		host = cfg.Source.Hosts[0]
	}

	if strings.Contains(host, ":27017") {
		logrus.Info(i18n.Tr("自动检测到 MongoDB 数据库", "Automatically detected MongoDB database"))
		return mongodb.NewMigration(cfg)
	} else if strings.Contains(host, ":9042") {
		logrus.Info(i18n.Tr("自动检测到 Cassandra 数据库", "Automatically detected Cassandra database"))
		return cassandra.NewMigration(cfg)
	} else if strings.Contains(host, ":5432") {
		logrus.Info(i18n.Tr("自动检测到 PostgreSQL 数据库", "Automatically detected PostgreSQL database"))
		return postgresql.NewMigration(cfg)
	} else {
		// 默认使用 MySQL
		logrus.Info(i18n.Tr("默认使用 MySQL 数据库", "Using MySQL database by default"))
		return mysql.NewMigration(cfg)
	}
}
