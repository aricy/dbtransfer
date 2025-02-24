package main

import (
	"time"

	"dbtransfer/internal/config"
)

// 使用 config 包中的类型
type Config = config.Config
type DBConfig = config.DBConfig
type TableMapping = config.TableMapping
type MigrationConfig = config.MigrationConfig

type Checkpoint struct {
	LastKey     map[string]string `json:"last_key"`
	LastUpdated time.Time         `json:"last_updated"`
	Complete    bool              `json:"complete"`
}
