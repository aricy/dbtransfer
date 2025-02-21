package main

import "time"

type Checkpoint struct {
	LastKey     map[string]string `json:"last_key"`
	LastUpdated time.Time         `json:"last_updated"`
	Complete    bool              `json:"complete"`
}

type DBConfig struct {
	Hosts    []string       `yaml:"hosts"`
	Keyspace string         `yaml:"keyspace"` // Cassandra 使用
	Database string         `yaml:"database"` // MySQL 使用
	Username string         `yaml:"username"`
	Password string         `yaml:"password"`
	Tables   []TableMapping `yaml:"tables,omitempty"`
}
