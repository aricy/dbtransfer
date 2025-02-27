package config

type Config struct {
	Source      DBConfig        `yaml:"source"`
	Destination DBConfig        `yaml:"destination"`
	Migration   MigrationConfig `yaml:"migration"`
}

type DBConfig struct {
	Type     string         `yaml:"type"` // 数据库类型：mongodb, mysql, cassandra
	Hosts    []string       `yaml:"hosts"`
	Keyspace string         `yaml:"keyspace"` // Cassandra 使用
	Database string         `yaml:"database"` // MySQL/MongoDB 使用
	Schema   string         `yaml:"schema"`   // PostgreSQL 使用
	Username string         `yaml:"username"`
	Password string         `yaml:"password"`
	Tables   []TableMapping `yaml:"tables,omitempty"`
	AuthDB   string         `yaml:"auth_db"` // 认证数据库，默认为 admin
}

type TableMapping struct {
	Name                  string                 `yaml:"name"`
	TargetName            string                 `yaml:"target_name,omitempty"`
	PrimaryKey            string                 `yaml:"primary_key,omitempty"`
	ColumnTransformations []ColumnTransformation `yaml:"column_transformations,omitempty"`
}

type ColumnTransformation struct {
	SourceColumn string `yaml:"source_column"`
	Expression   string `yaml:"expression"`
}

type MigrationConfig struct {
	BatchSize              int    `yaml:"batch_size"`
	Workers                int    `yaml:"workers"`
	RateLimit              int    `yaml:"rate_limit"`
	Timeout                int    `yaml:"timeout"`
	CheckpointDir          string `yaml:"checkpoint_dir"`
	LogFile                string `yaml:"log_file"`
	LogLevel               string `yaml:"log_level"`
	ProgressInterval       int    `yaml:"progress_interval"`
	Language               string `yaml:"language"`
	CheckpointRowThreshold int    `yaml:"checkpoint_row_threshold"` // 每处理多少行刷新一次断点
	CheckpointInterval     int    `yaml:"checkpoint_interval"`      // 断点刷新时间间隔（秒）
	CheckpointDelay        int    `yaml:"checkpoint_delay"`         // 单位：秒
}

// 设置默认配置
func (c *MigrationConfig) SetDefaults() {
	if c.BatchSize <= 0 {
		c.BatchSize = 1000
	}
	if c.Workers <= 0 {
		c.Workers = 4
	}
	if c.RateLimit <= 0 {
		c.RateLimit = 10000
	}
	if c.Timeout <= 0 {
		c.Timeout = 30
	}
	if c.CheckpointDir == "" {
		c.CheckpointDir = "./data/checkpoints"
	}
	if c.ProgressInterval <= 0 {
		c.ProgressInterval = 10
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	if c.CheckpointRowThreshold <= 0 {
		c.CheckpointRowThreshold = 1000
	}
	if c.CheckpointInterval <= 0 {
		c.CheckpointInterval = 1
	}
	if c.CheckpointDelay <= 0 {
		c.CheckpointDelay = 5
	}
}

func (c *DBConfig) SetDefaults() {
	if len(c.Hosts) == 0 {
		c.Hosts = []string{"localhost:27017"}
	}
	if c.AuthDB == "" {
		c.AuthDB = "admin" // 设置默认认证数据库
	}
}
