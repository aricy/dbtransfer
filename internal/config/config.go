package config

type Config struct {
    Source      DBConfig        `yaml:"source"`
    Destination DBConfig        `yaml:"destination"`
    Migration   MigrationConfig `yaml:"migration"`
}

type DBConfig struct {
    Type     string         `yaml:"type"`     // 数据库类型：mongodb, mysql, cassandra
    Hosts    []string       `yaml:"hosts"`
    Keyspace string        `yaml:"keyspace"` // Cassandra 使用
    Database string        `yaml:"database"` // MySQL/MongoDB 使用
    Username string        `yaml:"username"`
    Password string        `yaml:"password"`
    Tables   []TableMapping `yaml:"tables,omitempty"`
}

type TableMapping struct {
    Name       string `yaml:"name"`
    TargetName string `yaml:"target_name,omitempty"`
}

type MigrationConfig struct {
    BatchSize       int    `yaml:"batch_size"`
    Workers         int    `yaml:"workers"`
    RateLimit       int    `yaml:"rate_limit"`
    Timeout         int    `yaml:"timeout"`
    CheckpointDir   string `yaml:"checkpoint_dir"`
    LogFile         string `yaml:"log_file"`
    LogLevel        string `yaml:"log_level"`
    CheckpointDelay int    `yaml:"checkpoint_delay"`
}