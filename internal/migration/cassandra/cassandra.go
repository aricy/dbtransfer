package cassandra

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
	"gopkg.in/yaml.v2"

	"dbtransfer/internal/config"
	"dbtransfer/internal/i18n"
	"dbtransfer/internal/migration"
)

type TableMapping struct {
	Name       string `yaml:"name"`
	TargetName string `yaml:"target_name,omitempty"`
}

type MigrationConfig struct {
	BatchSize        int    `yaml:"batch_size"`
	Workers          int    `yaml:"workers"`
	RateLimit        int    `yaml:"rate_limit"`
	Timeout          int    `yaml:"timeout"`
	CheckpointDir    string `yaml:"checkpoint_dir"`
	LogFile          string `yaml:"log_file"`
	LogLevel         string `yaml:"log_level"`
	CheckpointDelay  int    `yaml:"checkpoint_delay"`  // 单位：秒
	ProgressInterval int    `yaml:"progress_interval"` // 进度报告间隔时间（秒）
}

type Config struct {
	Source      config.DBConfig `yaml:"source"`
	Destination config.DBConfig `yaml:"destination"`
	Migration   MigrationConfig `yaml:"migration"`
}

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
	duration := time.Since(s.StartTime)
	rate := float64(s.ProcessedRows) / duration.Seconds()
	logrus.Infof(i18n.Tr(
		"已处理: %d/%d 行 (%.2f%%), 速率: %.2f 行/秒",
		"Processed: %d/%d rows (%.2f%%), rate: %.2f rows/sec"),
		s.ProcessedRows, s.TotalRows,
		float64(s.ProcessedRows)/float64(s.TotalRows)*100, rate)
}

type CassandraMigration struct {
	source             *gocql.Session
	dest               *gocql.Session
	config             *config.Config
	limiter            *rate.Limiter
	statsMap           map[string]*migration.MigrationStats
	statsMutex         sync.RWMutex
	maxRetries         int
	retryDelay         time.Duration
	lastID             interface{}
	lastCheckpointTime time.Time // 上次保存断点的时间
}

type TokenRange struct {
	Start    int64             `json:"start"`
	End      int64             `json:"end"`
	LastKey  map[string]string `json:"last_key,omitempty"`
	Complete bool              `json:"complete"`
}

func NewMigration(config *config.Config) (migration.Migration, error) {
	// 初始化日志
	if err := migration.InitLogger(config.Migration.LogFile, config.Migration.LogLevel); err != nil {
		return nil, fmt.Errorf(i18n.Tr("初始化日志失败: %v", "Failed to initialize logger: %v"), err)
	}

	if config.Source.Keyspace == "" || config.Destination.Keyspace == "" {
		return nil, fmt.Errorf(i18n.Tr("未配置 keyspace", "Keyspace not configured"))
	}

	// 先验证配置
	if err := validateConfig(config); err != nil {
		return nil, err
	}

	sourceCluster := createClusterConfig(config.Source)
	destCluster := createClusterConfig(config.Destination)

	sourceSession, err := sourceCluster.CreateSession()
	if err != nil {
		logrus.Errorf(i18n.Tr("连接源 Cassandra 失败: %v", "Failed to connect to source Cassandra: %v"), err)
		return nil, fmt.Errorf(i18n.Tr("连接源数据库失败: %v", "Failed to connect to source database: %v"), err)
	}

	destSession, err := destCluster.CreateSession()
	if err != nil {
		sourceSession.Close()
		logrus.Errorf(i18n.Tr("连接目标 Cassandra 失败: %v", "Failed to connect to destination Cassandra: %v"), err)
		return nil, fmt.Errorf(i18n.Tr("连接目标数据库失败: %v", "Failed to connect to destination database: %v"), err)
	}

	// 验证连接
	if err := validateConnection(sourceSession, config.Source.Keyspace); err != nil {
		sourceSession.Close()
		destSession.Close()
		return nil, fmt.Errorf(i18n.Tr("验证源数据库连接失败: %v", "Failed to validate source database connection: %v"), err)
	}

	if err := validateConnection(destSession, config.Destination.Keyspace); err != nil {
		sourceSession.Close()
		destSession.Close()
		return nil, fmt.Errorf(i18n.Tr("验证目标数据库连接失败: %v", "Failed to validate destination database connection: %v"), err)
	}

	logrus.Info(i18n.Tr("Cassandra 连接成功", "Cassandra connection successful"))

	// 创建限流器
	var limiter *rate.Limiter
	if config.Migration.RateLimit > 0 {
		burstSize := config.Migration.BatchSize / 2
		if burstSize < 1 {
			burstSize = 1
		}
		limiter = rate.NewLimiter(rate.Limit(config.Migration.RateLimit), burstSize)
	}

	// 初始化全局限速器
	migration.InitGlobalLimiter(config.Migration.RateLimit)

	return &CassandraMigration{
		source:             sourceSession,
		dest:               destSession,
		config:             config,
		limiter:            limiter,
		statsMap:           make(map[string]*migration.MigrationStats),
		maxRetries:         3,
		retryDelay:         time.Second * 5,
		lastCheckpointTime: time.Now(),
	}, nil
}

func (m *CassandraMigration) Close() error {
	if m.source != nil {
		m.source.Close()
	}
	if m.dest != nil {
		m.dest.Close()
	}
	return nil
}

func (m *CassandraMigration) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(m.config.Source.Tables))

	// 创建工作池
	workers := m.config.Migration.Workers
	if workers <= 0 {
		workers = 4 // 默认值
	}
	semaphore := make(chan struct{}, workers)
	logrus.Infof(i18n.Tr("使用 %d 个工作线程进行并发迁移", "Using %d worker threads for concurrent migration"), workers)

	// 输出全局配置信息
	logrus.Infof(i18n.Tr("开始执行数据查询，批次大小: %d, 全局限速: %d 行/秒",
		"Starting data query, batch size: %d, global rate limit: %d rows/sec"),
		m.config.Migration.BatchSize, m.config.Migration.RateLimit)

	// 并发迁移表
	for _, table := range m.config.Source.Tables {
		wg.Add(1)
		go func(table config.TableMapping) {
			defer wg.Done()

			// 获取信号量
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			// 初始化表的统计信息
			stats := migration.NewMigrationStats(0, m.config.Migration.ProgressInterval, table.Name)
			m.statsMutex.Lock()
			m.statsMap[table.Name] = stats
			m.statsMutex.Unlock()

			stats.StartReporting(time.Duration(m.config.Migration.ProgressInterval) * time.Second)
			defer func() {
				m.statsMutex.Lock()
				delete(m.statsMap, table.Name)
				m.statsMutex.Unlock()
				stats.StopReporting()
			}()

			if err := m.migrateTable(ctx, table); err != nil {
				errChan <- fmt.Errorf(i18n.Tr("迁移表 %s 失败: %v", "Failed to migrate table %s: %v"), table.Name, err)
			}
		}(table)
	}

	wg.Wait()
	close(errChan)

	// 收集错误
	var errors []string
	for err := range errChan {
		errors = append(errors, err.Error())
	}

	if len(errors) > 0 {
		return fmt.Errorf(i18n.Tr("迁移过程中发生错误:\n%s", "Errors occurred during migration:\n%s"), strings.Join(errors, "\n"))
	}

	return nil
}

func (m *CassandraMigration) migrateTable(ctx context.Context, table config.TableMapping) error {
	// 首先检查断点
	checkpoint, err := m.loadCheckpoint(table.Name)
	if err != nil {
		logrus.Infof(i18n.Tr("加载断点信息失败: %v, 将从头开始迁移", "Failed to load checkpoint: %v, will start migration from beginning"), err)
	} else if checkpoint != nil && checkpoint.Complete {
		logrus.Infof(i18n.Tr("表 %s 已完成迁移，跳过", "Table %s migration completed, skipping"), table.Name)
		return nil
	}

	// 首先检查表是否存在
	keyspace := m.config.Source.Keyspace
	tableExistsQuery := fmt.Sprintf(`
		SELECT table_name 
		FROM system_schema.tables 
		WHERE keyspace_name = '%s' 
		AND table_name = '%s'`,
		keyspace, table.Name)

	iter := m.source.Query(tableExistsQuery).Iter()
	var tableName string
	exists := iter.Scan(&tableName)
	iter.Close()

	if !exists {
		return fmt.Errorf(i18n.Tr("表 %s 不存在", "Table %s does not exist"), table.Name)
	}

	targetName := table.Name
	if table.TargetName != "" {
		targetName = table.TargetName
	}

	// 先迁移自定义类型和函数
	if err := m.migrateDependencies(); err != nil {
		return fmt.Errorf(i18n.Tr("迁移依赖项失败: %v", "Failed to migrate dependencies: %v"), err)
	}

	// 获取表结构
	schema, err := m.getTableSchema(table.Name)
	if err != nil {
		return fmt.Errorf(i18n.Tr("获取表结构失败: %v", "Failed to get table schema: %v"), err)
	}

	// 修改表名
	if targetName != table.Name {
		schema = strings.Replace(schema, table.Name, targetName, 1)
	}

	// 创建目标表
	if err := m.dest.Query(schema).Exec(); err != nil {
		return fmt.Errorf(i18n.Tr("创建目标表失败: %v", "Failed to create target table: %v"), err)
	}

	return m.copyData(ctx, table.Name, targetName, checkpoint)
}

func (m *CassandraMigration) migrateDependencies() error {
	// 首先检查表是否使用了自定义类型和函数
	usedTypes := make(map[string]bool)
	usedFunctions := make(map[string]bool)

	// 获取表的列类型信息
	columnsQuery := fmt.Sprintf(`
		SELECT table_name, column_name, type
		FROM system_schema.columns
		WHERE keyspace_name = '%s'`,
		m.config.Source.Keyspace)

	colIter := m.source.Query(columnsQuery).Iter()
	var tableName, columnName, columnType string

	for colIter.Scan(&tableName, &columnName, &columnType) {
		// 如果列类型是自定义类型（不是基本类型）
		if !isBasicType(columnType) {
			typeName := strings.TrimSuffix(strings.TrimPrefix(columnType, "frozen<"), ">")
			usedTypes[typeName] = true
		}
	}

	if err := colIter.Close(); err != nil {
		return fmt.Errorf(i18n.Tr("获取列信息失败: %v", "Failed to get column information: %v"), err)
	}

	// 获取表的索引和触发器，检查是否使用了自定义函数
	indexQuery := fmt.Sprintf(`
		SELECT options 
		FROM system_schema.indexes 
		WHERE keyspace_name = '%s'`,
		m.config.Source.Keyspace)

	indexIter := m.source.Query(indexQuery).Iter()
	var options map[string]string

	for indexIter.Scan(&options) {
		if expr, ok := options["expression"]; ok {
			extractFunctionNames(expr, usedFunctions)
		}
	}

	if err := indexIter.Close(); err != nil {
		logrus.Warnf(i18n.Tr("获取索引信息失败: %v", "Failed to get index information: %v"), err)
	}

	// 获取自定义类型
	typeQuery := fmt.Sprintf(`
		SELECT type_name, field_names, field_types 
		FROM system_schema.types 
		WHERE keyspace_name = '%s'`,
		m.config.Source.Keyspace)

	typeIter := m.source.Query(typeQuery).Iter()
	var typeName string
	var fieldNames, fieldTypes []string

	// 使用 map 来跟踪已创建的类型
	createdTypes := make(map[string]bool)

	for typeIter.Scan(&typeName, &fieldNames, &fieldTypes) {
		// 只创建被使用的类型
		if !usedTypes[typeName] {
			logrus.Debugf(i18n.Tr("跳过未使用的类型: %s", "Skipping unused type: %s"), typeName)
			continue
		}

		if createdTypes[typeName] {
			continue
		}

		createType := fmt.Sprintf("CREATE TYPE IF NOT EXISTS %s.%s (%s)",
			m.config.Destination.Keyspace,
			typeName,
			buildTypeFields(fieldNames, fieldTypes))

		if err := m.dest.Query(createType).Exec(); err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				return fmt.Errorf(i18n.Tr("创建类型 %s 失败: %v", "Failed to create type %s: %v"), typeName, err)
			}
		} else {
			logrus.Infof(i18n.Tr("创建自定义类型: %s", "Created custom type: %s"), typeName)
		}
		createdTypes[typeName] = true
	}

	if err := typeIter.Close(); err != nil {
		return fmt.Errorf(i18n.Tr("获取类型信息失败: %v", "Failed to get type information: %v"), err)
	}

	// 获取自定义函数
	funcQuery := fmt.Sprintf(`
		SELECT function_name, argument_types, return_type, language, body, called_on_null_input
		FROM system_schema.functions 
		WHERE keyspace_name = '%s'`,
		m.config.Source.Keyspace)

	funcIter := m.source.Query(funcQuery).Iter()
	var funcName, language, body string
	var argTypes, returnType []string
	var calledOnNull bool

	// 使用 map 来跟踪已创建的函数
	createdFuncs := make(map[string]bool)

	for funcIter.Scan(&funcName, &argTypes, &returnType, &language, &body, &calledOnNull) {
		// 只创建被使用的函数
		if !usedFunctions[funcName] {
			logrus.Debugf(i18n.Tr("跳过未使用的函数: %s", "Skipping unused function: %s"), funcName)
			continue
		}

		if createdFuncs[funcName] {
			continue
		}

		nullInput := "RETURNS NULL ON NULL INPUT"
		if !calledOnNull {
			nullInput = "CALLED ON NULL INPUT"
		}

		createFunc := fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s.%s (%s)
			RETURNS %s
			LANGUAGE %s
			%s
			AS $$%s$$`,
			m.config.Destination.Keyspace,
			funcName,
			strings.Join(argTypes, ", "),
			returnType[0],
			language,
			nullInput,
			body)

		if err := m.dest.Query(createFunc).Exec(); err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				return fmt.Errorf(i18n.Tr("创建函数 %s 失败: %v", "Failed to create function %s: %v"), funcName, err)
			}
		} else {
			logrus.Infof(i18n.Tr("创建自定义函数: %s", "Created custom function: %s"), funcName)
		}
		createdFuncs[funcName] = true
	}

	if err := funcIter.Close(); err != nil {
		return fmt.Errorf(i18n.Tr("获取函数信息失败: %v", "Failed to get function information: %v"), err)
	}

	return nil
}

func buildTypeFields(names, types []string) string {
	fields := make([]string, len(names))
	for i := range names {
		fields[i] = fmt.Sprintf("%s %s", names[i], types[i])
	}
	return strings.Join(fields, ", ")
}

func (m *CassandraMigration) copyData(ctx context.Context, sourceName, targetName string, checkpoint *migration.Checkpoint) error {
	// 在开始处检查完成标记
	if checkpoint != nil && checkpoint.Complete {
		logrus.Infof(i18n.Tr("表 %s 已完成迁移，跳过执行", "Table %s migration completed, skipping"), sourceName)
		return nil
	}

	// 首先获取表的列信息
	keyspace := m.config.Source.Keyspace
	columnsQuery := fmt.Sprintf(`
		SELECT column_name, type 
		FROM system_schema.columns 
		WHERE keyspace_name = '%s' 
		AND table_name = '%s'`, keyspace, sourceName)

	logrus.Infof(i18n.Tr("执行列信息查询: %s", "Executing column information query: %s"), columnsQuery)

	iter := m.source.Query(columnsQuery).Iter()
	var colName, colType string
	columns := make([]gocql.ColumnInfo, 0)
	columnTypes := make(map[string]string)

	for iter.Scan(&colName, &colType) {
		logrus.Debugf(i18n.Tr("发现列: %s (类型: %s)", "Found column: %s (type: %s)"), colName, colType)
		columns = append(columns, gocql.ColumnInfo{
			Name: colName,
		})
		columnTypes[colName] = colType
	}

	if err := iter.Close(); err != nil {
		return fmt.Errorf(i18n.Tr("获取列信息失败: %v", "Failed to get column information: %v"), err)
	}

	if len(columns) == 0 {
		return fmt.Errorf(i18n.Tr("表 %s.%s 中没有找到任何列", "No columns found in table %s.%s"), keyspace, sourceName)
	}

	logrus.Infof(i18n.Tr("成功获取到 %d 个列", "Successfully got %d columns"), len(columns))

	// 获取所有列信息
	schemaQuery := fmt.Sprintf(`
		SELECT column_name, kind, position
		FROM system_schema.columns
		WHERE keyspace_name = '%s'
		AND table_name = '%s'`,
		keyspace, sourceName)

	pkMap := make(map[string]bool)
	partitionKeys := make(map[string]bool)

	// 获取所有列信息
	schemaIter := m.source.Query(schemaQuery).Iter()
	var kind string
	var position int

	for schemaIter.Scan(&colName, &kind, &position) {
		switch kind {
		case "partition_key":
			pkMap[colName] = true
			partitionKeys[colName] = true
			logrus.Debugf(i18n.Tr("分区键: %s", "Partition key: %s"), colName)
		case "clustering":
			pkMap[colName] = true
			logrus.Debugf(i18n.Tr("聚集键: %s", "Clustering key: %s"), colName)
		}
	}

	if err := schemaIter.Close(); err != nil {
		return fmt.Errorf(i18n.Tr("获取主键信息失败: %v", "Failed to get primary key information: %v"), err)
	}

	// 如果没有找到任何主键，返回错误
	if len(pkMap) == 0 {
		return fmt.Errorf(i18n.Tr("表 %s 没有主键", "Table %s has no primary key"), sourceName)
	}

	// 构建查询，包括列转换
	queryParts := make([]string, 0, len(columns))
	for _, col := range columns {
		colType := columnTypes[col.Name]
		isCollection := strings.Contains(colType, "list<") ||
			strings.Contains(colType, "map<") ||
			strings.Contains(colType, "set<")

		// 检查是否有列转换
		transformed := false
		// 从源表配置中获取列转换信息
		var tableConfig config.TableMapping
		for _, t := range m.config.Source.Tables {
			if t.Name == sourceName {
				tableConfig = t
				break
			}
		}

		for _, transform := range tableConfig.ColumnTransformations {
			if transform.SourceColumn == col.Name {
				// 使用转换表达式，但保持原始列名
				if pkMap[col.Name] || isCollection {
					// 主键列和集合类型不需要 TTL
					queryParts = append(queryParts, fmt.Sprintf("%s AS %s",
						transform.Expression, col.Name))
				} else {
					// 非主键且非集合类型的列添加 TTL
					queryParts = append(queryParts, fmt.Sprintf("%s AS %s, TTL(%s)",
						transform.Expression, col.Name, col.Name))
				}
				transformed = true
				break
			}
		}

		if !transformed {
			if pkMap[col.Name] || isCollection {
				// 主键列和集合类型不需要 TTL
				queryParts = append(queryParts, col.Name)
			} else {
				// 非主键且非集合类型的列添加 TTL
				queryParts = append(queryParts, fmt.Sprintf("%s, TTL(%s)", col.Name, col.Name))
			}
		}
	}

	// 构建完整查询
	query := fmt.Sprintf("SELECT %s FROM %s.%s",
		strings.Join(queryParts, ", "),
		m.config.Source.Keyspace,
		sourceName)

	logrus.Infof(i18n.Tr("完整查询语句: %s", "Full query: %s"), query)

	// 只处理断点续传的位置
	if checkpoint != nil && len(checkpoint.LastKey) > 0 && !checkpoint.Complete {
		whereClause := m.buildWhereClause(columnTypes, sourceName)
		if whereClause != "" {
			query = fmt.Sprintf("%s WHERE %s", query, whereClause)
			logrus.Infof(i18n.Tr("从断点继续: %s", "Continuing from checkpoint: %s"), whereClause)
		}
	}

	// 为所有列创建值存储（包括TTL列）
	values := make([]interface{}, len(columns))             // 只存储列值
	ttlValues := make([]interface{}, len(columns))          // 存储TTL值
	valuePointers := make([]interface{}, 0, len(columns)*2) // 存储所有指针

	// 根据列类型创建正确的接收器
	for i, col := range columns {
		colType := columnTypes[col.Name]
		switch {
		case strings.Contains(colType, "list<frozen<"):
			values[i] = &[]map[string]interface{}{}
		case strings.Contains(colType, "frozen<"):
			values[i] = &map[string]interface{}{}
		case strings.Contains(colType, "list<"):
			values[i] = &[]interface{}{}
		case strings.Contains(colType, "map<"):
			values[i] = &map[string]interface{}{}
		case strings.Contains(colType, "set<"):
			values[i] = &[]interface{}{}
		case colType == "blob":
			values[i] = &[]byte{}
		case strings.Contains(colType, "bigint"):
			values[i] = new(int64)
		case strings.Contains(colType, "int"):
			values[i] = new(int)
		case strings.Contains(colType, "boolean"):
			values[i] = new(bool)
		default:
			values[i] = new(string)
		}

		// 添加列值指针
		valuePointers = append(valuePointers, values[i])

		// 只为非主键且非集合类型的列添加TTL值指针
		isCollection := strings.Contains(colType, "list<") ||
			strings.Contains(colType, "map<") ||
			strings.Contains(colType, "set<")

		if !pkMap[col.Name] && !isCollection {
			ttlValues[i] = new(int)
			valuePointers = append(valuePointers, ttlValues[i])
		}

		logrus.Debugf(i18n.Tr("列 %s (类型: %s) 使用接收器类型: %T", "Column %s (type: %s) uses receiver type: %T"), col.Name, colType, values[i])
	}

	logrus.Infof(i18n.Tr("总列数: %d (数据列: %d, TTL列: %d)", "Total columns: %d (data columns: %d, TTL columns: %d)"),
		len(values), len(queryParts), len(queryParts))

	// 执行数据查询
	dataIter := m.source.Query(query).PageSize(m.config.Migration.BatchSize).Iter()
	defer dataIter.Close()

	// 构建插入语句，支持每列的 TTL
	insertParts := make([]string, len(columns))
	placeholders := make([]string, len(columns))
	ttlCount := 0
	for i, col := range columns {
		colType := columnTypes[col.Name]
		isCollection := strings.Contains(colType, "list<") ||
			strings.Contains(colType, "map<") ||
			strings.Contains(colType, "set<")

		insertParts[i] = col.Name
		placeholders[i] = "?"

		// 只计算非主键且非集合类型的列
		if !pkMap[col.Name] && !isCollection {
			ttlCount++
		}
	}

	// 构建基本的插入语句
	insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		targetName,
		strings.Join(insertParts, ", "),
		strings.Join(placeholders, ", "))

	// 创建数据值数组
	var dataValues []interface{}

	// 只有在有非主键列时才添加 TTL 子句
	if ttlCount > 0 {
		insertQuery += " USING TTL ?"
		// 提取数据值时也要相应修改
		dataValues = make([]interface{}, len(columns)+1) // +1 是为了TTL值
		for i := range columns {
			colType := columnTypes[columns[i].Name]
			isCollection := strings.Contains(colType, "list<") ||
				strings.Contains(colType, "map<") ||
				strings.Contains(colType, "set<")

			if !pkMap[columns[i].Name] && !isCollection {
				dataValues[i] = *(ttlValues[i].(*int))
			}
		}
		dataValues[len(dataValues)-1] = dataValues[0] // 将TTL值设置为第一个非集合且非主键列的TTL
	} else {
		dataValues = make([]interface{}, len(columns)) // 没有TTL值
	}

	logrus.Debugf(i18n.Tr("插入语句: %s", "Insert query: %s"), insertQuery)
	logrus.Debugf(i18n.Tr("列数: %d, 主键列数: %d, TTL列数: %d", "Column count: %d, primary key column count: %d, TTL column count: %d"), len(columns), len(columns)-ttlCount, ttlCount)

	// 处理数据
	batch := m.dest.NewBatch(gocql.UnloggedBatch)
	count := 0
	rowCount := 0

	// 创建断点保存计时器
	checkpointTicker := time.NewTicker(time.Second) // 每秒保存一次断点
	defer checkpointTicker.Stop()

	// 创建一个变量来存储最后处理的位置
	var lastProcessedKey map[string]string

	go func() {
		for range checkpointTicker.C {
			if stats, ok := m.statsMap[sourceName]; ok {
				stats.Lock()
				if lastProcessedKey != nil {
					if err := m.saveCheckpoint(sourceName, lastProcessedKey, false); err != nil {
						logrus.Warnf(i18n.Tr("保存断点失败: %v", "Failed to save checkpoint: %v"), err)
					}
				}
				stats.Unlock()
			}
		}
	}()

	// 处理数据
	for dataIter.Scan(valuePointers...) {
		rowCount++
		if err := m.limiter.Wait(ctx); err != nil {
			return err
		}

		// 提取数据值
		dataValues := make([]interface{}, len(columns))
		valueIndex := 0

		// 复制列值
		for i := range columns {
			switch v := values[i].(type) {
			case *[]map[string]interface{}:
				// 对于 list<frozen<...>> 类型
				if v != nil {
					dataValues[valueIndex] = *v // 直接使用解引用的值
				}
			case *map[string]interface{}:
				// 对于 frozen<...> 类型
				if v != nil {
					dataValues[valueIndex] = *v // 直接使用解引用的值
				}
			case *[]interface{}:
				// 对于 list<...> 类型
				if v != nil {
					dataValues[valueIndex] = *v // 直接使用解引用的值
				}
			case *[]byte:
				if v != nil {
					dataValues[valueIndex] = *v // 对于blob类型，直接使用解引用的值
				}
			case *int64:
				if v != nil {
					dataValues[valueIndex] = *v
				}
			case *int:
				if v != nil {
					dataValues[valueIndex] = *v
				}
			case *bool:
				if v != nil {
					dataValues[valueIndex] = *v
				}
			case *string:
				if v != nil {
					dataValues[valueIndex] = *v
				}
			default:
				if v != nil {
					// 对于其他类型，使用反射获取实际值
					dataValues[valueIndex] = reflect.ValueOf(v).Elem().Interface()
				}
			}
			valueIndex++
		}

		// 如果需要TTL，添加TTL值
		if ttlCount > 0 {
			// 找到第一个非集合且非主键列的TTL值
			for i := range columns {
				colType := columnTypes[columns[i].Name]
				isCollection := strings.Contains(colType, "list<") ||
					strings.Contains(colType, "map<") ||
					strings.Contains(colType, "set<")

				if !pkMap[columns[i].Name] && !isCollection && ttlValues[i] != nil {
					ttlVal := *(ttlValues[i].(*int))
					dataValues = append(dataValues, ttlVal)
					break
				}
			}
		}

		// 更新最后处理的位置
		currentKey := make(map[string]string)
		for i, col := range columns {
			if partitionKeys[col.Name] {
				switch v := values[i].(type) {
				case *string:
					if v != nil {
						currentKey[col.Name] = *v
					}
				case *[]byte:
					if v != nil {
						currentKey[col.Name] = fmt.Sprintf("%x", *v)
					}
				case *int:
					if v != nil {
						currentKey[col.Name] = fmt.Sprintf("%d", *v)
					}
				case *int64:
					if v != nil {
						currentKey[col.Name] = fmt.Sprintf("%d", *v)
					}
				default:
					if v != nil {
						currentKey[col.Name] = fmt.Sprintf("%v", reflect.ValueOf(v).Elem().Interface())
					}
				}
			}
		}
		m.statsMap[sourceName].Lock()
		lastProcessedKey = currentKey
		m.statsMap[sourceName].Unlock()

		// 添加到批处理
		batch.Query(insertQuery, dataValues...)
		count++

		if count >= m.config.Migration.BatchSize {
			if err := m.executeBatchWithRetry(batch); err != nil {
				return err
			}
			batch = m.dest.NewBatch(gocql.UnloggedBatch)
			m.statsMap[sourceName].Increment(count)
			count = 0
		}
	}

	// 处理最后一批数据
	if count > 0 {
		if err := m.executeBatchWithRetry(batch); err != nil {
			return err
		}
		m.statsMap[sourceName].Increment(count)
	}

	// 如果迭代器没有错误，说明已经处理完所有数据，标记为完成
	if err := dataIter.Close(); err != nil {
		return fmt.Errorf(i18n.Tr("查询执行失败: %v", "Query execution failed: %v"), err)
	}

	// 更新最终进度
	if stats, ok := m.statsMap[sourceName]; ok {
		stats.Lock()
		stats.ProcessedRows += int64(count)
		stats.Unlock()
		// 停止进度报告
		stats.StopReporting()
		// 最终报告
		stats.Report()
	}

	// 标记为完成
	logrus.Infof(i18n.Tr("表 %s 迁移完成，标记为完成状态", "Table %s migration completed, marking as completed state"), sourceName)
	completionKey := make(map[string]string)
	for k, v := range lastProcessedKey {
		completionKey[k] = v
	}

	if err := m.saveCheckpoint(sourceName, completionKey, true); err != nil {
		logrus.Errorf(i18n.Tr("保存完成标记失败: %v", "Failed to save completion marker: %v"), err)
		return fmt.Errorf(i18n.Tr("保存完成标记失败: %v", "Failed to save completion marker: %v"), err)
	}

	logrus.Infof(i18n.Tr("表 %s 迁移完成并已保存完成标记", "Table %s migration completed and completion marker saved"), sourceName)

	return nil
}

func (m *CassandraMigration) executeBatchWithRetry(batch *gocql.Batch) error {
	var lastErr error
	for i := 0; i < m.maxRetries; i++ {
		if err := m.dest.ExecuteBatch(batch); err != nil {
			lastErr = err
			time.Sleep(m.retryDelay)
			continue
		}
		return nil
	}
	return fmt.Errorf(i18n.Tr("执行批量写入失败，重试%d次后仍然失败: %v", "Execution of batch write failed, retrying %d times but still failed: %v"), m.maxRetries, lastErr)
}

func (m *CassandraMigration) getTableSchema(tableName string) (string, error) {
	keyspace := m.config.Source.Keyspace

	// 获取表的列信息
	query := fmt.Sprintf(`
		SELECT column_name, type, kind, position
		FROM system_schema.columns
		WHERE keyspace_name = '%s' AND table_name = '%s'`,
		keyspace, tableName)

	iter := m.source.Query(query).Iter()
	var columnName, columnType, kind string
	var position int
	columns := make([]string, 0)
	pkColumns := make([]string, 0)
	clusteringColumns := make([]string, 0)

	// 一次性获取所有列信息，包括主键信息
	for iter.Scan(&columnName, &columnType, &kind, &position) {
		columns = append(columns, fmt.Sprintf("%s %s", columnName, columnType))

		switch kind {
		case "partition_key":
			pkColumns = append(pkColumns, columnName)
		case "clustering":
			clusteringColumns = append(clusteringColumns, columnName)
		}
	}

	if err := iter.Close(); err != nil {
		return "", fmt.Errorf(i18n.Tr("获取表结构失败: %v", "Failed to get table schema: %v"), err)
	}

	if len(columns) == 0 {
		return "", fmt.Errorf(i18n.Tr("未找到表 %s 的结构信息", "No table schema found for table %s"), tableName)
	}

	// 获取表的属性，包括默认 TTL
	tableQuery := fmt.Sprintf(`
		SELECT default_time_to_live
		FROM system_schema.tables
		WHERE keyspace_name = '%s' AND table_name = '%s'`,
		keyspace, tableName)

	var defaultTTL int
	if err := m.source.Query(tableQuery).Scan(&defaultTTL); err != nil {
		logrus.Warnf(i18n.Tr("获取表默认TTL失败: %v", "Failed to get table default TTL: %v"), err)
	}

	// 构建主键约束
	var constraints []string
	if len(pkColumns) > 0 {
		constraints = append(constraints, fmt.Sprintf("PRIMARY KEY ((%s)%s)",
			strings.Join(pkColumns, ", "),
			func() string {
				if len(clusteringColumns) > 0 {
					return ", " + strings.Join(clusteringColumns, ", ")
				}
				return ""
			}()))
	}

	// 构建完整的CREATE TABLE语句
	createTable := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n    %s%s\n)%s",
		tableName,
		strings.Join(columns, ",\n    "),
		func() string {
			if len(constraints) > 0 {
				return ",\n    " + strings.Join(constraints, ",\n    ")
			}
			return ""
		}(),
		func() string {
			if defaultTTL > 0 {
				return fmt.Sprintf(" WITH default_time_to_live = %d", defaultTTL)
			}
			return ""
		}())

	return createTable, nil
}

func (m *CassandraMigration) buildInsertQuery(tableName string, columnNames []string) string {
	placeholders := make([]string, len(columnNames))
	for i := range columnNames {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columnNames, ", "),
		strings.Join(placeholders, ", "))

	return query
}

func (m *CassandraMigration) loadCheckpoint(tableName string) (*migration.Checkpoint, error) {
	checkpoint, err := m.loadCheckpointFile(tableName)
	if err != nil {
		return nil, fmt.Errorf(i18n.Tr("加载断点文件失败: %v", "Failed to load checkpoint file: %v"), err)
	}

	// 如果发现完成标记，返回带完成标记的 checkpoint
	if checkpoint != nil && checkpoint.Complete {
		logrus.Infof(i18n.Tr("表 %s 已完成迁移，跳过", "Table %s migration completed, skipping"), tableName)
		return checkpoint, nil
	}

	return checkpoint, nil
}

func (m *CassandraMigration) loadCheckpointFile(tableName string) (*migration.Checkpoint, error) {
	if m.config.Migration.CheckpointDir == "" {
		return nil, nil
	}

	filename := filepath.Join(m.config.Migration.CheckpointDir,
		fmt.Sprintf("cassandra_%s.checkpoint", tableName))

	data, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf(i18n.Tr("读取断点文件失败: %v", "Failed to read checkpoint file: %v"), err)
	}

	var checkpoint migration.Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, fmt.Errorf(i18n.Tr("解析断点文件失败: %v", "Failed to parse checkpoint file: %v"), err)
	}

	return &checkpoint, nil
}

func (m *CassandraMigration) saveCheckpoint(tableName string, lastKey map[string]string, completed bool) error {
	if m.config.Migration.CheckpointDir == "" {
		return nil
	}

	// 确保 checkpoint 目录存在
	if err := migration.EnsureDir(m.config.Migration.CheckpointDir); err != nil {
		return fmt.Errorf(i18n.Tr("创建断点目录失败: %v", "Failed to create checkpoint directory: %v"), err)
	}

	checkpoint := &migration.Checkpoint{
		LastKey:     lastKey,
		LastUpdated: time.Now(),
		Complete:    completed,
	}

	data, err := json.Marshal(checkpoint)
	if err != nil {
		return fmt.Errorf(i18n.Tr("序列化断点数据失败: %v", "Failed to serialize checkpoint data: %v"), err)
	}

	filename := filepath.Join(m.config.Migration.CheckpointDir,
		fmt.Sprintf("cassandra_%s.checkpoint", tableName))

	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf(i18n.Tr("写入断点文件失败: %v", "Failed to write checkpoint file: %v"), err)
	}

	return nil
}

func (m *CassandraMigration) buildWhereClause(columnTypes map[string]string, tableName string) string {
	// 加载断点信息
	checkpoint, err := m.loadCheckpoint(tableName)
	if err != nil || checkpoint == nil || len(checkpoint.LastKey) == 0 {
		return ""
	}

	// 获取所有分区键
	pkQuery := fmt.Sprintf(`
		SELECT column_name, kind, position
		FROM system_schema.columns 
		WHERE keyspace_name = '%s' 
		AND table_name = '%s'`,
		m.config.Source.Keyspace, tableName)

	iter := m.source.Query(pkQuery).Iter()
	var columnName, kind string
	var position int
	partitionKeys := make([]string, 0)

	// 收集所有分区键，并按position排序
	type pkInfo struct {
		name     string
		position int
	}
	var pks []pkInfo

	for iter.Scan(&columnName, &kind, &position) {
		if kind == "partition_key" {
			pks = append(pks, pkInfo{columnName, position})
		}
	}
	if err := iter.Close(); err != nil {
		logrus.Warnf(i18n.Tr("获取分区键信息失败: %v", "Failed to get partition key information: %v"), err)
		return ""
	}

	// 按position排序
	sort.Slice(pks, func(i, j int) bool {
		return pks[i].position < pks[j].position
	})

	// 提取排序后的分区键名称
	for _, pk := range pks {
		partitionKeys = append(partitionKeys, pk.name)
	}

	// 构建 token 函数的参数
	tokenArgs := make([]string, len(partitionKeys))
	lastTokenArgs := make([]string, len(partitionKeys))

	for i, key := range partitionKeys {
		tokenArgs[i] = key
		if lastVal, ok := checkpoint.LastKey[key]; ok {
			switch columnTypes[key] {
			case "text", "varchar", "ascii":
				lastTokenArgs[i] = fmt.Sprintf("'%s'", lastVal)
			case "blob":
				lastTokenArgs[i] = fmt.Sprintf("0x%s", lastVal)
			default:
				lastTokenArgs[i] = lastVal
			}
		} else {
			// 如果缺少任何分区键的值，返回空字符串
			return ""
		}
	}

	whereClause := fmt.Sprintf("token(%s) >= token(%s)",
		strings.Join(tokenArgs, ", "),
		strings.Join(lastTokenArgs, ", "))

	logrus.Infof(i18n.Tr("断点续传条件:\n  分区键: %v\n  完整条件: %s", "Checkpoint condition:\n   Partition keys: %v\n   Full condition: %s"),
		partitionKeys, whereClause)

	return whereClause
}

func loadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf(i18n.Tr("读取配置文件失败: %v", "Failed to read config file: %v"), err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf(i18n.Tr("解析配置文件失败: %v", "Failed to parse config file: %v"), err)
	}

	return &config, nil
}

func createClusterConfig(dbConfig config.DBConfig) *gocql.ClusterConfig {
	if len(dbConfig.Hosts) == 0 {
		logrus.Fatal(i18n.Tr("未配置 Cassandra 主机地址", "Cassandra host address not configured"))
	}

	logrus.Infof(i18n.Tr("正在连接 Cassandra 集群: %v, keyspace: %s", "Connecting to Cassandra cluster: %v, keyspace: %s"), dbConfig.Hosts, dbConfig.Keyspace)
	cluster := gocql.NewCluster(dbConfig.Hosts...)
	cluster.Keyspace = dbConfig.Keyspace
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: dbConfig.Username,
		Password: dbConfig.Password,
	}
	cluster.Consistency = gocql.Quorum
	cluster.ConnectTimeout = time.Second * 10
	cluster.Timeout = time.Second * 30
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 3}
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	return cluster
}

// 添加配置验证函数
func validateConfig(config *config.Config) error {
	if len(config.Source.Hosts) == 0 {
		return fmt.Errorf(i18n.Tr("未配置源 Cassandra 主机地址", "Source Cassandra host address not configured"))
	}
	if len(config.Destination.Hosts) == 0 {
		return fmt.Errorf(i18n.Tr("未配置目标 Cassandra 主机地址", "Destination Cassandra host address not configured"))
	}
	if config.Source.Username == "" || config.Source.Password == "" {
		return fmt.Errorf(i18n.Tr("未配置源 Cassandra 认证信息", "Source Cassandra authentication information not configured"))
	}
	if config.Destination.Username == "" || config.Destination.Password == "" {
		return fmt.Errorf(i18n.Tr("未配置目标 Cassandra 认证信息", "Destination Cassandra authentication information not configured"))
	}
	return nil
}

// 添加连接验证函数
func validateConnection(session *gocql.Session, keyspace string) error {
	// 尝试执行一个简单的查询来验证连接
	if err := session.Query("SELECT release_version FROM system.local").Exec(); err != nil {
		return fmt.Errorf(i18n.Tr("验证连接失败: %v", "Failed to validate connection: %v"), err)
	}
	return nil
}

// 从表达式中提取函数名
func extractFunctionNames(expr string, functions map[string]bool) {
	// 使用正则表达式匹配函数调用
	funcRegex := regexp.MustCompile(`(\w+)\s*\(`)
	matches := funcRegex.FindAllStringSubmatch(expr, -1)

	for _, match := range matches {
		if len(match) > 1 {
			funcName := strings.ToLower(match[1])
			// 排除一些常见的内置函数
			if !isBuiltinFunction(funcName) {
				functions[funcName] = true
			}
		}
	}
}

// 判断是否为内置函数
func isBuiltinFunction(name string) bool {
	builtins := map[string]bool{
		"token": true,
		"now":   true,
		"uuid":  true,
		"ttl":   true,
		// 添加其他内置函数...
	}
	return builtins[name]
}

// 添加 isBasicType 函数
func isBasicType(typeName string) bool {
	basicTypes := map[string]bool{
		"ascii":     true,
		"bigint":    true,
		"blob":      true,
		"boolean":   true,
		"counter":   true,
		"date":      true,
		"decimal":   true,
		"double":    true,
		"float":     true,
		"inet":      true,
		"int":       true,
		"smallint":  true,
		"text":      true,
		"time":      true,
		"timestamp": true,
		"timeuuid":  true,
		"tinyint":   true,
		"uuid":      true,
		"varchar":   true,
		"varint":    true,
	}

	// 处理集合类型
	if strings.HasPrefix(typeName, "list<") ||
		strings.HasPrefix(typeName, "set<") ||
		strings.HasPrefix(typeName, "map<") {
		return true
	}

	return basicTypes[typeName]
}
