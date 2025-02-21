package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
	"gopkg.in/yaml.v2"
)

type TableMapping struct {
	Name       string `yaml:"name"`
	TargetName string `yaml:"target_name,omitempty"`
}

type DBConfig struct {
	Hosts    []string       `yaml:"hosts"`
	Keyspace string         `yaml:"keyspace"`
	Username string         `yaml:"username"`
	Password string         `yaml:"password"`
	Tables   []TableMapping `yaml:"tables,omitempty"`
}

type MigrationConfig struct {
	BatchSize       int    `yaml:"batch_size"`
	Workers         int    `yaml:"workers"`
	RateLimit       int    `yaml:"rate_limit"`
	Timeout         int    `yaml:"timeout"`
	CheckpointDir   string `yaml:"checkpoint_dir"`
	LogFile         string `yaml:"log_file"`
	LogLevel        string `yaml:"log_level"`
	CheckpointDelay int    `yaml:"checkpoint_delay"` // 单位：秒
}

type Config struct {
	Source      DBConfig        `yaml:"source"`
	Destination DBConfig        `yaml:"destination"`
	Migration   MigrationConfig `yaml:"migration"`
}

type MigrationStats struct {
	TotalRows     int64
	ProcessedRows int64
	StartTime     time.Time
	mu            sync.Mutex
}

func (s *MigrationStats) increment(count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ProcessedRows += int64(count)
}

func (s *MigrationStats) report() {
	duration := time.Since(s.StartTime)
	rate := float64(s.ProcessedRows) / duration.Seconds()
	logrus.Infof("进度: %d/%d 行 (%.2f%%), 速率: %.2f 行/秒",
		s.ProcessedRows, s.TotalRows,
		float64(s.ProcessedRows)/float64(s.TotalRows)*100, rate)
}

type Migration struct {
	source     *gocql.Session
	dest       *gocql.Session
	config     *Config
	limiter    *rate.Limiter
	stats      *MigrationStats
	maxRetries int
	retryDelay time.Duration
}

type TokenRange struct {
	Start    int64             `json:"start"`
	End      int64             `json:"end"`
	LastKey  map[string]string `json:"last_key,omitempty"`
	Complete bool              `json:"complete"`
}

type Checkpoint struct {
	LastKey     map[string]string `json:"last_key"`
	LastUpdated time.Time         `json:"last_updated"`
	AllComplete bool              `json:"all_complete"`
}

func NewMigration(config *Config) (*Migration, error) {
	sourceCluster := createClusterConfig(config.Source)
	destCluster := createClusterConfig(config.Destination)

	sourceCluster.Timeout = time.Duration(config.Migration.Timeout) * time.Second
	destCluster.Timeout = time.Duration(config.Migration.Timeout) * time.Second

	sourceSession, err := sourceCluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("连接源数据库失败: %v", err)
	}

	destSession, err := destCluster.CreateSession()
	if err != nil {
		sourceSession.Close()
		return nil, fmt.Errorf("连接目标数据库失败: %v", err)
	}

	return &Migration{
		source:     sourceSession,
		dest:       destSession,
		config:     config,
		limiter:    rate.NewLimiter(rate.Limit(config.Migration.RateLimit), config.Migration.RateLimit),
		stats:      &MigrationStats{StartTime: time.Now()},
		maxRetries: 3,
		retryDelay: time.Second * 5,
	}, nil
}

func (m *Migration) Close() {
	if m.source != nil {
		m.source.Close()
	}
	if m.dest != nil {
		m.dest.Close()
	}
}

func (m *Migration) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(m.config.Source.Tables))

	// 启动进度报告
	ticker := time.NewTicker(time.Second * 10)
	go func() {
		for range ticker.C {
			m.stats.report()
		}
	}()
	defer ticker.Stop()

	// 并发迁移表
	for _, table := range m.config.Source.Tables {
		wg.Add(1)
		go func(table TableMapping) {
			defer wg.Done()
			if err := m.migrateTable(ctx, table); err != nil {
				errChan <- fmt.Errorf("迁移表 %s 失败: %v", table.Name, err)
			}
		}(table)
	}

	// 等待所有迁移完成
	wg.Wait()
	close(errChan)

	// 收集错误
	var errors []string
	for err := range errChan {
		errors = append(errors, err.Error())
	}

	if len(errors) > 0 {
		return fmt.Errorf("迁移过程中发生错误:\n%s", strings.Join(errors, "\n"))
	}

	m.stats.report() // 最终报告
	return nil
}

func (m *Migration) migrateTable(ctx context.Context, table TableMapping) error {
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
		logrus.Infof("表 %s 在源数据库中不存在，跳过", table.Name)
		return fmt.Errorf("表 %s 不存在", table.Name)
	}

	// 首先检查断点
	checkpoint, err := m.loadCheckpoint(table.Name)
	if err != nil {
		logrus.Infof("加载断点信息失败: %v, 将从头开始迁移", err)
	} else if checkpoint != nil && checkpoint.AllComplete {
		logrus.Infof("表 %s 已完成迁移，跳过", table.Name)
		return nil
	}

	targetName := table.Name
	if table.TargetName != "" {
		targetName = table.TargetName
	}

	// 先迁移自定义类型和函数
	if err := m.migrateDependencies(); err != nil {
		return fmt.Errorf("迁移依赖项失败: %v", err)
	}

	// 获取表结构
	schema, err := m.getTableSchema(table.Name)
	if err != nil {
		return fmt.Errorf("获取表结构失败: %v", err)
	}

	// 修改表名
	if targetName != table.Name {
		schema = strings.Replace(schema, table.Name, targetName, 1)
	}

	// 创建目标表
	if err := m.dest.Query(schema).Exec(); err != nil {
		return fmt.Errorf("创建目标表失败: %v", err)
	}

	return m.copyData(ctx, table.Name, targetName, checkpoint)
}

func (m *Migration) migrateDependencies() error {
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
		if createdTypes[typeName] {
			continue
		}

		createType := fmt.Sprintf("CREATE TYPE IF NOT EXISTS %s.%s (%s)",
			m.config.Destination.Keyspace,
			typeName,
			buildTypeFields(fieldNames, fieldTypes))

		if err := m.dest.Query(createType).Exec(); err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				return fmt.Errorf("创建类型失败 %s: %v", typeName, err)
			}
		} else {
			logrus.Infof("创建自定义类型: %s", typeName)
		}
		createdTypes[typeName] = true
	}

	if err := typeIter.Close(); err != nil {
		return fmt.Errorf("获取类型信息失败: %v", err)
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
				return fmt.Errorf("创建函数失败 %s: %v", funcName, err)
			}
		} else {
			logrus.Infof("创建自定义函数: %s", funcName)
		}
		createdFuncs[funcName] = true
	}

	if err := funcIter.Close(); err != nil {
		return fmt.Errorf("获取函数信息失败: %v", err)
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

func (m *Migration) copyData(ctx context.Context, sourceName, targetName string, checkpoint *Checkpoint) error {
	// 在开始处检查完成标记
	if checkpoint != nil && checkpoint.AllComplete {
		logrus.Infof("表 %s 已完成迁移，跳过执行", sourceName)
		return nil
	}

	// 首先获取表的列信息
	keyspace := m.config.Source.Keyspace
	columnsQuery := fmt.Sprintf(`
		SELECT column_name, type 
		FROM system_schema.columns 
		WHERE keyspace_name = '%s' 
		AND table_name = '%s'`, keyspace, sourceName)

	logrus.Infof("执行列信息查询: %s", columnsQuery)

	iter := m.source.Query(columnsQuery).Iter()
	var columnName, columnType string
	columns := make([]gocql.ColumnInfo, 0)

	// 创建列类型映射
	columnTypes := make(map[string]string)
	for iter.Scan(&columnName, &columnType) {
		logrus.Debugf("发现列: %s (类型: %s)", columnName, columnType)
		columns = append(columns, gocql.ColumnInfo{
			Name: columnName,
		})
		columnTypes[columnName] = columnType
	}

	if err := iter.Close(); err != nil {
		return fmt.Errorf("获取列信息失败: %v", err)
	}

	if len(columns) == 0 {
		return fmt.Errorf("表 %s.%s 中没有找到任何列", keyspace, sourceName)
	}

	logrus.Infof("成功获取到 %d 个列", len(columns))

	// 获取主键列信息
	pkQuery := fmt.Sprintf(`
		SELECT column_name, kind 
		FROM system_schema.columns 
		WHERE keyspace_name = '%s' 
		AND table_name = '%s'`,
		keyspace, sourceName)

	logrus.Infof("执行主键查询: %s", pkQuery)

	pkIter := m.source.Query(pkQuery).Iter()
	var colName, kind string
	pkMap := make(map[string]bool)

	for pkIter.Scan(&colName, &kind) {
		if kind == "partition_key" || kind == "clustering" {
			logrus.Debugf("主键列: %s (%s)", colName, kind)
			pkMap[colName] = true
		}
	}

	if err := pkIter.Close(); err != nil {
		return fmt.Errorf("获取主键信息失败: %v", err)
	}

	// 构建查询，包括每列的 TTL（除了主键列）
	queryParts := make([]string, 0, len(columns))
	for _, col := range columns {
		if pkMap[col.Name] {
			// 主键列不需要 TTL
			queryParts = append(queryParts, col.Name)
		} else {
			// 非主键列添加 TTL
			queryParts = append(queryParts, fmt.Sprintf("%s, TTL(%s)", col.Name, col.Name))
		}
	}

	query := fmt.Sprintf("SELECT %s FROM %s.%s",
		strings.Join(queryParts, ", "),
		m.config.Source.Keyspace,
		sourceName)

	logrus.Infof("完整查询语句: %s", query)

	// 只处理断点续传的位置
	if checkpoint != nil && len(checkpoint.LastKey) > 0 && !checkpoint.AllComplete {
		whereClause := m.buildWhereClause(columnTypes, sourceName)
		if whereClause != "" {
			query = fmt.Sprintf("%s WHERE %s", query, whereClause)
			logrus.Infof("从断点继续: %s", whereClause)
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

		// 如果不是主键列，添加TTL值指针
		if !pkMap[col.Name] {
			ttlValues[i] = new(int)
			valuePointers = append(valuePointers, ttlValues[i])
		}

		logrus.Debugf("列 %s (类型: %s) 使用接收器类型: %T", col.Name, colType, values[i])
	}

	// TTL 列使用 int 类型
	for i := len(columns); i < len(values); i++ {
		values[i] = new(int)
		valuePointers = append(valuePointers, values[i])
	}

	logrus.Infof("总列数: %d (数据列: %d, TTL列: %d)",
		len(values), len(queryParts), len(queryParts))

	// 执行数据查询
	logrus.Infof("开始执行数据查询，批次大小: %d", m.config.Migration.BatchSize)
	dataIter := m.source.Query(query).PageSize(m.config.Migration.BatchSize).Iter()
	defer dataIter.Close()

	// 构建插入语句，支持每列的 TTL
	insertParts := make([]string, len(columns))
	placeholders := make([]string, len(columns))
	ttlCount := 0
	for i, col := range columns {
		insertParts[i] = col.Name
		if pkMap[col.Name] {
			// 主键列不需要TTL
			placeholders[i] = "?"
		} else {
			// 非主键列需要TTL
			placeholders[i] = "?"
			ttlCount++
		}
	}

	// 构建基本的插入语句
	insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		targetName,
		strings.Join(insertParts, ", "),
		strings.Join(placeholders, ", "))

	// 为非主键列添加 TTL 子句
	ttlClauses := make([]string, 0)
	for _, col := range columns {
		if !pkMap[col.Name] {
			ttlClauses = append(ttlClauses, fmt.Sprintf("TTL ?"))
		}
	}

	if len(ttlClauses) > 0 {
		insertQuery += " USING " + strings.Join(ttlClauses, " AND ")
	}

	logrus.Debugf("插入语句: %s", insertQuery)
	logrus.Debugf("列数: %d, 主键列数: %d, TTL列数: %d", len(columns), len(columns)-ttlCount, ttlCount)

	// 处理数据
	batch := m.dest.NewBatch(gocql.UnloggedBatch)
	count := 0
	rowCount := 0

	// 定期保存断点，使用配置的间隔时间，默认60秒
	checkpointDelay := time.Duration(m.config.Migration.CheckpointDelay)
	if checkpointDelay == 0 {
		checkpointDelay = 60
	}
	checkpointTicker := time.NewTicker(time.Second * checkpointDelay)
	defer checkpointTicker.Stop()

	// 创建一个变量来存储最后处理的位置
	var lastProcessedKey map[string]string

	go func() {
		for range checkpointTicker.C {
			m.stats.mu.Lock()
			if lastProcessedKey != nil {
				if err := m.saveCheckpoint(sourceName, lastProcessedKey); err != nil {
					logrus.Errorf("保存断点失败: %v", err)
				} else {
					logrus.Infof("保存断点成功，位置: %+v", lastProcessedKey)
				}
			}
			m.stats.mu.Unlock()
		}
	}()

	// 处理数据
	for dataIter.Scan(valuePointers...) {
		rowCount++
		if err := m.limiter.Wait(ctx); err != nil {
			return err
		}

		// 提取数据值
		dataValues := make([]interface{}, len(columns)+ttlCount) // 调整大小为列数+TTL数
		valueIndex := 0
		for i := range columns { // 只使用索引
			// 所有列都需要值
			dataValues[valueIndex] = values[i]
			valueIndex++
		}

		// 添加TTL值
		for i := range columns { // 只使用索引
			if !pkMap[columns[i].Name] {
				dataValues[valueIndex] = ttlValues[i]
				valueIndex++
			}
		}


		// 更新最后处理的位置
		currentKey := make(map[string]string)
		for i, col := range columns {
			if pkMap[col.Name] {
				switch v := values[i].(type) {
				case *string:
					if v != nil {
						currentKey[col.Name] = *v
					}
				case *[]byte:
					if v != nil {
						currentKey[col.Name] = string(*v)
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
					// 对于其他类型，先解引用再转换
					if v != nil {
						currentKey[col.Name] = fmt.Sprintf("%v", reflect.ValueOf(v).Elem().Interface())
					}
				}
			}
		}
		m.stats.mu.Lock()
		lastProcessedKey = currentKey
		m.stats.mu.Unlock()

		// 添加到批处理
		batch.Query(insertQuery, dataValues...)
		count++

		if count >= m.config.Migration.BatchSize {
			if err := m.executeBatchWithRetry(batch); err != nil {
				return err
			}
			batch = m.dest.NewBatch(gocql.UnloggedBatch)
			m.stats.increment(count)
			count = 0
		}
	}

	// 处理最后一批数据
	if count > 0 {
		if err := m.executeBatchWithRetry(batch); err != nil {
			return err
		}
		m.stats.increment(count)
	}

	// 如果迭代器没有错误，说明已经处理完所有数据，标记为完成
	if err := dataIter.Close(); err != nil {
		return fmt.Errorf("查询执行失败: %v", err)
	}

	// 标记为完成
	logrus.Infof("表 %s 迁移完成，标记为完成状态", sourceName)
	completionKey := make(map[string]string)
	for k, v := range lastProcessedKey {
		completionKey[k] = v
	}
	completionKey["completed"] = "true"

	if err := m.saveCheckpoint(sourceName, completionKey); err != nil {
		logrus.Errorf("保存完成标记失败: %v", err)
		return fmt.Errorf("保存完成标记失败: %v", err)
	}

	logrus.Infof("表 %s 迁移完成并已保存完成标记", sourceName)
	return nil
}

func (m *Migration) executeBatchWithRetry(batch *gocql.Batch) error {
	var lastErr error
	for i := 0; i < m.maxRetries; i++ {
		if err := m.dest.ExecuteBatch(batch); err != nil {
			lastErr = err
			time.Sleep(m.retryDelay)
			continue
		}
		return nil
	}
	return fmt.Errorf("执行批量写入失败，重试%d次后仍然失败: %v", m.maxRetries, lastErr)
}

func (m *Migration) getTableSchema(tableName string) (string, error) {
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
		return "", fmt.Errorf("获取表结构失败: %v", err)
	}

	if len(columns) == 0 {
		return "", fmt.Errorf("未找到表 %s 的结构信息", tableName)
	}

	// 获取表的属性，包括默认 TTL
	tableQuery := fmt.Sprintf(`
		SELECT default_time_to_live
		FROM system_schema.tables
		WHERE keyspace_name = '%s' AND table_name = '%s'`,
		keyspace, tableName)

	var defaultTTL int
	if err := m.source.Query(tableQuery).Scan(&defaultTTL); err != nil {
		logrus.Warnf("获取表默认TTL失败: %v", err)
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

func (m *Migration) buildInsertQuery(tableName string, columnNames []string) string {
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

func (m *Migration) loadCheckpoint(tableName string) (*Checkpoint, error) {
	checkpoint, err := m.loadCheckpointFile(tableName)
	if err != nil {
		return nil, err
	}

	// 如果发现完成标记，返回带完成标记的 checkpoint
	if checkpoint != nil && checkpoint.AllComplete {
		logrus.Infof("表 %s 已完成迁移，跳过", tableName)
		return checkpoint, nil // 返回 checkpoint 而不是 nil
	}

	return checkpoint, nil
}

func (m *Migration) loadCheckpointFile(tableName string) (*Checkpoint, error) {
	if m.config.Migration.CheckpointDir == "" {
		return nil, nil
	}

	filename := filepath.Join(m.config.Migration.CheckpointDir,
		fmt.Sprintf("%s.checkpoint", tableName))

	data, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var checkpoint Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, err
	}

	return &checkpoint, nil
}

func (m *Migration) saveCheckpoint(tableName string, lastKey map[string]string) error {
	if m.config.Migration.CheckpointDir == "" {
		return nil
	}

	// 加载现有的checkpoint
	checkpoint, _ := m.loadCheckpoint(tableName)
	if checkpoint == nil {
		logrus.Infof("创建新的checkpoint")
		checkpoint = &Checkpoint{
			LastKey:     make(map[string]string),
			LastUpdated: time.Now(),
			AllComplete: false,
		}
	}

	// 更新断点信息
	if len(lastKey) > 0 {
		logrus.Infof("更新断点位置，lastKey: %+v", lastKey)
		checkpoint.LastKey = lastKey
	}

	checkpoint.LastUpdated = time.Now()

	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}

	filename := filepath.Join(m.config.Migration.CheckpointDir,
		fmt.Sprintf("%s.checkpoint", tableName))

	logrus.Infof("保存断点到文件 %s，checkpoint内容: %+v", filename, checkpoint)
	return os.WriteFile(filename, data, 0644)
}

func (m *Migration) buildWhereClause(columnTypes map[string]string, tableName string) string {
	// 获取分区键
	var partitionKeys []string

	// 获取列信息
	colQuery := fmt.Sprintf(`
		SELECT column_name, kind
		FROM system_schema.columns 
		WHERE keyspace_name = '%s' 
		AND table_name = '%s'`,
		m.config.Source.Keyspace,
		tableName)

	logrus.Infof("执行列信息查询:\n%s", colQuery)
	iter := m.source.Query(colQuery).Iter()
	var colName, kind string

	for iter.Scan(&colName, &kind) {
		if kind == "partition_key" {
			logrus.Debugf("找到分区键: %s", colName)
			partitionKeys = append(partitionKeys, colName)
		}
	}
	iter.Close()

	if len(partitionKeys) == 0 {
		logrus.Warningln("警告: 未找到分区键")
		return ""
	}

	// 加载断点信息
	checkpoint, err := m.loadCheckpoint(tableName)
	if err != nil {
		logrus.Warnf("加载断点失败: %v, 将从头开始", err)
		return ""
	}

	// 如果有断点信息，只使用分区键构建条件
	if checkpoint != nil && len(checkpoint.LastKey) > 0 {
		// 检查是否所有分区键都存在
		allPartitionKeysPresent := true
		partitionKeyValues := make([]string, len(partitionKeys))
		for i, key := range partitionKeys {
			if val, ok := checkpoint.LastKey[key]; ok {
				partitionKeyValues[i] = fmt.Sprintf("'%s'", val)
			} else {
				allPartitionKeysPresent = false
				break
			}
		}

		if allPartitionKeysPresent {
			// 构建 token 条件，包含所有分区键
			whereClause := fmt.Sprintf("token(%s) >= token(%s)",
				strings.Join(partitionKeys, ", "),
				strings.Join(partitionKeyValues, ", "))

			logrus.Infof("断点续传条件:\n  分区键: %v\n  完整条件: %s",
				partitionKeys,
				whereClause)
			return whereClause
		}
	}

	return ""
}

func loadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %v", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %v", err)
	}

	return &config, nil
}

func createClusterConfig(dbConfig DBConfig) *gocql.ClusterConfig {
	cluster := gocql.NewCluster(dbConfig.Hosts...)
	cluster.Keyspace = dbConfig.Keyspace
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: dbConfig.Username,
		Password: dbConfig.Password,
	}
	return cluster
}

// 添加自定义格式器
type customFormatter struct {
	logrus.TextFormatter
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
	msg := fmt.Sprintf("%s[%s] %s%s%s%s\n",
		color,
		timestamp,
		levelText,
		colorReset,
		" ", // 添加一个空格作为分隔符
		entry.Message)

	return []byte(msg), nil
}

func main() {
	// 添加命令行参数
	configFile := flag.String("config", "config.yaml", "配置文件路径")
	flag.Parse()

	// 加载配置文件
	config, err := loadConfig(*configFile)
	if err != nil {
		logrus.Fatalf("加载配置文件 %s 失败: %v", *configFile, err)
	}

	// 设置日志级别
	logLevel := strings.ToLower(config.Migration.LogLevel)
	switch logLevel {
	case "debug":
		logrus.SetLevel(logrus.DebugLevel)
	case "info":
		logrus.SetLevel(logrus.InfoLevel)
	case "warn", "warning":
		logrus.SetLevel(logrus.WarnLevel)
	case "error":
		logrus.SetLevel(logrus.ErrorLevel)
	default:
		// 如果配置文件中没有指定或值无效，则使用环境变量
		if os.Getenv("DEBUG") != "" {
			logrus.SetLevel(logrus.DebugLevel)
		} else {
			logrus.SetLevel(logrus.InfoLevel)
		}
	}

	// 设置日志输出
	if config.Migration.LogFile != "" {
		logFile, err := os.OpenFile(config.Migration.LogFile,
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			logrus.Fatalf("打开日志文件失败: %v", err)
		}
		defer logFile.Close()

		// 同时输出到文件和标准输出
		multiWriter := io.MultiWriter(os.Stdout, logFile)
		logrus.SetOutput(multiWriter)
	}

	// 设置自定义日志格式
	logrus.SetFormatter(&customFormatter{
		TextFormatter: logrus.TextFormatter{
			DisableColors:   false, // 启用颜色
			TimestampFormat: "2006-01-02 15:04:05",
		},
	})

	migration, err := NewMigration(config)
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
