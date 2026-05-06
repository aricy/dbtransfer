package cassandra

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
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
	"gopkg.in/inf.v0"
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
	lastCheckpointTime time.Time
	estimator          TotalRowEstimator
}

// TableInfo 缓存表的元数据，避免重复查询
type TableInfo struct {
	Columns           []gocql.ColumnInfo
	ColumnTypes       map[string]string
	PkMap             map[string]bool
	PartitionKeys     map[string]bool
	TTLMode           string
	TTLValue          int
	TableConfig       config.TableMapping
	PartitionKeyNames []string // 排序后的分区键名称
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
	migration.InitGlobalLimiter(config.Migration.RateLimit, config.Migration.BatchSize)

	return &CassandraMigration{
		source:             sourceSession,
		dest:               destSession,
		config:             config,
		limiter:            limiter,
		statsMap:           make(map[string]*migration.MigrationStats),
		maxRetries:         3,
		retryDelay:         time.Second * 5,
		lastCheckpointTime: time.Now(),
		estimator:          NewSizeEstimatesEstimatorWithAuth(sourceSession, config.Source.Hosts, config.Source.Username, config.Source.Password),
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
	logrus.Infof(i18n.Tr("开始执行数据查询，读取页大小: %d, 写入批次: %d, 全局限速: %d 行/秒",
		"Starting data query, read page size: %d, write batch: %d, global rate limit: %d rows/sec"),
		m.config.Migration.BatchSize, m.config.Migration.WriteBatchSize, m.config.Migration.RateLimit)

	for _, table := range m.config.Source.Tables {
		// 获取预估总分区数
		totalRows, _, err := m.estimator.EstimateTotalRows(m.config.Source.Keyspace, table.Name)
		if err != nil {
			logrus.Warnf(i18n.Tr("预估总分区数失败: %v", "Failed to estimate total partitions: %v"), err)
		} else {
			logrus.Infof(i18n.Tr("表 %s 来自 system.size_estimates 的总分区数: %d", "Table %s total partitions from system.size_estimates: %d"), table.Name, totalRows)
		}

		avgPartSize := m.estimator.GetAvgPartitionSize()
		rowsPerPart := 1.0
		hasClustering, err := m.hasClusteringKeys(m.config.Source.Keyspace, table.Name)
		if err != nil {
			logrus.Warnf(i18n.Tr("检测 clustering key 失败: %v, 默认 rowsPerPart=1", "Failed to detect clustering keys: %v, defaulting rowsPerPart=1"), err)
		}
		if hasClustering {
			if avgPartSize > 0 {
				avgRowSize, err := m.sampleAvgRowSize(m.config.Source.Keyspace, table.Name)
				if err != nil {
					logrus.Warnf(i18n.Tr("采样行大小失败: %v, 使用默认估算 100 bytes/行", "Failed to sample row size: %v, using default estimate 100 bytes/row"), err)
					avgRowSize = 100
				}
				rowsPerPart = float64(avgPartSize) / float64(avgRowSize)
				if rowsPerPart < 1 {
					rowsPerPart = 1.0
				}
			}
			logrus.Infof(i18n.Tr("表 %s 存在 clustering key, 估算每分区 %.1f 行 (平均分区大小: %d bytes, 采样行大小: %.0f bytes)", "Table %s has clustering keys, estimated %.1f rows/partition (avg partition size: %d bytes, sampled row size: %.0f bytes)"), table.Name, rowsPerPart, avgPartSize, float64(avgPartSize)/rowsPerPart)
		} else {
			logrus.Infof(i18n.Tr("表 %s 无 clustering key, 分区数=行数", "Table %s has no clustering keys, partitions=rows"), table.Name)
		}

		// 估算总行数
		estimatedTotalRows := int64(float64(totalRows) * rowsPerPart)

		stats := migration.NewMigrationStats(estimatedTotalRows, m.config.Migration.ProgressInterval, table.Name)
		m.statsMutex.Lock()
		m.statsMap[table.Name] = stats
		m.statsMutex.Unlock()

		stats.StartReporting(time.Duration(m.config.Migration.ProgressInterval) * time.Second)

		err = m.migrateTable(ctx, table)

		m.statsMutex.Lock()
		delete(m.statsMap, table.Name)
		m.statsMutex.Unlock()
		stats.StopReporting()

		if err != nil {
			return fmt.Errorf(i18n.Tr("迁移表 %s 失败: %v", "Failed to migrate table %s: %v"), table.Name, err)
		}
	}

	return nil
}

func (m *CassandraMigration) migrateTable(ctx context.Context, table config.TableMapping) error {
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

	// 加载断点
	checkpoint, err := m.loadCheckpoint(table.Name)
	if err != nil {
		logrus.Warnf(i18n.Tr("加载断点失败: %v, 将重新开始", "Failed to load checkpoint: %v, restarting"), err)
	}

	var ranges []migration.TokenRange
	if checkpoint != nil && len(checkpoint.Ranges) > 0 {
		ranges = checkpoint.Ranges
		if checkpoint.Complete {
			logrus.Infof(i18n.Tr("表 %s 已完成迁移", "Table %s migration completed"), table.Name)
			return nil
		}
		logrus.Infof(i18n.Tr("从断点恢复，剩余 %d 个范围", "Resuming from checkpoint, %d ranges remaining"), countIncompleteRanges(ranges))
	} else {
		if table.Where != "" {
			ranges = []migration.TokenRange{{Start: 0, End: 0, Complete: false}}
			logrus.Infof(i18n.Tr("使用自定义 WHERE 条件: %s", "Using custom WHERE condition: %s"), table.Where)
		} else {
			numWorkers := m.config.Migration.Workers
			if numWorkers <= 0 {
				numWorkers = 4
			}
			numRanges := numWorkers * 4
			ranges = m.splitTokenRing(numRanges)
			logrus.Infof(i18n.Tr("初始化 %d 个 Token 范围", "Initialized %d token ranges"), len(ranges))
		}
	}

	// 按剩余范围比例调整估算总行数
	m.adjustTotalByRemainingRanges(table.Name, ranges)

	// 创建任务通道
	rangeChan := make(chan migration.TokenRange, len(ranges))
	updateChan := make(chan migration.TokenRange, len(ranges))

	// 启动 Checkpoint 保存协程
	done := make(chan struct{})

	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second * 5)
		defer ticker.Stop()

		// 使用 map 快速更新 range 状态
		rangeMap := make(map[int64]*migration.TokenRange)
		for i := range ranges {
			rangeMap[ranges[i].Start] = &ranges[i]
		}

		dirty := false

		for {
			select {
			case r, ok := <-updateChan:
				if !ok {
					// 通道关闭，保存最终状态
					if dirty {
						if err := m.saveCheckpoint(table.Name, ranges, false); err != nil {
							logrus.Warnf("Failed to save checkpoint on close: %v", err)
						}
					}
					return
				}
				if target, ok := rangeMap[r.Start]; ok {
					*target = r
					dirty = true
				}
			case <-ticker.C:
				if dirty {
					if err := m.saveCheckpoint(table.Name, ranges, false); err != nil {
						logrus.Warnf("Failed to save checkpoint: %v", err)
					}
					dirty = false
				}
			case <-ctx.Done():
				// 保存最终状态再退出
				if dirty {
					if err := m.saveCheckpoint(table.Name, ranges, false); err != nil {
						logrus.Warnf("Failed to save checkpoint on cancel: %v", err)
					}
				}
				return
			}
		}
	}()

	// 启动工作池
	numWorkers := m.config.Migration.Workers
	if numWorkers <= 0 {
		numWorkers = 4
	}

	// 获取表元数据（只查询一次）
	tableInfo, err := m.getTableInfo(table.Name)
	if err != nil {
		return fmt.Errorf(i18n.Tr("获取表信息失败: %v", "Failed to get table info: %v"), err)
	}

	workerWg := sync.WaitGroup{}
	for i := 0; i < numWorkers; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for r := range rangeChan {
				if r.Complete {
					continue
				}

				updatedRange, err := m.copyData(ctx, table.Name, targetName, r, updateChan, tableInfo)
				if err != nil {
					logrus.Errorf(i18n.Tr("处理范围 %d-%d 失败: %v", "Failed to process range %d-%d: %v"), r.Start, r.End, err)
				} else {
					updateChan <- updatedRange
				}
			}
		}()
	}

	// 发送任务
	for _, r := range ranges {
		if !r.Complete {
			rangeChan <- r
		}
	}
	close(rangeChan)

	// 等待所有工作完成
	workerWg.Wait()
	close(updateChan)
	<-done // 等待 checkpoint 保存完成

	// 检查是否所有范围都完成了
	allComplete := true
	for _, r := range ranges {
		if !r.Complete {
			allComplete = false
			break
		}
	}

	if allComplete {
		if err := m.saveCheckpoint(table.Name, ranges, true); err != nil {
			return err
		}
		// 输出最终统计
		m.statsMutex.RLock()
		stats := m.statsMap[table.Name]
		var totalRows int64
		if stats != nil {
			totalRows = stats.ProcessedRows
		}
		m.statsMutex.RUnlock()
		logrus.Infof(i18n.Tr("表 %s 迁移完成，总计 %d 行", "Table %s migration completed, total %d rows"), table.Name, totalRows)
	} else {
		return fmt.Errorf(i18n.Tr("表 %s 迁移未完全完成", "Table %s migration not fully completed"), table.Name)
	}

	return nil
}

func countIncompleteRanges(ranges []migration.TokenRange) int {
	count := 0
	for _, r := range ranges {
		if !r.Complete {
			count++
		}
	}
	return count
}

// adjustTotalByRemainingRanges adjusts stats.TotalRows proportionally based on remaining ranges.
func (m *CassandraMigration) adjustTotalByRemainingRanges(tableName string, ranges []migration.TokenRange) {
	m.statsMutex.RLock()
	stats := m.statsMap[tableName]
	m.statsMutex.RUnlock()
	if stats == nil {
		return
	}

	totalRanges := len(ranges)
	remainingRanges := countIncompleteRanges(ranges)
	if totalRanges == 0 || remainingRanges == totalRanges {
		return // no adjustment needed
	}

	adjusted := float64(stats.TotalRows) * float64(remainingRanges) / float64(totalRanges)
	stats.Lock()
	stats.TotalRows = int64(adjusted)
	stats.Unlock()
	logrus.Infof(i18n.Tr("按剩余范围调整估算: %d/%d 范围, 调整后估算行数: %d", "Adjusted estimate by remaining ranges: %d/%d ranges, adjusted estimated rows: %d"), remainingRanges, totalRanges, int64(adjusted))
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

		createType := fmt.Sprintf("CREATE TYPE IF NOT EXISTS %s.\"%s\" (%s)",
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

		createFunc := fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s."%s" (%s)
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
		fields[i] = fmt.Sprintf("\"%s\" %s", names[i], types[i])
	}
	return strings.Join(fields, ", ")
}

type BatchJob struct {
	Batch   *gocql.Batch
	LastKey map[string]string
	Index   int
	Count   int
}

type BatchResult struct {
	Index   int
	LastKey map[string]string
	Count   int
	Error   error
}

func (m *CassandraMigration) worker(ctx context.Context, jobs <-chan BatchJob, results chan<- BatchResult) {
	for job := range jobs {
		select {
		case <-ctx.Done():
			return
		default:
		}

		err := m.executeBatchWithRetry(job.Batch)
		select {
		case results <- BatchResult{
			Index:   job.Index,
			LastKey: job.LastKey,
			Count:   job.Count,
			Error:   err,
		}:
		case <-ctx.Done():
			return
		}
	}
}

func (m *CassandraMigration) copyData(ctx context.Context, sourceName, targetName string, tokenRange migration.TokenRange, updateChan chan<- migration.TokenRange, tableInfo *TableInfo) (migration.TokenRange, error) {
	// 在开始处检查完成标记
	if tokenRange.Complete {
		return tokenRange, nil
	}

	// 使用缓存的表信息
	columns := tableInfo.Columns
	columnTypes := tableInfo.ColumnTypes
	pkMap := tableInfo.PkMap
	partitionKeys := tableInfo.PartitionKeys
	ttlMode := tableInfo.TTLMode
	ttlValue := tableInfo.TTLValue
	tableConfig := tableInfo.TableConfig

	// 构建查询，包括列转换
	queryParts := make([]string, 0, len(columns))
	for _, col := range columns {
		colType := columnTypes[col.Name]
		isCollection := strings.Contains(colType, "list<") ||
			strings.Contains(colType, "map<") ||
			strings.Contains(colType, "set<")

		// 检查是否有列转换
		transformed := false

		for _, transform := range tableConfig.ColumnTransformations {
			if transform.SourceColumn == col.Name {
				// 使用转换表达式，但保持原始列名
				if pkMap[col.Name] || isCollection || ttlMode == "target" {
					// 主键列、集合类型或目标模式不需要 TTL
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
			if pkMap[col.Name] || isCollection || ttlMode == "target" {
				// 主键列、集合类型或目标模式不需要 TTL
				queryParts = append(queryParts, fmt.Sprintf("\"%s\"", col.Name))
			} else {
				// 非主键且非集合类型的列添加 TTL
				queryParts = append(queryParts, fmt.Sprintf("\"%s\", TTL(\"%s\")", col.Name, col.Name))
			}
		}
	}

	// 构建完整查询
	query := fmt.Sprintf("SELECT %s FROM %s.%s",
		strings.Join(queryParts, ", "),
		m.config.Source.Keyspace,
		sourceName)

	// 构建 Where 子句
	var whereClause string
	if tableConfig.Where != "" {
		// 如果配置了自定义 WHERE 条件，直接使用它（不能同时使用 token range 和主键条件）
		whereClause = tableConfig.Where
	} else {
		// 否则使用 token range 条件
		var err error
		whereClause, err = m.buildRangeWhereClauseWithPKs(tokenRange, tableInfo.PartitionKeyNames)
		if err != nil {
			return tokenRange, fmt.Errorf(i18n.Tr("构建查询条件失败: %v", "Failed to build query condition: %v"), err)
		}
	}

	query = fmt.Sprintf("%s WHERE %s", query, whereClause)
	logrus.Debugf(i18n.Tr("范围查询: %s", "Range query: %s"), query)

	// 为所有列创建值存储（包括TTL列）
	values := make([]interface{}, len(columns))             // 只存储列值
	ttlValues := make([]interface{}, len(columns))          // 存储TTL值
	valuePointers := make([]interface{}, 0, len(columns)*2) // 存储所有指针

	// 根据列类型创建正确的接收器
	for i, col := range columns {
		colType := columnTypes[col.Name]
		goType := m.getGoType(colType)
		// Create **T (pointer to pointer) to handle nullable values
		values[i] = reflect.New(reflect.PtrTo(goType)).Interface()

		// 添加列值指针
		valuePointers = append(valuePointers, values[i])

		// 只为非主键且非集合类型的列添加TTL值指针
		isCollection := strings.Contains(colType, "list<") ||
			strings.Contains(colType, "map<") ||
			strings.Contains(colType, "set<")

		if !pkMap[col.Name] && !isCollection && ttlMode != "target" {
			ttlValues[i] = new(int)
			valuePointers = append(valuePointers, ttlValues[i])
		}

		logrus.Debugf(i18n.Tr("列 %s (类型: %s) 使用接收器类型: %T", "Column %s (type: %s) uses receiver type: %T"), col.Name, colType, values[i])
	}

	logrus.Debugf(i18n.Tr("总列数: %d (数据列: %d, TTL列: %d)", "Total columns: %d (data columns: %d, TTL columns: %d)"),
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

		insertParts[i] = fmt.Sprintf("\"%s\"", col.Name)
		placeholders[i] = "?"

		// 只计算非主键且非集合类型的列
		if !pkMap[col.Name] && !isCollection && ttlMode != "target" {
			ttlCount++
		}
	}

	// 构建基本的插入语句
	insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		targetName,
		strings.Join(insertParts, ", "),
		strings.Join(placeholders, ", "))

	// 只有在有非主键列时才添加 TTL 子句
	if ttlCount > 0 {
		insertQuery += " USING TTL ?"
	}

	logrus.Debugf(i18n.Tr("插入语句: %s", "Insert query: %s"), insertQuery)
	logrus.Debugf(i18n.Tr("列数: %d, 主键列数: %d, TTL列数: %d", "Column count: %d, primary key column count: %d, TTL column count: %d"), len(columns), len(columns)-ttlCount, ttlCount)

	// 初始化并发控制
	numWorkers := 4
	jobs := make(chan BatchJob, numWorkers*2)
	results := make(chan BatchResult, numWorkers*2)
	var wg sync.WaitGroup

	// 启动工作协程
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.worker(ctx, jobs, results)
		}()
	}

	// 启动提交协程
	commitDone := make(chan error, 1)

	// 启动提交协程
	// (上面已声明 commitDone)

	go func() {
		defer close(commitDone)
		var lastProcessedKey map[string]string
		nextIndex := 0
		buffer := make(map[int]BatchResult)

		// 定期更新进度
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case result, ok := <-results:
				if !ok {
					// 所有结果已处理
					return
				}

				if result.Error != nil {
					commitDone <- result.Error
					return
				}

				buffer[result.Index] = result

				// 处理连续的结果
				for {
					if res, exists := buffer[nextIndex]; exists {
						m.statsMap[sourceName].Increment(res.Count)
						lastProcessedKey = res.LastKey
						delete(buffer, nextIndex)
						nextIndex++
					} else {
						break
					}
				}

			case <-ticker.C:
				// 发送进度更新
				if lastProcessedKey != nil {
					tokenRange.LastKey = lastProcessedKey
					// 注意：这里发送的是副本，避免并发问题
					updateChan <- tokenRange
				}

			case <-ctx.Done():
				commitDone <- ctx.Err()
				return
			}
		}
	}()

	// 处理数据
	batch := m.dest.NewBatch(gocql.UnloggedBatch)
	count := 0
	batchIndex := 0
	var currentKey map[string]string

	for dataIter.Scan(valuePointers...) {

		// 提取数据值
		var dataValues []interface{}
		if ttlCount > 0 {
			dataValues = make([]interface{}, len(columns)+1)
		} else {
			dataValues = make([]interface{}, len(columns))
		}

		valueIndex := 0

		// 复制列值
		for i := range columns {
			// values[i] is **T
			valPtr := reflect.ValueOf(values[i]).Elem() // *T
			if !valPtr.IsNil() {
				// *T is not nil, so we have a value. Extract T (or *T if it's a pointer type like *big.Int)
				// Wait, if T is inf.Dec, *T is *inf.Dec. valPtr.Interface() is *inf.Dec.
				// If T is int, *T is *int. valPtr.Interface() is *int.
				// If T is []int, *T is *[]int. valPtr.Interface() is *[]int.

				// We want to pass the value to the insert query.
				// For basic types (int, string), we pass the value (int, string).
				// For structs (inf.Dec, UUID), we pass the value or pointer? gocql Marshal handles both usually.
				// But for collections, we pass slice/map.

				// Let's use valPtr.Elem().Interface() which returns T.
				// Except for big.Int which is a struct but usually passed as pointer?
				// gocql marshals big.Int (value) or *big.Int.

				dataValues[valueIndex] = valPtr.Elem().Interface()
			}
			valueIndex++
		}

		// 如果需要TTL，添加TTL值
		skipRow := false
		if ttlCount > 0 {
			for i := range columns {
				colType := columnTypes[columns[i].Name]
				isCollection := strings.Contains(colType, "list<") ||
					strings.Contains(colType, "map<") ||
					strings.Contains(colType, "set<")

				if !pkMap[columns[i].Name] && !isCollection && ttlValues[i] != nil {
					ttlVal := *(ttlValues[i].(*int))
					// Apply TTL modification if needed
					if ttlMode == "modify" {
						if ttlVal > 0 {
							ttlVal += ttlValue
							if ttlVal <= 0 {
								skipRow = true
								break
							}
						}
					}
					dataValues[len(dataValues)-1] = ttlVal
					break
				}
			}
		}

		if skipRow {
			continue
		}

		// 更新当前键
		currentKey = make(map[string]string)
		for i, col := range columns {
			if partitionKeys[col.Name] {
				valPtr := reflect.ValueOf(values[i]).Elem()
				if !valPtr.IsNil() {
					val := valPtr.Elem().Interface()
					switch v := val.(type) {
					case string:
						currentKey[col.Name] = v
					case []byte:
						currentKey[col.Name] = fmt.Sprintf("%x", v)
					case int, int8, int16, int32, int64:
						currentKey[col.Name] = fmt.Sprintf("%d", v)
					case gocql.UUID:
						currentKey[col.Name] = v.String()
					case float32, float64:
						currentKey[col.Name] = fmt.Sprintf("%g", v)
					case inf.Dec:
						currentKey[col.Name] = v.String()
					case time.Time:
						currentKey[col.Name] = fmt.Sprintf("%d", v.UnixMilli())
					case big.Int:
						currentKey[col.Name] = v.String()
					default:
						currentKey[col.Name] = fmt.Sprintf("%v", v)
					}
				}
			}
		}

		// 添加到批处理
		batch.Query(insertQuery, dataValues...)
		count++

		if count >= m.config.Migration.WriteBatchSize {
			// 在提交批次前进行限速
			if err := migration.EnforceGlobalRateLimit(ctx, count); err != nil {
				return tokenRange, err
			}
			jobs <- BatchJob{
				Batch:   batch,
				LastKey: currentKey,
				Index:   batchIndex,
				Count:   count,
			}
			batch = m.dest.NewBatch(gocql.UnloggedBatch)
			count = 0
			batchIndex++
		}
	}

	// 处理最后一批数据
	if count > 0 {
		// 对最后一批也进行限速
		if err := migration.EnforceGlobalRateLimit(ctx, count); err != nil {
			return tokenRange, err
		}
		jobs <- BatchJob{
			Batch:   batch,
			LastKey: currentKey,
			Index:   batchIndex,
			Count:   count,
		}
	}

	// 关闭任务通道，等待工作协程完成
	close(jobs)
	wg.Wait()
	close(results)

	// 等待提交协程完成
	if err := <-commitDone; err != nil {
		return tokenRange, err
	}

	// 如果迭代器没有错误，说明已经处理完所有数据，标记为完成
	if err := dataIter.Close(); err != nil {
		return tokenRange, fmt.Errorf(i18n.Tr("查询执行失败: %v", "Query execution failed: %v"), err)
	}

	// 标记为完成
	tokenRange.Complete = true
	tokenRange.LastKey = nil // 完成后不需要 LastKey

	return tokenRange, nil
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
		columns = append(columns, fmt.Sprintf("\"%s\" %s", columnName, columnType))

		switch kind {
		case "partition_key":
			pkColumns = append(pkColumns, fmt.Sprintf("\"%s\"", columnName))
		case "clustering":
			clusteringColumns = append(clusteringColumns, fmt.Sprintf("\"%s\"", columnName))
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

func (m *CassandraMigration) saveCheckpoint(tableName string, ranges []migration.TokenRange, completed bool) error {
	if m.config.Migration.CheckpointDir == "" {
		return nil
	}

	// 确保 checkpoint 目录存在
	if err := migration.EnsureDir(m.config.Migration.CheckpointDir); err != nil {
		return fmt.Errorf(i18n.Tr("创建断点目录失败: %v", "Failed to create checkpoint directory: %v"), err)
	}

	checkpoint := &migration.Checkpoint{
		Ranges:      ranges,
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

func (m *CassandraMigration) splitTokenRing(numRanges int) []migration.TokenRange {
	ranges := make([]migration.TokenRange, 0, numRanges)

	// Cassandra Murmur3Partitioner token range: -2^63 to 2^63-1
	minToken := new(big.Int).SetInt64(math.MinInt64)
	maxToken := new(big.Int).SetInt64(math.MaxInt64)

	// Total range size = 2^64
	totalSize := new(big.Int).Sub(maxToken, minToken)

	step := new(big.Int).Div(totalSize, big.NewInt(int64(numRanges)))

	current := new(big.Int).Set(minToken)

	for i := 0; i < numRanges; i++ {
		next := new(big.Int).Add(current, step)

		// Ensure we don't exceed maxToken for the last range
		if i == numRanges-1 {
			next = maxToken
		}

		ranges = append(ranges, migration.TokenRange{
			Start:    current.Int64(),
			End:      next.Int64(),
			Complete: false,
		})

		current = next
	}

	return ranges
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
		tokenArgs[i] = fmt.Sprintf("\"%s\"", key)
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

func (m *CassandraMigration) getGoType(colType string) reflect.Type {
	colType = strings.TrimSpace(colType)

	// Handle frozen
	if strings.HasPrefix(colType, "frozen<") {
		return m.getGoType(colType[7 : len(colType)-1])
	}

	// Handle collections
	if strings.HasPrefix(colType, "list<") {
		inner := colType[5 : len(colType)-1]
		elemType := m.getGoType(inner)
		return reflect.SliceOf(elemType)
	}
	if strings.HasPrefix(colType, "set<") {
		inner := colType[4 : len(colType)-1]
		elemType := m.getGoType(inner)
		return reflect.SliceOf(elemType)
	}
	if strings.HasPrefix(colType, "map<") {
		parts := splitTypeParams(colType[4 : len(colType)-1])
		if len(parts) != 2 {
			return reflect.TypeOf(map[string]interface{}{})
		}
		keyType := m.getGoType(parts[0])
		valType := m.getGoType(parts[1])
		return reflect.MapOf(keyType, valType)
	}

	// Basic types
	switch {
	case colType == "text" || colType == "varchar" || colType == "ascii":
		return reflect.TypeOf("")
	case colType == "int":
		return reflect.TypeOf(int(0))
	case colType == "bigint" || colType == "counter" || colType == "time":
		return reflect.TypeOf(int64(0))
	case colType == "blob":
		return reflect.TypeOf([]byte{})
	case colType == "boolean":
		return reflect.TypeOf(true)
	case colType == "float":
		return reflect.TypeOf(float32(0))
	case colType == "double":
		return reflect.TypeOf(float64(0))
	case colType == "decimal":
		return reflect.TypeOf(inf.Dec{})
	case colType == "timestamp":
		return reflect.TypeOf(time.Time{})
	case colType == "date":
		return reflect.TypeOf(time.Time{})
	case colType == "uuid" || colType == "timeuuid":
		return reflect.TypeOf(gocql.UUID{})
	case colType == "inet":
		return reflect.TypeOf("")
	case colType == "varint":
		return reflect.TypeOf(big.Int{})
	case colType == "smallint":
		return reflect.TypeOf(int16(0))
	case colType == "tinyint":
		return reflect.TypeOf(int8(0))
	default:
		return reflect.TypeOf("")
	}
}

func splitTypeParams(s string) []string {
	var parts []string
	start := 0
	depth := 0
	for i, c := range s {
		switch c {
		case '<':
			depth++
		case '>':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(s[start:]))
	return parts
}

func (m *CassandraMigration) buildRangeWhereClause(tableName string, r migration.TokenRange, pkMap map[string]bool) (string, error) {
	// 获取分区键
	pks, err := m.getPartitionKeys(tableName)
	if err != nil {
		return "", err
	}

	tokenFunc := fmt.Sprintf("token(%s)", strings.Join(pks, ", "))

	startToken := fmt.Sprintf("%d", r.Start)
	endToken := fmt.Sprintf("%d", r.End)

	// 使用 token(pk) > start AND token(pk) <= end
	return fmt.Sprintf("%s > %s AND %s <= %s", tokenFunc, startToken, tokenFunc, endToken), nil
}

func (m *CassandraMigration) getPartitionKeys(tableName string) ([]string, error) {
	// 查询所有列，在 Go 代码中过滤，避免 ALLOW FILTERING
	query := fmt.Sprintf(`
		SELECT column_name, kind, position
		FROM system_schema.columns 
		WHERE keyspace_name = '%s' 
		AND table_name = '%s'`,
		m.config.Source.Keyspace, tableName)

	iter := m.source.Query(query).Iter()
	var columnName, kind string
	var position int

	type pkInfo struct {
		name     string
		position int
	}
	var pks []pkInfo

	for iter.Scan(&columnName, &kind, &position) {
		// 只收集 partition_key 类型的列
		if kind == "partition_key" {
			pks = append(pks, pkInfo{fmt.Sprintf("\"%s\"", columnName), position})
		}
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to get partition keys: %v", err)
	}

	sort.Slice(pks, func(i, j int) bool {
		return pks[i].position < pks[j].position
	})

	result := make([]string, len(pks))
	for i, pk := range pks {
		result[i] = pk.name
	}

	return result, nil
}

func (m *CassandraMigration) buildRangeWhereClauseWithPKs(r migration.TokenRange, partitionKeyNames []string) (string, error) {
	tokenFunc := fmt.Sprintf("token(%s)", strings.Join(partitionKeyNames, ", "))

	startToken := fmt.Sprintf("%d", r.Start)
	endToken := fmt.Sprintf("%d", r.End)

	// 使用 token(pk) > start AND token(pk) <= end
	return fmt.Sprintf("%s > %s AND %s <= %s", tokenFunc, startToken, tokenFunc, endToken), nil
}

func (m *CassandraMigration) getTableInfo(tableName string) (*TableInfo, error) {
	keyspace := m.config.Source.Keyspace

	// 查询列信息
	columnsQuery := fmt.Sprintf(`
		SELECT column_name, type 
		FROM system_schema.columns 
		WHERE keyspace_name = '%s' 
		AND table_name = '%s'`, keyspace, tableName)

	iter := m.source.Query(columnsQuery).Iter()
	var colName, colType string
	columns := make([]gocql.ColumnInfo, 0)
	columnTypes := make(map[string]string)

	for iter.Scan(&colName, &colType) {
		columns = append(columns, gocql.ColumnInfo{Name: colName})
		columnTypes[colName] = colType
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf(i18n.Tr("获取列信息失败: %v", "Failed to get column information: %v"), err)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf(i18n.Tr("表 %s.%s 中没有找到任何列", "No columns found in table %s.%s"), keyspace, tableName)
	}

	logrus.Infof(i18n.Tr("成功获取到 %d 个列", "Successfully got %d columns"), len(columns))

	// 查询主键信息
	schemaQuery := fmt.Sprintf(`
		SELECT column_name, kind, position
		FROM system_schema.columns
		WHERE keyspace_name = '%s'
		AND table_name = '%s'`,
		keyspace, tableName)

	pkMap := make(map[string]bool)
	partitionKeys := make(map[string]bool)

	type pkInfo struct {
		name     string
		position int
	}
	var pks []pkInfo

	schemaIter := m.source.Query(schemaQuery).Iter()
	var kind string
	var position int

	for schemaIter.Scan(&colName, &kind, &position) {
		switch kind {
		case "partition_key":
			pkMap[colName] = true
			partitionKeys[colName] = true
			pks = append(pks, pkInfo{fmt.Sprintf("\"%s\"", colName), position})
		case "clustering":
			pkMap[colName] = true
		}
	}

	if err := schemaIter.Close(); err != nil {
		return nil, fmt.Errorf(i18n.Tr("获取主键信息失败: %v", "Failed to get primary key information: %v"), err)
	}

	if len(pkMap) == 0 {
		return nil, fmt.Errorf(i18n.Tr("表 %s 没有主键", "Table %s has no primary key"), tableName)
	}

	// 按 position 排序分区键
	sort.Slice(pks, func(i, j int) bool {
		return pks[i].position < pks[j].position
	})

	partitionKeyNames := make([]string, len(pks))
	for i, pk := range pks {
		partitionKeyNames[i] = pk.name
	}

	// 获取表配置
	var tableConfig config.TableMapping
	for _, t := range m.config.Source.Tables {
		if t.Name == tableName {
			tableConfig = t
			break
		}
	}

	// 确定 TTL 模式
	ttlMode := tableConfig.TTL.Mode
	if ttlMode == "" {
		ttlMode = "source"
	}

	logrus.Infof(i18n.Tr("表 %s TTL 模式: %s, 值: %d", "Table %s TTL mode: %s, value: %d"), tableName, ttlMode, tableConfig.TTL.Value)

	return &TableInfo{
		Columns:           columns,
		ColumnTypes:       columnTypes,
		PkMap:             pkMap,
		PartitionKeys:     partitionKeys,
		TTLMode:           ttlMode,
		TTLValue:          tableConfig.TTL.Value,
		TableConfig:       tableConfig,
		PartitionKeyNames: partitionKeyNames,
	}, nil
}

// hasClusteringKeys checks if a table has any clustering columns.
func (m *CassandraMigration) hasClusteringKeys(keyspace, tableName string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT kind
		FROM system_schema.columns
		WHERE keyspace_name = '%s'
		AND table_name = '%s'`,
		keyspace, tableName)

	iter := m.source.Query(query).Iter()
	var kind string
	for iter.Scan(&kind) {
		if kind == "clustering" {
			iter.Close()
			return true, nil
		}
	}
	if err := iter.Close(); err != nil {
		return false, fmt.Errorf("failed to check clustering keys: %w", err)
	}
	return false, nil
}

// sampleAvgRowSize samples up to 1000 rows and returns the estimated average row size in bytes.
func (m *CassandraMigration) sampleAvgRowSize(keyspace, tableName string) (float64, error) {
	query := fmt.Sprintf(`
		SELECT column_name, type
		FROM system_schema.columns
		WHERE keyspace_name = '%s' AND table_name = '%s'`,
		keyspace, tableName)

	type colInfo struct {
		name   string
		goType reflect.Type
	}
	var cols []colInfo
	iter := m.source.Query(query).Iter()
	var colName, colType string
	for iter.Scan(&colName, &colType) {
		cols = append(cols, colInfo{name: colName, goType: m.getGoType(colType)})
	}
	if err := iter.Close(); err != nil {
		return 0, fmt.Errorf("failed to get columns for sampling: %w", err)
	}
	if len(cols) == 0 {
		return 0, fmt.Errorf("no columns found for table %s", tableName)
	}

	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = fmt.Sprintf("\"%s\"", c.name)
	}
	sampleQuery := fmt.Sprintf("SELECT %s FROM %s.%s LIMIT 1000",
		strings.Join(colNames, ", "), keyspace, tableName)

	values := make([]interface{}, len(cols))
	for i, c := range cols {
		values[i] = reflect.New(reflect.PtrTo(c.goType)).Interface()
	}

	sampleIter := m.source.Query(sampleQuery).Iter()
	var totalSize int64
	var rowCount int64
	for sampleIter.Scan(values...) {
		for i, valPtr := range values {
			totalSize += estimateValueSize(valPtr, cols[i].goType)
		}
		rowCount++
	}
	if err := sampleIter.Close(); err != nil {
		return 0, fmt.Errorf("sample query failed: %w", err)
	}
	if rowCount == 0 {
		return 0, fmt.Errorf("table %s has no rows to sample", tableName)
	}

	return float64(totalSize) / float64(rowCount), nil
}

// estimateValueSize estimates the byte size of a deserialized value based on its Go type.
func estimateValueSize(valPtr interface{}, goType reflect.Type) int64 {
	ptrVal := reflect.ValueOf(valPtr)
	if ptrVal.IsNil() || ptrVal.Elem().IsNil() {
		return 0
	}
	val := ptrVal.Elem().Elem().Interface()

	switch v := val.(type) {
	case string:
		return int64(len(v))
	case int:
		return 8
	case int8:
		return 1
	case int16:
		return 2
	case int32:
		return 4
	case int64:
		return 8
	case float32:
		return 4
	case float64:
		return 8
	case bool:
		return 1
	case []byte:
		return int64(len(v))
	case gocql.UUID:
		return 16
	case time.Time:
		return 8
	case big.Int:
		return int64(len(v.Bytes()))
	case inf.Dec:
		return 16
	default:
		rv := reflect.ValueOf(val)
		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			var size int64
			for i := 0; i < rv.Len(); i++ {
				elem := rv.Index(i)
				if elem.CanInterface() {
					size += estimateSimpleSize(elem.Interface())
				}
			}
			if size > 0 {
				return size
			}
			return 100
		case reflect.Map:
			var size int64
			for _, key := range rv.MapKeys() {
				mv := rv.MapIndex(key)
				if key.CanInterface() {
					size += estimateSimpleSize(key.Interface())
				}
				if mv.CanInterface() {
					size += estimateSimpleSize(mv.Interface())
				}
			}
			if size > 0 {
				return size
			}
			return 200
		default:
			s := fmt.Sprintf("%v", val)
			return int64(len(s))
		}
	}
}

// estimateSimpleSize estimates size of a simple scalar value.
func estimateSimpleSize(val interface{}) int64 {
	switch v := val.(type) {
	case string:
		return int64(len(v))
	case int, int64:
		return 8
	case int8:
		return 1
	case int16:
		return 2
	case int32:
		return 4
	case float32:
		return 4
	case float64:
		return 8
	case bool:
		return 1
	case []byte:
		return int64(len(v))
	case gocql.UUID:
		return 16
	case time.Time:
		return 8
	default:
		s := fmt.Sprintf("%v", val)
		return int64(len(s))
	}
}
