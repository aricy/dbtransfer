package postgresql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"

	"dbtransfer/internal/config"
	"dbtransfer/internal/i18n"
	"dbtransfer/internal/migration"
)

type PostgreSQLMigration struct {
	source             *sql.DB
	dest               *sql.DB
	config             *config.Config
	limiter            *rate.Limiter
	statsMap           map[string]*migration.MigrationStats
	maxRetries         int
	retryDelay         time.Duration
	lastID             int64
	rateLimit          int
	processed          int
	lastTick           time.Time
	lastCheckpointTime time.Time
}

func NewMigration(config *config.Config) (migration.Migration, error) {
	// 初始化日志
	if err := migration.InitLogger(config.Migration.LogFile, config.Migration.LogLevel); err != nil {
		return nil, fmt.Errorf(i18n.Tr("初始化日志失败: %v", "Failed to initialize logger: %v"), err)
	}

	// 设置语言
	if config.Migration.Language != "" {
		i18n.SetLanguage(config.Migration.Language)
	}

	// 检查配置
	if len(config.Source.Hosts) == 0 || len(config.Destination.Hosts) == 0 {
		return nil, fmt.Errorf(i18n.Tr("未配置数据库主机地址", "Database host addresses not configured"))
	}

	// 解析主机和端口
	host, port := "localhost", "5432"
	if hostStr := config.Source.Hosts[0]; hostStr != "" {
		if strings.Contains(hostStr, ":") {
			parts := strings.Split(hostStr, ":")
			host = parts[0]
			port = parts[1]
		} else {
			host = hostStr
		}
	}

	// 创建断点目录
	if config.Migration.CheckpointDir != "" {
		if err := os.MkdirAll(config.Migration.CheckpointDir, 0755); err != nil {
			return nil, fmt.Errorf(i18n.Tr("创建断点目录失败: %v", "Failed to create checkpoint directory: %v"), err)
		}
	}

	// 构建连接字符串
	schema := config.Source.Schema
	if schema == "" {
		schema = "public" // 默认使用public模式
	}

	sourceConnStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable search_path=%s",
		host, port, config.Source.Username, config.Source.Password, config.Source.Database, schema)
	logrus.Infof(i18n.Tr("正在连接源数据库: %s", "Connecting to source database: %s"),
		strings.Replace(sourceConnStr, config.Source.Password, "****", 1))

	// 连接源数据库
	sourceDB, err := sql.Open("postgres", sourceConnStr)
	if err != nil {
		return nil, fmt.Errorf(i18n.Tr("连接源数据库失败: %v", "Failed to connect to source database: %v"), err)
	}

	// 测试连接
	if err = sourceDB.Ping(); err != nil {
		sourceDB.Close()
		return nil, fmt.Errorf(i18n.Tr("源数据库连接测试失败: %v", "Source database connection test failed: %v"), err)
	}

	// 连接目标数据库
	destHost, destPort := "localhost", "5432"
	if hostStr := config.Destination.Hosts[0]; hostStr != "" {
		if strings.Contains(hostStr, ":") {
			parts := strings.Split(hostStr, ":")
			destHost = parts[0]
			destPort = parts[1]
		} else {
			destHost = hostStr
		}
	}

	destSchema := config.Destination.Schema
	if destSchema == "" {
		destSchema = "public" // 默认使用public模式
	}

	destConnStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable search_path=%s",
		destHost, destPort, config.Destination.Username, config.Destination.Password, config.Destination.Database, destSchema)
	destDB, err := sql.Open("postgres", destConnStr)
	if err != nil {
		sourceDB.Close()
		return nil, fmt.Errorf(i18n.Tr("连接目标数据库失败: %v", "Failed to connect to destination database: %v"), err)
	}

	// 修改限流器的初始化
	var limiter *rate.Limiter
	if config.Migration.RateLimit > 0 {
		// 使用每秒限制的行数作为速率，burst 设置为批次大小的一半，确保更平滑的限流
		burstSize := config.Migration.BatchSize / 2
		if burstSize < 1 {
			burstSize = 1
		}
		limiter = rate.NewLimiter(rate.Limit(config.Migration.RateLimit), burstSize)
	}

	// 初始化全局限速器
	migration.InitGlobalLimiter(config.Migration.RateLimit)

	logrus.Info(i18n.Tr("数据库连接成功", "Database connection successful"))
	return &PostgreSQLMigration{
		source:             sourceDB,
		dest:               destDB,
		config:             config,
		limiter:            limiter,
		statsMap:           make(map[string]*migration.MigrationStats),
		maxRetries:         3,
		retryDelay:         time.Second * 5,
		rateLimit:          config.Migration.RateLimit,
		lastTick:           time.Now(),
		lastCheckpointTime: time.Now(),
	}, nil
}

func (m *PostgreSQLMigration) Close() error {
	var errs []string
	if m.source != nil {
		if err := m.source.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("关闭源数据库连接失败: %v", err))
		}
	}
	if m.dest != nil {
		if err := m.dest.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("关闭目标数据库连接失败: %v", err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf(strings.Join(errs, "; "))
	}
	return nil
}

func (m *PostgreSQLMigration) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(m.config.Source.Tables))

	// 创建工作池
	workers := m.config.Migration.Workers
	if workers <= 0 {
		workers = 4 // 默认值
	}
	semaphore := make(chan struct{}, workers)
	logrus.Infof(i18n.Tr("使用 %d 个工作线程进行并发迁移", "Using %d worker threads for concurrent migration"), workers)

	// 验证所有表的转换表达式
	for _, table := range m.config.Source.Tables {
		for _, transform := range table.ColumnTransformations {
			if err := validateTransformExpression(transform.Expression); err != nil {
				return fmt.Errorf(i18n.Tr(
					"表 %s 的列 %s 转换表达式无效: %v",
					"Invalid transform expression for column %s in table %s: %v"),
					table.Name, transform.SourceColumn, err)
			}
		}
	}

	// 并发迁移表
	for _, table := range m.config.Source.Tables {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(table config.TableMapping) {
			defer wg.Done()
			defer func() { <-semaphore }()
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
		return fmt.Errorf(i18n.Tr("迁移过程中发生错误:\n%s", "Migration failed with errors:\n%s"), strings.Join(errors, "\n"))
	}

	return nil
}

func (m *PostgreSQLMigration) migrateTable(ctx context.Context, table config.TableMapping) error {
	logrus.Infof(i18n.Tr("开始迁移表: %s", "Starting migration of table: %s"), table.Name)

	// 首先检查表是否存在
	var exists bool
	checkQuery := fmt.Sprintf("SELECT 1 FROM %s LIMIT 1", table.Name)
	err := m.source.QueryRow(checkQuery).Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		logrus.Warnf(i18n.Tr("表 %s 在源数据库中不存在，跳过", "Table %s does not exist in source database, skipping"), table.Name)
		return fmt.Errorf(i18n.Tr("表 %s 不存在", "Table %s does not exist"), table.Name)
	}

	// 获取表的主键
	primaryKey, err := m.getPrimaryKey(table.Name)
	if err != nil {
		logrus.Errorf(i18n.Tr("获取表 %s 的主键失败: %v", "Failed to get primary key for table %s: %v"), table.Name, err)
		return fmt.Errorf(i18n.Tr("表 %s 必须有主键才能迁移", "Table %s must have a primary key for migration"), table.Name)
	}
	logrus.Infof(i18n.Tr("表 %s 使用主键: %s", "Table %s using primary key: %s"), table.Name, primaryKey)

	// 首先检查断点
	checkpoint, err := m.loadCheckpoint(table.Name)
	if err != nil {
		logrus.Infof(i18n.Tr("加载断点信息失败: %v, 将从头开始迁移", "Failed to load checkpoint: %v, will start migration from beginning"), err)
	} else if checkpoint != nil && checkpoint.Complete {
		logrus.Infof(i18n.Tr("表 %s 已完成迁移，跳过", "Table %s migration already completed, skipping"), table.Name)
		return nil
	}

	// 获取表的列信息
	columns, err := m.getTableColumns(table.Name)
	if err != nil {
		return fmt.Errorf(i18n.Tr("获取表结构失败: %v", "Failed to get table schema: %v"), err)
	}

	// 构建查询语句，包含列转换
	var selectColumns []string
	for _, col := range columns {
		found := false
		// 检查是否有该列的转换规则
		for _, transform := range table.ColumnTransformations {
			if transform.SourceColumn == col {
				// 使用转换表达式
				selectColumns = append(selectColumns, fmt.Sprintf("%s AS %s", transform.Expression, col))
				found = true
				break
			}
		}
		if !found {
			// 没有转换规则，直接使用列名
			selectColumns = append(selectColumns, col)
		}
	}

	// 获取总行数
	var totalRows int64
	lastID := int64(0)

	// 如果有断点，从断点继续
	if checkpoint != nil && checkpoint.LastKey != nil {
		if idStr, ok := checkpoint.LastKey[primaryKey]; ok {
			id, err := strconv.ParseInt(idStr, 10, 64)
			if err == nil {
				lastID = id
				logrus.Infof(i18n.Tr("从断点继续，上次处理到 %s=%d", "Continuing from checkpoint, last processed %s=%d"), primaryKey, lastID)
			}
		}
	}

	// 获取总行数
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", table.Name)
	if err := m.source.QueryRow(countQuery).Scan(&totalRows); err != nil {
		return fmt.Errorf(i18n.Tr("获取总行数失败: %v", "Failed to get total row count: %v"), err)
	}

	// 初始化统计信息
	stats := migration.NewMigrationStats(totalRows, m.config.Migration.ProgressInterval, table.Name)
	m.statsMap[table.Name] = stats

	// 启动进度报告
	stats.StartReporting(time.Duration(m.config.Migration.ProgressInterval) * time.Second)
	defer func() {
		stats.StopReporting()
		delete(m.statsMap, table.Name) // 清理统计信息
	}()

	// 更新总行数
	stats.TotalRows = totalRows

	// 如果有断点，计算已完成的行数
	if lastID > 0 {
		var completedRows int64
		completedQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s <= $1", table.Name, primaryKey)
		if err := m.source.QueryRow(completedQuery, lastID).Scan(&completedRows); err != nil {
			return fmt.Errorf(i18n.Tr("获取已完成行数失败: %v", "Failed to get completed row count: %v"), err)
		}

		// 计算剩余行数
		remainingRows := totalRows - completedRows

		// 设置起始进度
		stats.ProcessedRows = 0         // 从0开始计数
		stats.TotalRows = remainingRows // 设置为剩余行数

		logrus.Infof(i18n.Tr("从断点继续，已完成 %d 行，剩余 %d 行",
			"Continuing from checkpoint, completed %d rows, remaining %d rows"),
			completedRows, remainingRows)

		// 重置统计信息
		stats.ResetStartTime(true)
	} else {
		// 从头开始迁移
		stats.ProcessedRows = 0
		stats.TotalRows = totalRows
		stats.ResetStartTime(false)
	}

	logrus.Infof(i18n.Tr("总行数: %d (从 %s=%d 开始)", "Total rows: %d (starting from %s=%d)"), totalRows, primaryKey, lastID)
	// 输出全局配置信息
	logrus.Infof(i18n.Tr("开始执行数据查询，批次大小: %d, 全局限速: %d 行/秒",
		"Starting data query, batch size: %d, global rate limit: %d rows/sec"),
		m.config.Migration.BatchSize, m.config.Migration.RateLimit)
	// 确定目标表名
	targetName := table.Name
	if table.TargetName != "" {
		targetName = table.TargetName
	}

	// 获取表结构
	schema, err := m.getTableSchema(table.Name)
	if err != nil {
		return fmt.Errorf(i18n.Tr("获取表结构失败: %v", "Failed to get table schema: %v"), err)
	}

	// 创建目标表
	if _, err := m.dest.Exec(schema); err != nil {
		return fmt.Errorf(i18n.Tr("创建表失败: %v", "Failed to create table: %v"), err)
	}

	// 开始迁移数据
	batchSize := m.config.Migration.BatchSize
	if batchSize <= 0 {
		batchSize = 1000 // 默认批处理大小
	}

	// 设置断点刷新间隔
	checkpointTimeInterval := time.Duration(m.config.Migration.CheckpointInterval) * time.Second
	if checkpointTimeInterval <= 0 {
		checkpointTimeInterval = time.Second // 默认每1秒刷新一次断点
	}
	var rowsSinceLastCheckpoint int   // 添加自上次断点以来处理的行数计数器
	m.lastCheckpointTime = time.Now() // 初始化上次断点时间

	// 循环查询数据
	for {
		// 查询数据
		selectQuery := fmt.Sprintf("SELECT %s FROM %s WHERE %s > $1 ORDER BY %s LIMIT $2",
			strings.Join(selectColumns, ", "), table.Name, primaryKey, primaryKey)
		rows, err := m.source.Query(selectQuery, lastID, batchSize)
		if err != nil {
			return fmt.Errorf(i18n.Tr("查询数据失败: %v", "Failed to query data: %v"), err)
		}

		// 处理数据
		batch, count, maxID, err := m.processBatch(rows, batchSize, primaryKey)
		rows.Close()
		if err != nil {
			return err
		}

		if count == 0 {
			lastKey := map[string]string{primaryKey: strconv.FormatInt(lastID, 10)}
			if err := m.saveCheckpoint(table.Name, lastKey, true); err != nil {
				logrus.Errorf(i18n.Tr("保存完成标记失败: %v", "Failed to save completion marker: %v"), err)
			} else {
				logrus.Infof(i18n.Tr("表 %s 迁移完成，共迁移 %d 行", "Table %s migration completed, migrated %d rows"),
					table.Name, stats.ProcessedRows)
			}
			break
		}

		// 执行批量插入
		if err := m.executeBatchWithRetry(ctx, targetName, batch); err != nil {
			return err
		}

		// 更新进度
		stats.Lock()
		stats.ProcessedRows += int64(count)
		stats.Unlock()
		m.lastID = maxID
		lastID = maxID
		rowsSinceLastCheckpoint += count

		// 应用全局限速
		if err := migration.EnforceGlobalRateLimit(ctx, count); err != nil {
			return err
		}

		// 更频繁地保存断点：基于行数或时间间隔
		now := time.Now()
		if rowsSinceLastCheckpoint >= 1000 || now.Sub(m.lastCheckpointTime) >= checkpointTimeInterval {
			if err := m.saveCheckpoint(table.Name, map[string]string{primaryKey: strconv.FormatInt(lastID, 10)}, false); err != nil {
				logrus.Warnf(i18n.Tr("保存断点失败: %v", "Failed to save checkpoint: %v"), err)
			}
			rowsSinceLastCheckpoint = 0
			m.lastCheckpointTime = now
		}
	}

	// 最终报告
	stats.Report()
	logrus.Infof(i18n.Tr("表 %s 迁移完成并已保存完成标记", "Table %s migration completed and completion marker saved"), table.Name)
	return nil
}

// 获取表结构
func (m *PostgreSQLMigration) getTableSchema(tableName string) (string, error) {
	// 获取表的创建语句
	var tableSchema string
	query := `
		SELECT 
			'CREATE TABLE IF NOT EXISTS ' || quote_ident($1) || ' (' ||
			string_agg(
				quote_ident(c.column_name) || ' ' ||
				c.data_type ||
				CASE 
					WHEN c.character_maximum_length IS NOT NULL 
					THEN '(' || c.character_maximum_length || ')'
					ELSE ''
				END ||
				CASE 
					WHEN c.is_nullable = 'NO' 
					THEN ' NOT NULL'
					ELSE ''
				END,
				', '
			) ||
			CASE 
				WHEN pc.contype = 'p' 
				THEN ', PRIMARY KEY (' || 
					string_agg(DISTINCT quote_ident(kcu.column_name), ', ') || 
				')'
				ELSE ''
			END ||
			');'
		FROM information_schema.columns c
		LEFT JOIN (
			SELECT conname, conrelid, contype
			FROM pg_constraint
			WHERE contype = 'p'
		) pc ON pc.conrelid = (
			SELECT oid 
			FROM pg_class 
			WHERE relname = $1 
			  AND relnamespace = (
				SELECT oid 
				FROM pg_namespace 
				WHERE nspname = 'public'
			)
		)
		LEFT JOIN information_schema.key_column_usage kcu
			ON kcu.table_name = c.table_name
			AND kcu.constraint_name = pc.conname
		WHERE c.table_name = $1
		  AND c.table_schema = 'public'
		GROUP BY pc.contype;
	`

	if err := m.source.QueryRow(query, tableName).Scan(&tableSchema); err != nil {
		return "", fmt.Errorf("获取表结构失败: %v", err)
	}

	return tableSchema, nil
}

// 处理批次数据
func (m *PostgreSQLMigration) processBatch(rows *sql.Rows, batchSize int, primaryKey string) ([]interface{}, int, int64, error) {
	// 获取列信息
	columns, err := rows.ColumnTypes()
	if err != nil {
		return nil, 0, 0, fmt.Errorf(i18n.Tr("获取列信息失败: %v", "Failed to get column information: %v"), err)
	}

	// 准备批量数据
	batch := make([]interface{}, 0, batchSize)
	count := 0
	var maxID int64 = 0

	// 处理每一行数据
	for rows.Next() {
		// 创建一个值的切片，用于扫描行数据
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		// 扫描行数据
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, 0, 0, fmt.Errorf(i18n.Tr("扫描行数据失败: %v", "Failed to scan row data: %v"), err)
		}

		// 将行数据添加到批处理中
		batch = append(batch, values)
		count++

		// 更新最大ID
		for i, col := range columns {
			if col.Name() == primaryKey {
				if id, ok := values[i].(int64); ok && id > maxID {
					maxID = id
				} else if id, ok := values[i].(int32); ok && int64(id) > maxID {
					maxID = int64(id)
				} else if id, ok := values[i].(int); ok && int64(id) > maxID {
					maxID = int64(id)
				} else if idStr, ok := values[i].(string); ok {
					if id, err := strconv.ParseInt(idStr, 10, 64); err == nil && id > maxID {
						maxID = id
					}
				}
				break
			}
		}
	}

	// 检查是否有错误
	if err := rows.Err(); err != nil {
		return nil, 0, 0, fmt.Errorf(i18n.Tr("读取行数据时发生错误: %v", "Error occurred while reading row data: %v"), err)
	}

	return batch, count, maxID, nil
}

// 执行批量插入
func (m *PostgreSQLMigration) executeBatchWithRetry(ctx context.Context, tableName string, batch []interface{}) error {
	var err error
	for i := 0; i <= m.maxRetries; i++ {
		err = m.insertBatch(ctx, tableName, batch)
		if err == nil {
			return nil
		}

		// 如果不是最后一次尝试，则等待后重试
		if i < m.maxRetries {
			logrus.Warnf(i18n.Tr("批量插入失败，将在 %v 后重试: %v",
				"Batch insert failed, will retry in %v: %v"),
				m.retryDelay, err)

			select {
			case <-time.After(m.retryDelay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return fmt.Errorf(i18n.Tr("批量插入失败，已重试 %d 次: %v",
		"Batch insert failed after %d retries: %v"),
		m.maxRetries, err)
}

// 构建更新子句
func buildUpdateClause(columns []string, primaryKey string) string {
	updates := make([]string, 0, len(columns))
	for _, col := range columns {
		if col != primaryKey { // 跳过主键
			updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
		}
	}
	return strings.Join(updates, ", ")
}

func (m *PostgreSQLMigration) loadCheckpoint(tableName string) (*migration.Checkpoint, error) {
	if m.config.Migration.CheckpointDir == "" {
		return nil, nil
	}

	filename := filepath.Join(m.config.Migration.CheckpointDir,
		fmt.Sprintf("postgresql_%s.checkpoint", tableName))

	data, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf(i18n.Tr("读取断点文件失败: %v", "Failed to read checkpoint file: %v"), err)
	}

	var checkpoint migration.Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, fmt.Errorf(i18n.Tr("解析断点数据失败: %v", "Failed to parse checkpoint data: %v"), err)
	}

	return &checkpoint, nil
}

func (m *PostgreSQLMigration) saveCheckpoint(tableName string, lastKey map[string]string, complete bool) error {
	// 确保断点目录存在
	if err := os.MkdirAll(m.config.Migration.CheckpointDir, 0755); err != nil {
		return fmt.Errorf(i18n.Tr("创建断点目录失败: %v", "Failed to create checkpoint directory: %v"), err)
	}

	checkpoint := &migration.Checkpoint{
		LastKey:     lastKey,
		LastUpdated: time.Now(),
		Complete:    complete,
	}

	data, err := json.Marshal(checkpoint)
	if err != nil {
		return fmt.Errorf(i18n.Tr("序列化断点数据失败: %v", "Failed to serialize checkpoint data: %v"), err)
	}

	// 创建临时文件
	tempFile := filepath.Join(m.config.Migration.CheckpointDir,
		fmt.Sprintf("postgresql_%s.checkpoint.tmp", tableName))
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return fmt.Errorf(i18n.Tr("写入断点文件失败: %v", "Failed to write checkpoint file: %v"), err)
	}

	// 确保数据已写入磁盘
	f, err := os.Open(tempFile)
	if err != nil {
		return fmt.Errorf(i18n.Tr("打开临时断点文件失败: %v", "Failed to open temporary checkpoint file: %v"), err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf(i18n.Tr("同步断点文件失败: %v", "Failed to sync checkpoint file: %v"), err)
	}
	f.Close()

	// 原子地重命名文件，确保断点文件的完整性
	if err := os.Rename(tempFile, filepath.Join(m.config.Migration.CheckpointDir,
		fmt.Sprintf("postgresql_%s.checkpoint", tableName))); err != nil {
		return fmt.Errorf(i18n.Tr("重命名断点文件失败: %v", "Failed to rename checkpoint file: %v"), err)
	}

	return nil
}

// 辅助函数
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// 添加一个函数来获取表的主键
func (m *PostgreSQLMigration) getPrimaryKey(tableName string) (string, error) {
	query := `
		SELECT a.attname
		FROM   pg_index i
		JOIN   pg_attribute a ON a.attrelid = i.indrelid
								AND a.attnum = ANY(i.indkey)
		WHERE  i.indrelid = $1::regclass
		AND    i.indisprimary;
	`

	var primaryKey string
	err := m.source.QueryRow(query, tableName).Scan(&primaryKey)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf(i18n.Tr("表 %s 没有主键", "Table %s has no primary key"), tableName)
		}
		return "", err
	}

	return primaryKey, nil
}

// 添加获取列信息的方法
func (m *PostgreSQLMigration) getTableColumns(tableName string) ([]string, error) {
	query := `
		SELECT column_name 
		FROM information_schema.columns 
		WHERE table_name = $1 
		ORDER BY ordinal_position;
	`
	rows, err := m.source.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf(i18n.Tr("查询列信息失败: %v", "Failed to query column information: %v"), err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf(i18n.Tr("读取列名失败: %v", "Failed to read column name: %v"), err)
		}
		columns = append(columns, column)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return columns, nil
}

// 批量插入数据
func (m *PostgreSQLMigration) insertBatch(ctx context.Context, tableName string, batch []interface{}) error {
	if len(batch) == 0 {
		return nil
	}

	// 获取第一行数据来确定列
	firstRow := batch[0].([]interface{})
	rowCount := len(batch)
	colCount := len(firstRow)

	// 获取列名
	columns, err := m.getTableColumns(tableName)
	if err != nil {
		return fmt.Errorf(i18n.Tr("获取列名失败: %v", "Failed to get column names: %v"), err)
	}

	if len(columns) != colCount {
		return fmt.Errorf(i18n.Tr("列数不匹配: 表有 %d 列，数据有 %d 列", "Column count mismatch: table has %d columns, data has %d columns"), len(columns), colCount)
	}

	// 获取主键
	primaryKey, err := m.getPrimaryKey(tableName)
	if err != nil {
		return fmt.Errorf(i18n.Tr("获取主键失败: %v", "Failed to get primary key: %v"), err)
	}

	// 构建参数占位符
	placeholders := make([]string, rowCount)
	valueArgs := make([]interface{}, 0, rowCount*colCount)

	for i := 0; i < rowCount; i++ {
		rowPlaceholders := make([]string, colCount)
		for j := 0; j < colCount; j++ {
			rowPlaceholders[j] = fmt.Sprintf("$%d", i*colCount+j+1)
			valueArgs = append(valueArgs, batch[i].([]interface{})[j])
		}
		placeholders[i] = fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", "))
	}

	// 构建 INSERT 语句
	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s ON CONFLICT (%s) DO UPDATE SET %s",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
		primaryKey,
		buildUpdateClause(columns, primaryKey),
	)

	// 执行插入
	_, err = m.dest.ExecContext(ctx, insertSQL, valueArgs...)
	return err
}

// 添加一个辅助函数来验证转换表达式的安全性
func validateTransformExpression(expr string) error {
	// 这里可以添加一些基本的安全检查
	// 例如：不允许 DELETE, DROP, TRUNCATE 等危险操作
	dangerousKeywords := []string{
		"DELETE", "DROP", "TRUNCATE", "ALTER", "CREATE",
		"GRANT", "REVOKE", "EXECUTE", "FUNCTION", "PROCEDURE",
	}

	exprUpper := strings.ToUpper(expr)
	for _, keyword := range dangerousKeywords {
		if strings.Contains(exprUpper, keyword) {
			return fmt.Errorf(i18n.Tr(
				"转换表达式包含不安全的关键字: %s",
				"Transform expression contains unsafe keyword: %s"),
				keyword)
		}
	}

	return nil
}
