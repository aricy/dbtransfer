package mysql

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

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"

	"dbtransfer/internal/config"
	"dbtransfer/internal/i18n"
	"dbtransfer/internal/migration"
)

type MySQLMigration struct {
	source             *sql.DB
	dest               *sql.DB
	config             *config.Config
	statsMap           map[string]*migration.MigrationStats
	statsMutex         sync.Mutex
	maxRetries         int
	retryDelay         time.Duration
	lastCheckpointTime time.Time // 上次保存断点的时间
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
		return nil, fmt.Errorf("%s", i18n.Tr("未配置数据库主机地址", "Database host addresses not configured"))
	}

	// 创建断点目录
	if config.Migration.CheckpointDir != "" {
		if err := os.MkdirAll(config.Migration.CheckpointDir, 0755); err != nil {
			return nil, fmt.Errorf(i18n.Tr("创建断点目录失败: %v", "Failed to create checkpoint directory: %v"), err)
		}
	}

	sourceConnStr := fmt.Sprintf("%s:%s@tcp(%s)/%s",
		config.Source.Username,
		config.Source.Password,
		config.Source.Hosts[0],
		config.Source.Database)
	logrus.Infof(i18n.Tr("正在连接源数据库: %s", "Connecting to source database: %s"),
		strings.Replace(sourceConnStr, config.Source.Password, "****", 1))

	// 连接源数据库
	sourceDB, err := sql.Open("mysql", sourceConnStr)
	if err != nil {
		logrus.Errorf(i18n.Tr("连接源数据库失败: %v", "Failed to connect to source database: %v"), err)
		return nil, fmt.Errorf(i18n.Tr("连接源数据库失败: %v", "Failed to connect to source database: %v"), err)
	}

	// 测试连接
	if err = sourceDB.Ping(); err != nil {
		sourceDB.Close()
		logrus.Errorf(i18n.Tr("源数据库连接测试失败: %v", "Source database connection test failed: %v"), err)
		return nil, fmt.Errorf(i18n.Tr("源数据库连接测试失败: %v", "Source database connection test failed: %v"), err)
	}

	// 连接目标数据库
	destDB, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s",
		config.Destination.Username,
		config.Destination.Password,
		config.Destination.Hosts[0],
		config.Destination.Database))
	if err != nil {
		sourceDB.Close()
		return nil, fmt.Errorf(i18n.Tr("连接目标数据库失败: %v", "Failed to connect to destination database: %v"), err)
	}

	logrus.Info(i18n.Tr("数据库连接成功", "Database connection successful"))

	// 初始化全局限速器
	migration.InitGlobalLimiter(config.Migration.RateLimit, config.Migration.BatchSize)

	return &MySQLMigration{
		source:             sourceDB,
		dest:               destDB,
		config:             config,
		statsMap:           make(map[string]*migration.MigrationStats),
		maxRetries:         3,
		retryDelay:         time.Second * 5,
		lastCheckpointTime: time.Now(),
	}, nil
}

func (m *MySQLMigration) Close() error {
	var errs []string
	if m.source != nil {
		if err := m.source.Close(); err != nil {
			errs = append(errs, fmt.Sprintf(i18n.Tr("关闭源数据库连接失败: %v", "Failed to close source database connection: %v"), err))
		}
	}
	if m.dest != nil {
		if err := m.dest.Close(); err != nil {
			errs = append(errs, fmt.Sprintf(i18n.Tr("关闭目标数据库连接失败: %v", "Failed to close destination database connection: %v"), err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}

func (m *MySQLMigration) Run(ctx context.Context) error {
	// 设置工作线程数
	workers := m.config.Migration.Workers
	if workers <= 0 {
		workers = 4 // 默认值
	}

	// 创建错误通道和等待组
	errChan := make(chan error, len(m.config.Source.Tables))
	var wg sync.WaitGroup
	sem := make(chan struct{}, workers)

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
			sem <- struct{}{}
			defer func() { <-sem }()

			if err := m.migrateTable(ctx, table); err != nil {
				errChan <- fmt.Errorf(i18n.Tr("迁移表 %s 失败: %v", "Failed to migrate table %s: %v"), table.Name, err)
			}
		}(table)
	}

	// 等待所有迁移完成
	wg.Wait()
	close(errChan)

	// 检查是否有错误
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	logrus.Info(i18n.Tr("所有表迁移完成", "All tables migration completed"))
	return nil
}

func (m *MySQLMigration) migrateTable(ctx context.Context, table config.TableMapping) error {
	// 设置批次大小
	batchSize := m.config.Migration.BatchSize

	// 初始化 lastID (Move declaration up)
	var lastID int64 = 0
	var completed bool

	// Move primaryKey declaration up
	var primaryKey string

	// 初始化统计信息
	stats := migration.NewMigrationStats(0, m.config.Migration.ProgressInterval, table.Name)
	m.statsMutex.Lock()
	m.statsMap[table.Name] = stats
	m.statsMutex.Unlock()

	// 启动进度报告
	stats.StartReporting(time.Duration(m.config.Migration.ProgressInterval) * time.Second)
	defer func() {
		stats.StopReporting()
		m.statsMutex.Lock()
		delete(m.statsMap, table.Name) // 清理统计信息
		m.statsMutex.Unlock()
		// Checkpoint save on exit (catches errors, returns, panics, etc.)
		if lastID > 0 && !completed {
			// Using primaryKey captured by closure
			// Note: primaryKey will be set by the time this runs unless error happens super early
			if primaryKey != "" {
				if err := m.saveCheckpoint(table.Name, lastID, primaryKey, false); err != nil {
					logrus.Warnf(i18n.Tr("退出时保存断点失败: %v", "Failed to save checkpoint on exit: %v"), err)
				}
			}
		}
	}()

	logrus.Infof(i18n.Tr("开始迁移表: %s", "Starting migration of table: %s"), table.Name)
	if table.Where != "" {
		logrus.Infof(i18n.Tr("使用自定义 WHERE 条件: %s", "Using custom WHERE condition: %s"), table.Where)
	}

	// 获取或使用配置的主键 (Always needed for checkpoint resumption)
	primaryKey = table.PrimaryKey
	if primaryKey == "" {
		var pkErr error
		primaryKey, pkErr = m.getPrimaryKey(table.Name)
		if pkErr != nil {
			return pkErr
		}
	}
	logrus.Infof(i18n.Tr("使用主键: %s", "Using primary key: %s"), primaryKey)

	// 获取列信息
	columns, colErr := m.getColumns(table.Name)
	if colErr != nil {
		return colErr
	}
	logrus.Debugf(i18n.Tr("获取到的列: %v", "Retrieved columns: %v"), columns)

	// Determine PK index (always needed for checkpoint)
	pkIndex := -1
	for i, col := range columns {
		if col == primaryKey {
			pkIndex = i
			break
		}
	}
	if pkIndex == -1 {
		return fmt.Errorf("column %s not found in table %s", primaryKey, table.Name)
	}

	// 加载断点信息 (Moved after PK determination)
	checkpoint, loadErr := m.loadCheckpoint(table.Name)
	if loadErr != nil {
		logrus.Infof(i18n.Tr("加载断点信息失败: %v, 将从头开始迁移",
			"Failed to load checkpoint: %v, will start migration from beginning"), loadErr)
	} else if checkpoint != nil {
		if checkpoint.Complete {
			logrus.Infof(i18n.Tr("表 %s 已完成迁移，跳过",
				"Table %s migration already completed, skipping"), table.Name)
			return nil
		}
		// 从断点恢复 - Using primaryKey as key
		if idStr, ok := checkpoint.LastKey[primaryKey]; ok {
			if id, err := strconv.ParseInt(idStr, 10, 64); err == nil {
				lastID = id
				logrus.Infof(i18n.Tr("从断点继续，上次处理到 ID: %d", "Continuing from checkpoint, last processed ID: %d"), lastID)
			}
		}
	}

	// 估算总行数（考虑 WHERE 条件和断点恢复）
	if totalRows, err := m.estimateTotalRows(table.Name, table.Where); err == nil && totalRows > 0 {
		if lastID > 0 {
			// 从断点恢复，扣除已完成的行数
			var completedRows int64
			completedQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s <= ?",
				table.Name, primaryKey)
			if table.Where != "" {
				completedQuery = fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE (%s) AND %s <= ?",
					table.Name, table.Where, primaryKey)
			}
			if err := m.source.QueryRow(completedQuery, lastID).Scan(&completedRows); err == nil {
				totalRows -= completedRows
				logrus.Infof(i18n.Tr("从断点恢复，已完成 %d 行，剩余 %d 行", "Resuming from checkpoint, completed %d rows, remaining %d rows"), completedRows, totalRows)
			}
		}
		stats.TotalRows = totalRows
		logrus.Infof(i18n.Tr("表 %s 估算剩余行数: %d", "Table %s estimated remaining rows: %d"), table.Name, totalRows)
	}

	// 构建查询语句
	var query string
	if table.Where != "" {
		// 使用自定义 WHERE 条件
		if len(table.ColumnTransformations) > 0 {
			transformedColumns := make([]string, 0)
			for _, col := range columns {
				// 检查是否有此列的转换
				transformed := false
				for _, transform := range table.ColumnTransformations {
					if transform.SourceColumn == col {
						// 应用转换表达式
						transformedColumns = append(transformedColumns, fmt.Sprintf("%s AS %s", transform.Expression, col))
						transformed = true
						break
					}
				}
				// 如果没有转换，使用原始列名
				if !transformed {
					transformedColumns = append(transformedColumns, col)
				}
			}
			query = fmt.Sprintf("SELECT %s FROM %s WHERE (%s) AND %s > ? ORDER BY %s LIMIT ?",
				strings.Join(transformedColumns, ", "),
				table.Name,
				table.Where,
				primaryKey,
				primaryKey)
		} else {
			query = fmt.Sprintf("SELECT %s FROM %s WHERE (%s) AND %s > ? ORDER BY %s LIMIT ?",
				strings.Join(columns, ", "),
				table.Name,
				table.Where,
				primaryKey,
				primaryKey)
		}
	} else if primaryKey != "" {
		// 使用主键作为条件
		if len(table.ColumnTransformations) > 0 {
			transformedColumns := make([]string, 0)
			for _, col := range columns {
				transformed := false
				for _, transform := range table.ColumnTransformations {
					if transform.SourceColumn == col {
						// 应用转换表达式
						transformedColumns = append(transformedColumns, fmt.Sprintf("%s AS %s", transform.Expression, col))
						transformed = true
						break
					}
				}
				if !transformed {
					transformedColumns = append(transformedColumns, col)
				}
			}
			query = fmt.Sprintf("SELECT %s FROM %s WHERE %s > ? ORDER BY %s LIMIT ?",
				strings.Join(transformedColumns, ", "),
				table.Name,
				primaryKey,
				primaryKey)
		} else {
			// 没有列转换，使用简单查询
			query = fmt.Sprintf("SELECT %s FROM %s WHERE %s > ? ORDER BY %s LIMIT ?",
				strings.Join(columns, ", "),
				table.Name,
				primaryKey,
				primaryKey)
		}
	} else {
		return fmt.Errorf("%s", i18n.Tr("未指定 WHERE 条件且无法获取主键",
			"No WHERE condition specified and unable to get primary key"))
	}
	logrus.Infof(i18n.Tr("执行查询: %s", "Executing query: %s"), query)

	// 获取表结构
	schema, err := m.getTableSchema(table.Name)
	if err != nil {
		return fmt.Errorf(i18n.Tr("获取表结构失败: %v", "Failed to get table schema: %v"), err)
	}

	// 修改表名并添加 IF NOT EXISTS
	if table.TargetName != "" {
		schema = strings.Replace(schema, table.Name, table.TargetName, 1)
	}
	schema = strings.Replace(schema, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)

	// 创建目标表
	if _, err := m.dest.Exec(schema); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf(i18n.Tr("创建目标表失败: %v", "Failed to create target table: %v"), err)
		}
		logrus.Infof(i18n.Tr("表 %s 已存在，继续迁移数据", "Table %s already exists, continuing with data migration"), table.Name)
	} else {
		logrus.Infof(i18n.Tr("成功创建表 %s", "Successfully created table %s"), table.Name)
	}

	// 创建目标表后，开始循环处理数据
	for {
		// 保存断点
		shouldSaveCheckpoint := false

		// 检查行数阈值
		if m.config.Migration.CheckpointRowThreshold > 0 &&
			stats.ProcessedRows%int64(m.config.Migration.CheckpointRowThreshold) == 0 {
			shouldSaveCheckpoint = true
		}

		// 检查时间间隔
		now := time.Now()
		if m.config.Migration.CheckpointInterval > 0 {
			checkpointInterval := time.Duration(m.config.Migration.CheckpointInterval) * time.Second
			if now.Sub(m.lastCheckpointTime) >= checkpointInterval {
				shouldSaveCheckpoint = true
				m.lastCheckpointTime = now
			}
		}

		if shouldSaveCheckpoint {
			if err := m.saveCheckpoint(table.Name, lastID, primaryKey, false); err != nil {
				logrus.Warnf(i18n.Tr("保存断点失败: %v", "Failed to save checkpoint: %v"), err)
			}
		}

		// 执行查询
		var rows *sql.Rows
		if table.Where != "" {
			rows, err = m.source.Query(query, lastID, batchSize)
			logrus.Debugf(i18n.Tr("使用自定义 WHERE 条件执行查询，lastID: %d, batchSize: %d",
				"Executing query with custom WHERE condition, lastID: %d, batchSize: %d"), lastID, batchSize)
		} else {
			rows, err = m.source.Query(query, lastID, batchSize)
			logrus.Debugf(i18n.Tr("使用主键条件执行查询，lastID: %d, batchSize: %d",
				"Executing query with primary key condition, lastID: %d, batchSize: %d"), lastID, batchSize)
		}
		if err != nil {
			return fmt.Errorf(i18n.Tr("查询数据失败: %v", "Failed to query data: %v"), err)
		}
		defer rows.Close()

		// 准备批量插入
		batch := make([]interface{}, 0, batchSize*len(columns))
		values := make([]interface{}, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		var rowCount int
		var hasData bool
		// 处理结果
		for rows.Next() {
			hasData = true
			// 扫描行数据
			if err := rows.Scan(scanArgs...); err != nil {
				return fmt.Errorf(i18n.Tr("扫描行数据失败: %v", "Failed to scan row data: %v"), err)
			}

			// 添加到批次
			batch = append(batch, values...)
			rowCount++

			// 当批次达到指定大小时执行插入
			if rowCount >= batchSize {
				if err := m.executeBatchWithRetry(ctx, table.Name, batch); err != nil {
					return err
				}

				// Global rate limiting
				if err := migration.EnforceGlobalRateLimit(ctx, batchSize); err != nil {
					return err
				}

				batch = batch[:0]
				rowCount = 0
			}

			stats.Increment(1) // 更新统计信息

			// 更新最后处理的 ID (always update for checkpoint)
			if pkIndex >= 0 && pkIndex < len(values) {
				// Safely get ID from the correct column
				val := values[pkIndex]
				if val != nil {
					switch v := val.(type) {
					case int64:
						lastID = v
					case int:
						lastID = int64(v)
					case []byte: // MySQL driver often returns []byte for numbers
						if id, err := strconv.ParseInt(string(v), 10, 64); err == nil {
							lastID = id
						}
					case string:
						if id, err := strconv.ParseInt(v, 10, 64); err == nil {
							lastID = id
						}
					}
				}
			}
		}

		// 处理剩余的数据
		if len(batch) > 0 {
			if err := m.executeBatchWithRetry(ctx, table.Name, batch); err != nil {
				return err
			}
			// Global rate limiting for remaining rows
			if err := migration.EnforceGlobalRateLimit(ctx, rowCount); err != nil {
				return err
			}
		}

		// 如果没有获取到新数据，退出循环
		if !hasData {
			completed = true
			// 保存最终断点
			if err := m.saveCheckpoint(table.Name, lastID, primaryKey, true); err != nil {
				logrus.Warnf(i18n.Tr("保存最终断点失败: %v", "Failed to save final checkpoint: %v"), err)
			}
			break
		}

		// Check if we need to stop
		select {
		case <-ctx.Done():
			// Save checkpoint on interrupt
			if err := m.saveCheckpoint(table.Name, lastID, primaryKey, false); err != nil {
				logrus.Warnf(i18n.Tr("中断时保存断点失败: %v", "Failed to save checkpoint on interrupt: %v"), err)
			}
			return ctx.Err()
		default:
			// Continue processing
		}
	}

	// 最终报告
	logrus.Infof(i18n.Tr("表 %s 迁移完成，共迁移 %d 行数据",
		"Table %s migration completed, migrated %d rows"),
		table.Name, stats.ProcessedRows)

	return nil
}

func (m *MySQLMigration) getColumns(tableName string) ([]string, error) {
	query := fmt.Sprintf("SHOW COLUMNS FROM %s", tableName)
	rows, err := m.source.Query(query)
	if err != nil {
		return nil, fmt.Errorf(i18n.Tr("查询列信息失败: %v", "Failed to query column information: %v"), err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var field, fieldType, null, key, defaultValue, extra sql.NullString
		if err := rows.Scan(&field, &fieldType, &null, &key, &defaultValue, &extra); err != nil {
			return nil, fmt.Errorf(i18n.Tr("读取列信息失败: %v", "Failed to read column information: %v"), err)
		}
		columns = append(columns, field.String)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(i18n.Tr("遍历列信息失败: %v", "Failed to iterate column information: %v"), err)
	}

	return columns, nil
}

func (m *MySQLMigration) getTableSchema(tableName string) (string, error) {
	var tableCreate string
	query := fmt.Sprintf("SHOW CREATE TABLE %s", tableName)
	rows := m.source.QueryRow(query)
	var tableName2 string
	if err := rows.Scan(&tableName2, &tableCreate); err != nil {
		return "", fmt.Errorf(i18n.Tr("获取表结构失败: %v", "Failed to get table schema: %v"), err)
	}
	return tableCreate, nil
}

func (m *MySQLMigration) executeBatchWithRetry(ctx context.Context, tableName string, batch []interface{}) error {
	// 获取列信息
	columns, err := m.getColumns(tableName)
	if err != nil {
		return fmt.Errorf(i18n.Tr("获取列信息失败: %v", "Failed to get column information: %v"), err)
	}

	// 构建基本的插入语句
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = "?"
	}

	// 构建 ON DUPLICATE KEY UPDATE 子句
	updates := make([]string, len(columns))
	for i, col := range columns {
		updates[i] = fmt.Sprintf("%s=VALUES(%s)", col, col)
	}

	// 使用 INSERT INTO ... ON DUPLICATE KEY UPDATE
	insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(updates, ", "))

	// 调用执行方法
	return m.executeBatchInsert(ctx, insertQuery, batch, len(columns))
}

// 重命名原来的方法
func (m *MySQLMigration) executeBatchInsert(ctx context.Context, query string, values []interface{}, colCount int) error {
	for i := 0; i < m.maxRetries; i++ {
		// 打印原始查询
		logrus.Debugf(i18n.Tr("原始查询: %s", "Original query: %s"), query)

		// 准备批量插入的参数
		batchCount := len(values) / colCount
		valueStrings := make([]string, batchCount)
		valueArgs := make([]interface{}, 0, len(values))

		// 构建值占位符和参数
		for i := 0; i < batchCount; i++ {
			valueStrings[i] = "(" + strings.TrimSuffix(strings.Repeat("?,", colCount), ",") + ")"
			baseIndex := i * colCount
			for j := 0; j < colCount; j++ {
				valueArgs = append(valueArgs, values[baseIndex+j])
			}
		}

		// 构建完整的插入语句，保持原始查询中的 ON DUPLICATE KEY UPDATE 部分
		parts := strings.SplitN(query, "VALUES", 2)
		baseQuery := parts[0]
		var duplicateClause string
		if len(parts) > 1 && strings.Contains(parts[1], "ON DUPLICATE KEY UPDATE") {
			duplicateParts := strings.SplitN(parts[1], "ON DUPLICATE KEY UPDATE", 2)
			duplicateClause = "ON DUPLICATE KEY UPDATE" + duplicateParts[1]
		}

		stmt := strings.TrimSpace(baseQuery) + " VALUES " + strings.Join(valueStrings, ",")
		if duplicateClause != "" {
			stmt += " " + duplicateClause
		}

		// 打印调试信息
		logrus.Debugf(i18n.Tr("最终SQL: %s", "Final SQL: %s"), stmt)
		logrus.Debugf(i18n.Tr("值数量: %d, 列数: %d, 批次数: %d", "Value count: %d, column count: %d, batch count: %d"), len(valueArgs), colCount, batchCount)

		// 执行插入
		if _, err := m.dest.Exec(stmt, valueArgs...); err != nil {
			if i == m.maxRetries-1 {
				return fmt.Errorf(i18n.Tr("执行批量插入失败: %v\nSQL: %s\n值数量: %d, 列数: %d, 批次数: %d",
					"Batch insert failed: %v\nSQL: %s\nValue count: %d, column count: %d, batch count: %d"),
					err, stmt, len(valueArgs), colCount, batchCount)
			}
			logrus.Warnf(i18n.Tr("插入失败，准备重试: %v", "Insert failed, preparing to retry: %v"), err)
			time.Sleep(m.retryDelay)
			continue
		}
		return nil
	}
	return fmt.Errorf(i18n.Tr("达到最大重试次数", "Max retries reached"))
}

// 辅助函数

// 添加断点相关方法
func (m *MySQLMigration) loadCheckpoint(tableName string) (*migration.Checkpoint, error) {
	if m.config.Migration.CheckpointDir == "" {
		return nil, nil
	}

	filename := filepath.Join(m.config.Migration.CheckpointDir,
		fmt.Sprintf("mysql_%s.checkpoint", tableName))

	data, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var checkpoint migration.Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, err
	}

	return &checkpoint, nil
}

func (m *MySQLMigration) saveCheckpoint(tableName string, lastID int64, primaryKey string, complete bool) error {
	// 确保断点目录存在
	if err := os.MkdirAll(m.config.Migration.CheckpointDir, 0755); err != nil {
		return fmt.Errorf(i18n.Tr("创建断点目录失败: %v", "Failed to create checkpoint directory: %v"), err)
	}

	checkpoint := &migration.Checkpoint{
		LastKey:     map[string]string{primaryKey: strconv.FormatInt(lastID, 10)},
		LastUpdated: time.Now(),
		Complete:    complete,
	}

	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}

	filename := filepath.Join(m.config.Migration.CheckpointDir,
		fmt.Sprintf("mysql_%s.checkpoint", tableName))

	// 创建临时文件
	tempFile := filename + ".tmp"
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
	if err := os.Rename(tempFile, filename); err != nil {
		return fmt.Errorf(i18n.Tr("重命名断点文件失败: %v", "Failed to rename checkpoint file: %v"), err)
	}

	return nil
}

func (m *MySQLMigration) getPrimaryKey(tableName string) (string, error) {
	query := fmt.Sprintf("SHOW KEYS FROM %s WHERE Key_name = 'PRIMARY'", tableName)
	rows, err := m.source.Query(query)
	if err != nil {
		return "", fmt.Errorf(i18n.Tr("查询主键信息失败: %v", "Failed to query primary key information: %v"), err)
	}
	defer rows.Close()

	var primaryKey string
	// 使用更简单、更通用的方法获取主键
	for rows.Next() {
		// 创建一个动态大小的值数组
		columns, err := rows.Columns()
		if err != nil {
			return "", fmt.Errorf(i18n.Tr("获取列名失败: %v", "Failed to get column names: %v"), err)
		}

		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))

		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return "", fmt.Errorf(i18n.Tr("扫描行失败: %v", "Failed to scan row: %v"), err)
		}

		// Column_name 通常是第5列（索引从1开始）
		// 但为了安全起见，我们查找列名为 "Column_name" 的列
		columnNameIndex := 4 // 默认位置
		for i, colName := range columns {
			if strings.EqualFold(colName, "Column_name") {
				columnNameIndex = i
				break
			}
		}

		// 确保索引在有效范围内
		if columnNameIndex < len(values) {
			if colName, ok := values[columnNameIndex].([]byte); ok {
				primaryKey = string(colName)
				break
			}
		}
	}

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf(i18n.Tr("遍历主键信息失败: %v", "Failed to iterate primary key information: %v"), err)
	}

	if primaryKey == "" {
		return "", fmt.Errorf(i18n.Tr("表 %s 没有主键", "Table %s has no primary key"), tableName)
	}

	return primaryKey, nil
}

// estimateTotalRows returns an estimated row count using information_schema.
func (m *MySQLMigration) estimateTotalRows(tableName, whereClause string) (int64, error) {
	if whereClause != "" {
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", tableName, whereClause)
		var count int64
		err := m.source.QueryRow(query).Scan(&count)
		return count, err
	}
	query := `SELECT TABLE_ROWS FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`
	var count int64
	err := m.source.QueryRow(query, m.config.Source.Database, tableName).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}
