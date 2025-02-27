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
	"golang.org/x/time/rate"

	"dbtransfer/internal/config"
	"dbtransfer/internal/i18n"
	"dbtransfer/internal/migration"
)

type MySQLMigration struct {
	source             *sql.DB
	dest               *sql.DB
	config             *config.Config
	limiter            *rate.Limiter
	statsMap           map[string]*migration.MigrationStats
	maxRetries         int
	retryDelay         time.Duration
	lastID             int64     // 记录最后处理的ID
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
		return nil, fmt.Errorf(i18n.Tr("未配置数据库主机地址", "Database host addresses not configured"))
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

	logrus.Info(i18n.Tr("数据库连接成功", "Database connection successful"))

	// 初始化全局限速器
	migration.InitGlobalLimiter(config.Migration.RateLimit)

	return &MySQLMigration{
		source:             sourceDB,
		dest:               destDB,
		config:             config,
		limiter:            limiter,
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
		return fmt.Errorf(strings.Join(errs, "; "))
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
	// 初始化统计信息，即使表已完成也需要创建，避免空指针
	stats := migration.NewMigrationStats(0, m.config.Migration.ProgressInterval, table.Name)
	m.statsMap[table.Name] = stats

	// 启动进度报告
	stats.StartReporting(time.Duration(m.config.Migration.ProgressInterval) * time.Second)
	defer func() {
		stats.StopReporting()
		delete(m.statsMap, table.Name) // 清理统计信息
	}()

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
		// 如果配置中指定了主键，使用配置的主键
		if table.PrimaryKey != "" {
			primaryKey = table.PrimaryKey
		} else {
			logrus.Errorf(i18n.Tr("获取表 %s 的主键失败: %v", "Failed to get primary key for table %s: %v"), table.Name, err)
			return fmt.Errorf(i18n.Tr("表 %s 必须有主键才能迁移", "Table %s must have a primary key for migration"), table.Name)
		}
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

	// 确定目标表名
	targetName := table.Name
	if table.TargetName != "" {
		targetName = table.TargetName
	}

	// 获取列信息
	columns, err := m.getColumns(table.Name)
	if err != nil {
		return fmt.Errorf(i18n.Tr("获取列信息失败: %v", "Failed to get column information: %v"), err)
	}

	// 获取表的总行数
	var totalRows int64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", table.Name)
	err = m.source.QueryRow(countQuery).Scan(&totalRows)
	if err != nil {
		return fmt.Errorf(i18n.Tr("获取总行数失败: %v", "Failed to get total row count: %v"), err)
	}

	// 更新总行数
	stats.TotalRows = totalRows

	// 获取表结构
	schema, err := m.getTableSchema(table.Name)
	if err != nil {
		return fmt.Errorf(i18n.Tr("获取表结构失败: %v", "Failed to get table schema: %v"), err)
	}

	// 修改表名并添加 IF NOT EXISTS
	if targetName != table.Name {
		schema = strings.Replace(schema, table.Name, targetName, 1)
	}
	schema = strings.Replace(schema, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)

	// 创建目标表
	if _, err := m.dest.Exec(schema); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf(i18n.Tr("创建目标表失败: %v", "Failed to create target table: %v"), err)
		}
		logrus.Infof(i18n.Tr("表 %s 已存在，继续迁移数据", "Table %s already exists, continuing with data migration"), targetName)
	} else {
		logrus.Infof(i18n.Tr("成功创建表 %s", "Successfully created table %s"), targetName)
	}

	// 获取总行数
	lastID := int64(0)

	// 如果有断点，从断点继续
	if checkpoint != nil && checkpoint.LastKey != nil {
		if idStr, ok := checkpoint.LastKey[primaryKey]; ok {
			id, err := strconv.ParseInt(idStr, 10, 64)
			if err == nil {
				lastID = id
				logrus.Infof(i18n.Tr("从断点继续，上次处理到 %s=%s", "Continuing from checkpoint, last processed %s=%s"), primaryKey, idStr)
			}
		}
	}

	// 获取表结构
	schema, err = m.getTableSchema(table.Name)
	if err != nil {
		return fmt.Errorf(i18n.Tr("获取表结构失败: %v", "Failed to get table schema: %v"), err)
	}

	// 获取批处理大小
	batchSize := m.config.Migration.BatchSize
	if batchSize <= 0 {
		batchSize = 1000 // 默认批处理大小
	}

	// 循环查询数据
	for {
		// 查询一批数据
		batch, count, maxID, err := m.processBatch(table.Name, columns, primaryKey, lastID, batchSize)
		if err != nil {
			return fmt.Errorf(i18n.Tr("查询数据失败: %v", "Failed to query data: %v"), err)
		}

		if count == 0 {
			break // 没有更多数据
		}

		// 插入数据到目标表
		if err := m.executeBatchWithRetry(ctx, targetName, batch); err != nil {
			return err
		}

		// 更新进度
		stats.Lock()
		stats.ProcessedRows += int64(count)
		stats.Unlock()

		// 应用全局限速
		if err := migration.EnforceGlobalRateLimit(ctx, count); err != nil {
			return err
		}

		// 更新最后处理的ID
		lastID = maxID

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

		// 检查是否需要停止
		select {
		case <-ctx.Done():
			// 在中断时保存断点
			if err := m.saveCheckpoint(table.Name, lastID, primaryKey, false); err != nil {
				logrus.Warnf(i18n.Tr("中断时保存断点失败: %v", "Failed to save checkpoint on interrupt: %v"), err)
			}
			return ctx.Err()
		default:
			// 继续处理
		}
	}

	// 最终报告
	stats.Report()
	logrus.Infof(i18n.Tr("表 %s 迁移完成并已保存完成标记", "Table %s migration completed and completion marker saved"), table.Name)

	// 迁移完成，保存最终断点
	if err := m.saveCheckpoint(table.Name, lastID, primaryKey, true); err != nil {
		logrus.Warnf(i18n.Tr("保存最终断点失败: %v", "Failed to save final checkpoint: %v"), err)
	}

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
		return "", fmt.Errorf("获取表结构失败: %v", err)
	}
	return tableCreate, nil
}

func (m *MySQLMigration) executeBatchWithRetry(ctx context.Context, tableName string, batch []interface{}) error {
	// 获取列信息
	columns, err := m.getColumns(tableName)
	if err != nil {
		return fmt.Errorf("获取列信息失败: %v", err)
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
		logrus.Debugf("原始查询: %s", query)

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
		logrus.Debugf("最终SQL: %s", stmt)
		logrus.Debugf("值数量: %d, 列数: %d, 批次数: %d", len(valueArgs), colCount, batchCount)

		// 执行插入
		if _, err := m.dest.Exec(stmt, valueArgs...); err != nil {
			if i == m.maxRetries-1 {
				return fmt.Errorf("执行批量插入失败: %v\nSQL: %s\n值数量: %d, 列数: %d, 批次数: %d",
					err, stmt, len(valueArgs), colCount, batchCount)
			}
			logrus.Warnf("插入失败，准备重试: %v", err)
			time.Sleep(m.retryDelay)
			continue
		}
		return nil
	}
	return fmt.Errorf("达到最大重试次数")
}

// 辅助函数
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

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

func (m *MySQLMigration) processBatch(tableName string, columns []string, primaryKey string, lastID int64, batchSize int) ([]interface{}, int, int64, error) {
	// 准备扫描目标
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &sql.RawBytes{}
	}

	// 构建查询
	var query string
	if len(m.config.Source.Tables) > 0 {
		// 查找当前表的配置
		var tableConfig config.TableMapping
		for _, t := range m.config.Source.Tables {
			if t.Name == tableName {
				tableConfig = t
				break
			}
		}

		// 如果有列转换，构建带有转换表达式的查询
		if len(tableConfig.ColumnTransformations) > 0 {
			transformedColumns := make([]string, 0)
			for _, col := range columns {
				// 检查是否有此列的转换
				transformed := false
				for _, transform := range tableConfig.ColumnTransformations {
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
			query = fmt.Sprintf("SELECT %s FROM %s WHERE %s > ? ORDER BY %s LIMIT ?",
				strings.Join(transformedColumns, ", "), tableName, primaryKey, primaryKey)
		} else {
			// 没有列转换，使用简单查询
			query = fmt.Sprintf("SELECT %s FROM %s WHERE %s > ? ORDER BY %s LIMIT ?",
				strings.Join(columns, ", "), tableName, primaryKey, primaryKey)
		}
	} else {
		// 没有表配置，使用简单查询
		query = fmt.Sprintf("SELECT %s FROM %s WHERE %s > ? ORDER BY %s LIMIT ?",
			strings.Join(columns, ", "), tableName, primaryKey, primaryKey)
	}

	// 遍历结果集
	rows, err := m.source.Query(query, lastID, batchSize)
	if err != nil {
		return nil, 0, 0, fmt.Errorf(i18n.Tr("查询数据失败: %v", "Failed to query data: %v"), err)
	}
	defer rows.Close()

	count := 0
	maxID := int64(0)
	batch := make([]interface{}, 0, batchSize*len(columns))

	// 遍历结果集
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, 0, 0, fmt.Errorf(i18n.Tr("扫描行失败: %v", "Failed to scan row: %v"), err)
		}

		// 处理每一列的值
		for i := range columns {
			rb := scanArgs[i].(*sql.RawBytes)
			if rb == nil || len(*rb) == 0 {
				values[i] = nil
			} else {
				// 对于主键列，特殊处理
				if i == 0 {
					id, err := strconv.ParseInt(string(*rb), 10, 64)
					if err != nil {
						return nil, 0, 0, fmt.Errorf(i18n.Tr("解析主键 %s 失败: %v", "Failed to parse primary key %s: %v"), primaryKey, err)
					}
					values[i] = id
					if id > maxID {
						maxID = id
					}
				} else {
					values[i] = string(*rb)
				}
			}
		}

		// 添加到批处理
		batch = append(batch, values...)
		count++
	}

	if err := rows.Err(); err != nil {
		return nil, 0, 0, fmt.Errorf(i18n.Tr("遍历行时发生错误: %v", "Failed to iterate rows: %v"), err)
	}

	return batch, count, maxID, nil
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
