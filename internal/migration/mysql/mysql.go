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
	"dbtransfer/internal/migration"
)

type MySQLMigration struct {
	source     *sql.DB
	dest       *sql.DB
	config     *config.Config
	limiter    *rate.Limiter
	stats      *migration.MigrationStats
	maxRetries int
	retryDelay time.Duration
	lastID     int64 // 记录最后处理的ID
}

func NewMigration(config *config.Config) (migration.Migration, error) {
	// 初始化日志
	if err := migration.InitLogger(config.Migration.LogFile, config.Migration.LogLevel); err != nil {
		return nil, fmt.Errorf("初始化日志失败: %v", err)
	}

	// 检查配置
	if len(config.Source.Hosts) == 0 || len(config.Destination.Hosts) == 0 {
		return nil, fmt.Errorf("未配置数据库主机地址")
	}

	sourceConnStr := fmt.Sprintf("%s:%s@tcp(%s)/%s",
		config.Source.Username,
		config.Source.Password,
		config.Source.Hosts[0],
		config.Source.Database)
	logrus.Infof("正在连接源数据库: %s", strings.Replace(sourceConnStr, config.Source.Password, "****", 1))

	// 连接源数据库
	sourceDB, err := sql.Open("mysql", sourceConnStr)
	if err != nil {
		logrus.Errorf("连接源数据库失败: %v", err)
		return nil, fmt.Errorf("连接源数据库失败: %v", err)
	}

	// 测试连接
	if err = sourceDB.Ping(); err != nil {
		sourceDB.Close()
		logrus.Errorf("源数据库连接测试失败: %v", err)
		return nil, fmt.Errorf("源数据库连接测试失败: %v", err)
	}

	// 连接目标数据库
	destDB, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s",
		config.Destination.Username,
		config.Destination.Password,
		config.Destination.Hosts[0],
		config.Destination.Database))
	if err != nil {
		sourceDB.Close()
		return nil, fmt.Errorf("连接目标数据库失败: %v", err)
	}

	// 修改限流器的初始化
	var limiter *rate.Limiter
	if config.Migration.RateLimit > 0 {
		// 使用每秒限制的行数作为速率
		limiter = rate.NewLimiter(rate.Limit(config.Migration.RateLimit), 1)
	}

	logrus.Info("数据库连接成功")
	return &MySQLMigration{
		source:     sourceDB,
		dest:       destDB,
		config:     config,
		limiter:    limiter,
		stats:      &migration.MigrationStats{StartTime: time.Now()},
		maxRetries: 3,
		retryDelay: time.Second * 5,
	}, nil
}

func (m *MySQLMigration) Close() {
	if m.source != nil {
		m.source.Close()
	}
	if m.dest != nil {
		m.dest.Close()
	}
}

func (m *MySQLMigration) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(m.config.Source.Tables))

	// 创建工作池
	workers := m.config.Migration.Workers
	if workers <= 0 {
		workers = 4 // 默认值
	}
	semaphore := make(chan struct{}, workers)
	logrus.Infof("使用 %d 个工作线程进行并发迁移", workers)

	// 设置进度报告间隔
	interval := time.Duration(m.config.Migration.ProgressInterval)
	if interval <= 0 {
		interval = 5 // 默认5秒
	}
	logrus.Infof("进度报告间隔: %d 秒", interval)

	// 启动进度报告
	ticker := time.NewTicker(time.Second * interval)
	go func() {
		for range ticker.C {
			m.stats.Report()
		}
	}()
	defer ticker.Stop()

	// 并发迁移表
	for _, table := range m.config.Source.Tables {
		wg.Add(1)
		// 获取信号量
		semaphore <- struct{}{}
		go func(table config.TableMapping) {
			defer wg.Done()
			defer func() { <-semaphore }() // 释放信号量
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

	m.stats.Report() // 最终报告
	return nil
}

func (m *MySQLMigration) migrateTable(ctx context.Context, table config.TableMapping) error {
	logrus.Infof("开始迁移表: %s", table.Name)

	// 首先检查表是否存在
	var exists bool
	checkQuery := fmt.Sprintf("SELECT 1 FROM %s LIMIT 1", table.Name)
	err := m.source.QueryRow(checkQuery).Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		logrus.Infof("表 %s 在源数据库中不存在，跳过", table.Name)
		return fmt.Errorf("表 %s 不存在", table.Name)
	}

	// 首先检查断点
	checkpoint, err := m.loadCheckpoint(table.Name)
	if err != nil {
		logrus.Infof("加载断点信息失败: %v, 将从头开始迁移", err)
	} else if checkpoint != nil && checkpoint.Complete {
		logrus.Infof("表 %s 已完成迁移，跳过", table.Name)
		return nil
	}

	targetName := table.Name
	if table.TargetName != "" {
		targetName = table.TargetName
	}

	// 获取表结构
	schema, err := m.getTableSchema(table.Name)
	if err != nil {
		return fmt.Errorf("获取表结构失败: %v", err)
	}

	// 修改表名并添加 IF NOT EXISTS
	if targetName != table.Name {
		schema = strings.Replace(schema, table.Name, targetName, 1)
	}
	schema = strings.Replace(schema, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)

	// 创建目标表
	if _, err := m.dest.Exec(schema); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("创建目标表失败: %v", err)
		}
		logrus.Infof("表 %s 已存在，继续迁移数据", targetName)
	} else {
		logrus.Infof("成功创建表 %s", targetName)
	}

	// 获取总行数
	var totalRows int64
	var lastID int64
	if checkpoint != nil {
		if id, ok := checkpoint.LastKey["id"]; ok {
			lastID, _ = strconv.ParseInt(id, 10, 64)
		}
	}

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id > ?", table.Name)
	if err := m.source.QueryRow(countQuery, lastID).Scan(&totalRows); err != nil {
		return fmt.Errorf("获取总行数失败: %v", err)
	}
	m.stats.TotalRows = totalRows

	logrus.Infof("总行数: %d (从ID %d 开始)", totalRows, lastID)

	// 设置定期保存断点
	checkpointDelay := time.Duration(m.config.Migration.CheckpointDelay)
	if checkpointDelay == 0 {
		checkpointDelay = 60
	}
	checkpointTicker := time.NewTicker(time.Second * checkpointDelay)
	defer checkpointTicker.Stop()

	// 启动断点保存协程
	go func() {
		for range checkpointTicker.C {
			if m.lastID > 0 {
				lastKey := map[string]string{"id": strconv.FormatInt(m.lastID, 10)}
				if err := m.saveCheckpoint(table.Name, lastKey, false); err != nil {
					logrus.Errorf("保存断点失败: %v", err)
				} else {
					logrus.Infof("保存断点成功，位置: %v", lastKey)
				}
			}
		}
	}()

	// 分批查询和插入数据
	batchSize := m.config.Migration.BatchSize
	logrus.Infof("开始执行数据查询，批次大小: %d, 限速: %d 行/秒",
		batchSize, m.config.Migration.RateLimit)

	// 创建令牌桶，用于批量获取令牌
	var limiter *rate.Limiter
	if m.config.Migration.RateLimit > 0 {
		// 使用较大的 burst 值来允许批量处理
		burst := max(batchSize, m.config.Migration.RateLimit)
		limiter = rate.NewLimiter(rate.Limit(m.config.Migration.RateLimit), burst)
	}

	for {
		// 查询数据
		selectQuery := fmt.Sprintf("SELECT * FROM %s WHERE id > ? ORDER BY id LIMIT ?", table.Name)
		rows, err := m.source.Query(selectQuery, lastID, batchSize)
		if err != nil {
			return fmt.Errorf("查询数据失败: %v", err)
		}

		// 处理数据
		batch, count, maxID, err := m.processBatch(rows, batchSize)
		rows.Close()
		if err != nil {
			return err
		}

		if count == 0 {
			lastKey := map[string]string{"id": strconv.FormatInt(lastID, 10)}
			if err := m.saveCheckpoint(table.Name, lastKey, true); err != nil {
				logrus.Errorf("保存完成标记失败: %v", err)
			} else {
				logrus.Infof("表 %s 迁移完成，共迁移 %d 行", table.Name, m.stats.ProcessedRows)
			}
			break
		}

		// 执行批量插入前进行限流
		if limiter != nil {
			// 一次性获取所需的所有令牌
			reservation := limiter.ReserveN(time.Now(), count)
			if !reservation.OK() {
				return fmt.Errorf("无法获取足够的令牌")
			}
			delay := reservation.Delay()
			if delay > 0 {
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}

		// 执行批量插入
		if err := m.executeBatchWithRetry(ctx, targetName, batch); err != nil {
			return err
		}

		// 更新进度
		m.stats.Increment(count)
		m.lastID = maxID
		lastID = maxID
	}

	// 最终报告
	m.stats.Report()
	logrus.Infof("表 %s 迁移完成并已保存完成标记", table.Name)
	return nil
}

func (m *MySQLMigration) getColumns(tableName string) ([]string, error) {
	query := fmt.Sprintf("SHOW COLUMNS FROM %s", tableName)
	rows, err := m.source.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var field, type_, null, key, extra string
		var default_ sql.NullString
		if err := rows.Scan(&field, &type_, &null, &key, &default_, &extra); err != nil {
			return nil, err
		}
		columns = append(columns, field)
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

func (m *MySQLMigration) saveCheckpoint(tableName string, lastKey map[string]string, completed bool) error {
	if m.config.Migration.CheckpointDir == "" {
		return nil
	}

	checkpoint := &migration.Checkpoint{
		LastKey:     lastKey,
		LastUpdated: time.Now(),
		Complete:    completed,
	}

	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}

	filename := filepath.Join(m.config.Migration.CheckpointDir,
		fmt.Sprintf("mysql_%s.checkpoint", tableName))

	return os.WriteFile(filename, data, 0644)
}

func (m *MySQLMigration) processBatch(rows *sql.Rows, batchSize int) ([]interface{}, int, int64, error) {
	// 获取列信息
	columns, err := rows.Columns()
	if err != nil {
		return nil, 0, 0, fmt.Errorf("获取列信息失败: %v", err)
	}

	// 准备批量数据
	var batch []interface{}
	count := 0
	var maxID int64

	for rows.Next() {
		// 创建值的容器
		values := make([]interface{}, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &sql.RawBytes{} // 使用 RawBytes 来扫描数据
		}

		// 扫描行
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, 0, 0, fmt.Errorf("扫描行失败: %v", err)
		}

		// 处理每一列的值
		for i := range columns {
			rb := scanArgs[i].(*sql.RawBytes)
			if rb == nil || len(*rb) == 0 {
				values[i] = nil
			} else {
				// 对于第一列（ID），特殊处理
				if i == 0 {
					id, err := strconv.ParseInt(string(*rb), 10, 64)
					if err != nil {
						return nil, 0, 0, fmt.Errorf("解析ID失败: %v", err)
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
		return nil, 0, 0, fmt.Errorf("遍历行时发生错误: %v", err)
	}

	return batch, count, maxID, nil
}

func init() {
	// 设置日志格式
	logrus.SetFormatter(&customFormatter{
		TextFormatter: logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: "2006-01-02 15:04:05",
		},
	})
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
	msg := fmt.Sprintf("[%s] %s%s%s %s\n",
		timestamp,
		color,
		levelText,
		colorReset,
		entry.Message)

	return []byte(msg), nil
}
