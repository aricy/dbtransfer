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
	"dbtransfer/internal/migration"
)

type PostgreSQLMigration struct {
	source     *sql.DB
	dest       *sql.DB
	config     *config.Config
	limiter    *rate.Limiter
	stats      *migration.MigrationStats
	maxRetries int
	retryDelay time.Duration
	lastID     int64
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

	// 解析主机和端口
	host, port := "localhost", "5432"
	if hostStr := config.Source.Hosts[0]; hostStr != "" {
		if strings.Contains(hostStr, ":") {
			parts := strings.Split(hostStr, ":")
			host = parts[0]
			if len(parts) > 1 {
				port = parts[1]
			}
		} else {
			host = hostStr
		}
	}

	// 构建连接字符串
	sourceConnStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host,
		port,
		config.Source.Username,
		config.Source.Password,
		config.Source.Database)

	logrus.Infof("正在连接源数据库: %s", strings.Replace(sourceConnStr, config.Source.Password, "****", 1))

	// 连接源数据库
	sourceDB, err := sql.Open("postgres", sourceConnStr)
	if err != nil {
		return nil, fmt.Errorf("连接源数据库失败: %v", err)
	}

	// 测试连接
	if err = sourceDB.Ping(); err != nil {
		sourceDB.Close()
		return nil, fmt.Errorf("源数据库连接测试失败: %v", err)
	}

	// 设置 search_path
	if _, err = sourceDB.Exec("SET search_path TO public"); err != nil {
		sourceDB.Close()
		return nil, fmt.Errorf("设置 search_path 失败: %v", err)
	}

	// 解析目标主机和端口
	destHost, destPort := "localhost", "5432"
	if hostStr := config.Destination.Hosts[0]; hostStr != "" {
		if strings.Contains(hostStr, ":") {
			parts := strings.Split(hostStr, ":")
			destHost = parts[0]
			if len(parts) > 1 {
				destPort = parts[1]
			}
		} else {
			destHost = hostStr
		}
	}

	// 构建目标连接字符串
	destDB, err := sql.Open("postgres", fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		destHost,
		destPort,
		config.Destination.Username,
		config.Destination.Password,
		config.Destination.Database))
	if err != nil {
		sourceDB.Close()
		return nil, fmt.Errorf("连接目标数据库失败: %v", err)
	}

	// 设置目标数据库 search_path
	if _, err = destDB.Exec("SET search_path TO public"); err != nil {
		sourceDB.Close()
		destDB.Close()
		return nil, fmt.Errorf("设置目标数据库 search_path 失败: %v", err)
	}

	// 创建限流器
	var limiter *rate.Limiter
	if config.Migration.RateLimit > 0 {
		burst := max(config.Migration.BatchSize, config.Migration.RateLimit)
		limiter = rate.NewLimiter(rate.Limit(config.Migration.RateLimit), burst)
	}

	logrus.Info("PostgreSQL 连接成功")
	return &PostgreSQLMigration{
		source:     sourceDB,
		dest:       destDB,
		config:     config,
		limiter:    limiter,
		stats:      &migration.MigrationStats{StartTime: time.Now()},
		maxRetries: 3,
		retryDelay: time.Second * 5,
	}, nil
}

func (m *PostgreSQLMigration) Close() {
	if m.source != nil {
		m.source.Close()
	}
	if m.dest != nil {
		m.dest.Close()
	}
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
	logrus.Infof("使用 %d 个工作线程进行并发迁移", workers)

	// 设置进度报告间隔
	interval := time.Duration(m.config.Migration.ProgressInterval)
	if interval <= 0 {
		interval = 5 // 默认5秒
	}

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
		semaphore <- struct{}{}
		go func(table config.TableMapping) {
			defer wg.Done()
			defer func() { <-semaphore }()
			if err := m.migrateTable(ctx, table); err != nil {
				errChan <- fmt.Errorf("迁移表 %s 失败: %v", table.Name, err)
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
		return fmt.Errorf("迁移过程中发生错误:\n%s", strings.Join(errors, "\n"))
	}

	m.stats.Report() // 最终报告
	return nil
}

func (m *PostgreSQLMigration) migrateTable(ctx context.Context, table config.TableMapping) error {
	logrus.Infof("开始迁移表: %s", table.Name)

	// 创建断点保存计时器
	checkpointTicker := time.NewTicker(time.Second) // 每秒保存一次断点
	defer checkpointTicker.Stop()

	// 获取表的主键
	primaryKey, err := m.getPrimaryKey(table.Name)
	if err != nil {
		logrus.Errorf("获取表 %s 的主键失败: %v", table.Name, err)
		return fmt.Errorf("表 %s 必须有主键才能迁移", table.Name)
	}
	logrus.Infof("表 %s 使用主键: %s", table.Name, primaryKey)

	// 检查断点
	checkpoint, err := m.loadCheckpoint(table.Name)
	if err != nil {
		logrus.Infof("加载断点信息失败: %v, 将从头开始迁移", err)
	} else if checkpoint != nil && checkpoint.Complete {
		logrus.Infof("表 %s 已完成迁移，跳过", table.Name)
		return nil
	}

	// 从断点恢复 lastID
	var lastID int64
	if checkpoint != nil && checkpoint.LastKey != nil {
		if idStr, ok := checkpoint.LastKey[primaryKey]; ok {
			if id, err := strconv.ParseInt(idStr, 10, 64); err == nil {
				lastID = id
				logrus.Infof("从断点恢复，继续从 %s=%d 开始迁移", primaryKey, lastID)
			}
		}
	}

	// 获取总行数
	var totalRows int64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", table.Name)
	if err := m.source.QueryRow(countQuery).Scan(&totalRows); err != nil {
		return fmt.Errorf("获取总行数失败: %v", err)
	}

	// 如果是从断点恢复，获取已迁移的行数
	if lastID > 0 {
		var completedRows int64
		completedQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s <= $1", table.Name, primaryKey)
		if err := m.source.QueryRow(completedQuery, lastID).Scan(&completedRows); err != nil {
			return fmt.Errorf("获取已完成行数失败: %v", err)
		}
		// 设置起始进度
		m.stats.ProcessedRows = 0
		m.stats.TotalRows = totalRows - completedRows
		logrus.Infof("已完成 %d 行，剩余 %d 行需要迁移", completedRows, m.stats.TotalRows)
	} else {
		// 从头开始迁移
		m.stats.ProcessedRows = 0
		m.stats.TotalRows = totalRows
	}

	logrus.Infof("总行数: %d (从 %s=%d 开始)", totalRows, primaryKey, lastID)
	logrus.Infof("开始执行数据查询，批次大小: %d, 限速: %d 行/秒",
		m.config.Migration.BatchSize, m.config.Migration.RateLimit)

	// 确定目标表名
	targetName := table.Name
	if table.TargetName != "" {
		targetName = table.TargetName
	}

	// 获取表结构
	schema, err := m.getTableSchema(table.Name)
	if err != nil {
		return fmt.Errorf("获取表结构失败: %v", err)
	}

	// 创建目标表
	if _, err := m.dest.Exec(schema); err != nil {
		return fmt.Errorf("创建表失败: %v", err)
	}

	// 开始迁移数据
	for {
		// 分批查询数据
		query := fmt.Sprintf("SELECT * FROM %s WHERE %s > $1 ORDER BY %s LIMIT $2", table.Name, primaryKey, primaryKey)
		rows, err := m.source.Query(query, lastID, m.config.Migration.BatchSize)
		if err != nil {
			return fmt.Errorf("查询数据失败: %v", err)
		}

		// 处理批次数据
		batch, count, maxID, err := m.processBatch(rows, primaryKey)
		rows.Close()
		if err != nil {
			return err
		}

		if count == 0 {
			break
		}

		// 执行批量插入
		if err := m.insertBatch(ctx, targetName, batch); err != nil {
			return err
		}

		// 更新进度
		m.stats.Increment(count)
		lastID = maxID

		// 检查是否需要保存断点
		select {
		case <-checkpointTicker.C:
			if err := m.saveCheckpoint(table.Name, map[string]string{primaryKey: strconv.FormatInt(lastID, 10)}, false); err != nil {
				logrus.Warnf("保存断点失败: %v", err)
			}
		default:
		}
	}

	// 标记完成
	if err := m.saveCheckpoint(table.Name, map[string]string{primaryKey: strconv.FormatInt(lastID, 10)}, true); err != nil {
		logrus.Warnf("保存完成标记失败: %v", err)
	}

	logrus.Infof("表 %s 迁移完成，共迁移 %d 行", table.Name, m.stats.ProcessedRows)
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
func (m *PostgreSQLMigration) processBatch(rows *sql.Rows, primaryKey string) ([]interface{}, int, int64, error) {
	// 获取列信息
	columns, err := rows.ColumnTypes()
	if err != nil {
		return nil, 0, 0, fmt.Errorf("获取列信息失败: %v", err)
	}

	// 找到主键列的索引
	primaryKeyIndex := -1
	for i, col := range columns {
		if col.Name() == primaryKey {
			primaryKeyIndex = i
			break
		}
	}

	if primaryKeyIndex == -1 {
		return nil, 0, 0, fmt.Errorf("未找到主键列 %s", primaryKey)
	}

	// 准备批量数据
	var batch []interface{}
	var count int
	var maxID int64

	// 遍历结果集
	for rows.Next() {
		// 创建一个值的切片来保存行数据
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))

		// 为每一列创建一个指针
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		// 扫描行到值指针
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, 0, 0, fmt.Errorf("扫描行失败: %v", err)
		}

		// 处理主键值
		if id, ok := values[primaryKeyIndex].(int64); ok {
			if id > maxID {
				maxID = id
			}
		} else {
			// 尝试转换其他类型到int64
			switch v := values[primaryKeyIndex].(type) {
			case int:
				if int64(v) > maxID {
					maxID = int64(v)
				}
			case int32:
				if int64(v) > maxID {
					maxID = int64(v)
				}
			case string:
				if id, err := strconv.ParseInt(v, 10, 64); err == nil {
					if id > maxID {
						maxID = id
					}
				}
			default:
				return nil, 0, 0, fmt.Errorf("无法处理主键类型: %T", values[primaryKeyIndex])
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

// 执行批量插入
func (m *PostgreSQLMigration) insertBatch(ctx context.Context, tableName string, batch []interface{}) error {
	if len(batch) == 0 {
		return nil
	}

	// 获取列信息
	columns, err := m.getColumns(tableName)
	if err != nil {
		return fmt.Errorf("获取列信息失败: %v", err)
	}

	// 获取主键
	primaryKey, err := m.getPrimaryKey(tableName)
	if err != nil {
		logrus.Warnf("获取表 %s 的主键失败: %v，将使用默认主键 'id'", tableName, err)
		primaryKey = "id"
	}

	// 构建批量插入语句
	rowCount := len(batch) / len(columns)
	valueStrings := make([]string, 0, rowCount)
	valueArgs := make([]interface{}, 0, len(batch))

	for i := 0; i < rowCount; i++ {
		valueParams := make([]string, len(columns))
		for j := range columns {
			valueParams[j] = fmt.Sprintf("$%d", i*len(columns)+j+1)
		}
		valueStrings = append(valueStrings, fmt.Sprintf("(%s)", strings.Join(valueParams, ", ")))

		// 添加参数
		for j := 0; j < len(columns); j++ {
			valueArgs = append(valueArgs, batch[i*len(columns)+j])
		}
	}

	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s ON CONFLICT (%s) DO UPDATE SET %s",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(valueStrings, ", "),
		primaryKey,
		buildUpdateClause(columns, primaryKey),
	)

	// 执行批量插入
	if m.limiter != nil {
		reservation := m.limiter.ReserveN(time.Now(), rowCount)
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

	// 执行插入
	for i := 0; i < m.maxRetries; i++ {
		_, err := m.dest.ExecContext(ctx, insertSQL, valueArgs...)
		if err == nil {
			return nil
		}
		if i < m.maxRetries-1 {
			logrus.Warnf("插入失败，准备重试: %v", err)
			time.Sleep(m.retryDelay)
			continue
		}
		return fmt.Errorf("批量插入失败: %v", err)
	}

	return nil
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
		return nil, err
	}

	var checkpoint migration.Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, err
	}

	return &checkpoint, nil
}

func (m *PostgreSQLMigration) saveCheckpoint(tableName string, lastKey map[string]string, completed bool) error {
	if m.config.Migration.CheckpointDir == "" {
		return nil
	}

	// 确保 checkpoint 目录存在
	if err := migration.EnsureDir(m.config.Migration.CheckpointDir); err != nil {
		return err
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
		fmt.Sprintf("postgresql_%s.checkpoint", tableName))

	return os.WriteFile(filename, data, 0644)
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
			return "", fmt.Errorf("表 %s 没有主键", tableName)
		}
		return "", err
	}

	return primaryKey, nil
}

// 添加获取列信息的方法
func (m *PostgreSQLMigration) getColumns(tableName string) ([]string, error) {
	query := `
		SELECT column_name 
		FROM information_schema.columns 
		WHERE table_name = $1 
		ORDER BY ordinal_position;
	`
	rows, err := m.source.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询列信息失败: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("读取列名失败: %v", err)
		}
		columns = append(columns, column)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return columns, nil
}
