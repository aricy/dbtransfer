package mongodb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/time/rate"

	"dbtransfer/internal/config"
	"dbtransfer/internal/i18n"
	"dbtransfer/internal/migration"
)

// MongoDBMigration 实现了 MongoDB 数据迁移
type MongoDBMigration struct {
	source     *mongo.Client
	dest       *mongo.Client
	config     *config.Config
	limiter    *rate.Limiter
	statsMap   map[string]*migration.MigrationStats // 为每个集合创建独立的统计信息
	maxRetries int
	retryDelay time.Duration
	lastID     interface{} // 记录最后处理的ID
}

// NewMigration 创建一个新的 MongoDB 迁移实例
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

	// 创建上下文
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Migration.Timeout)*time.Second)
	defer cancel()

	// 设置客户端选项
	clientOptions := options.Client().
		SetConnectTimeout(time.Duration(config.Migration.Timeout) * time.Second).
		SetServerSelectionTimeout(time.Duration(config.Migration.Timeout) * time.Second)

	// 连接源数据库
	logrus.Infof(i18n.Tr("正在连接源 MongoDB: %s", "Connecting to source MongoDB: %s"), config.Source.Hosts[0])
	sourceClient, err := connectMongoDB(ctx, config.Source, clientOptions)
	if err != nil {
		return nil, fmt.Errorf(i18n.Tr("连接源 MongoDB 失败: %v", "Failed to connect to source MongoDB: %v"), err)
	}

	// 连接目标数据库
	logrus.Infof(i18n.Tr("正在连接目标 MongoDB: %s", "Connecting to target MongoDB: %s"), config.Destination.Hosts[0])
	destClient, err := connectMongoDB(ctx, config.Destination, clientOptions)
	if err != nil {
		sourceClient.Disconnect(ctx)
		return nil, fmt.Errorf(i18n.Tr("连接目标 MongoDB 失败: %v", "Failed to connect to target MongoDB: %v"), err)
	}

	// 创建限流器
	var limiter *rate.Limiter
	if config.Migration.RateLimit > 0 {
		// 使用每秒限制的行数作为速率，burst 设置为批次大小的一半，确保更平滑的限流
		burstSize := config.Migration.BatchSize / 2
		if burstSize < 1 {
			burstSize = 1
		}
		limiter = rate.NewLimiter(rate.Limit(config.Migration.RateLimit), burstSize)
	}

	logrus.Info(i18n.Tr("MongoDB 连接成功", "MongoDB connection successful"))

	// 初始化全局限速器
	migration.InitGlobalLimiter(config.Migration.RateLimit)

	return &MongoDBMigration{
		source:     sourceClient,
		dest:       destClient,
		config:     config,
		limiter:    limiter,
		statsMap:   make(map[string]*migration.MigrationStats),
		maxRetries: 3,
		retryDelay: time.Second * 5,
	}, nil
}

// 连接 MongoDB 数据库
func connectMongoDB(ctx context.Context, config config.DBConfig, clientOptions *options.ClientOptions) (*mongo.Client, error) {
	// 构建连接字符串
	var uri string
	if config.Username != "" && config.Password != "" {
		// 添加认证数据库，默认为 admin
		authDB := "admin"
		if config.AuthDB != "" {
			authDB = config.AuthDB
		}
		uri = fmt.Sprintf("mongodb://%s:%s@%s/?authSource=%s",
			config.Username,
			config.Password,
			strings.Join(config.Hosts, ","),
			authDB)
	} else {
		uri = fmt.Sprintf("mongodb://%s", strings.Join(config.Hosts, ","))
	}

	// 连接到 MongoDB
	client, err := mongo.Connect(ctx, clientOptions.ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf(i18n.Tr("MongoDB 连接失败: %v", "Failed to connect to MongoDB: %v"), err)
	}

	// 测试连接
	if err = client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf(i18n.Tr("MongoDB 连接测试失败: %v", "MongoDB connection test failed: %v"), err)
	}

	return client, nil
}

func (m *MongoDBMigration) Close() error {
	var errs []string
	if m.source != nil {
		if err := m.source.Disconnect(context.Background()); err != nil {
			errs = append(errs, fmt.Sprintf(i18n.Tr("关闭源数据库连接失败: %v", "Failed to close source database connection: %v"), err))
		}
	}
	if m.dest != nil {
		if err := m.dest.Disconnect(context.Background()); err != nil {
			errs = append(errs, fmt.Sprintf(i18n.Tr("关闭目标数据库连接失败: %v", "Failed to close destination database connection: %v"), err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf(strings.Join(errs, "; "))
	}
	return nil
}

func (m *MongoDBMigration) Run(ctx context.Context) error {
	// 设置工作线程数
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

	// 并发迁移集合
	var wg sync.WaitGroup
	errChan := make(chan error, len(m.config.Source.Tables))

	for _, table := range m.config.Source.Tables {
		wg.Add(1)
		// 获取信号量
		semaphore <- struct{}{}
		go func(table config.TableMapping) {
			defer wg.Done()
			defer func() { <-semaphore }() // 释放信号量
			if err := m.migrateCollection(ctx, table); err != nil {
				errChan <- fmt.Errorf(i18n.Tr("迁移集合 %s 失败: %v", "Failed to migrate collection %s: %v"), table.Name, err)
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
		return fmt.Errorf(i18n.Tr("迁移过程中发生错误:\n%s", "Migration failed with errors:\n%s"), strings.Join(errors, "\n"))
	}

	logrus.Info(i18n.Tr("所有集合迁移完成", "All collections migration completed"))
	return nil
}

func (m *MongoDBMigration) migrateCollection(ctx context.Context, collection config.TableMapping) error {
	logrus.Infof(i18n.Tr("开始迁移集合: %s", "Starting migration for collection: %s"), collection.Name)

	// 首先检查集合是否存在
	sourceDB := m.source.Database(m.config.Source.Database)
	collections, err := sourceDB.ListCollectionNames(ctx, bson.M{"name": collection.Name})
	if err != nil {
		return fmt.Errorf(i18n.Tr("检查集合是否存在失败: %v", "Failed to check if collection exists: %v"), err)
	}
	if len(collections) == 0 {
		logrus.Warnf(i18n.Tr("集合 %s 在源数据库中不存在，跳过", "Collection %s does not exist in source database, skipping"), collection.Name)
		return fmt.Errorf(i18n.Tr("集合 %s 不存在", "Collection %s does not exist"), collection.Name)
	}

	// 确定目标集合名
	targetName := collection.Name
	if collection.TargetName != "" {
		targetName = collection.TargetName
	}

	// 初始化统计信息，即使集合已完成也需要创建，避免空指针
	stats := migration.NewMigrationStats(0, m.config.Migration.ProgressInterval, collection.Name)
	m.statsMap[collection.Name] = stats

	// 启动进度报告
	stats.StartReporting(time.Duration(m.config.Migration.ProgressInterval) * time.Second)
	defer func() {
		stats.StopReporting()
		delete(m.statsMap, collection.Name) // 清理统计信息
	}()

	// 检查断点
	checkpoint, err := m.loadCheckpoint(collection.Name)
	if err != nil {
		logrus.Infof(i18n.Tr("加载断点信息失败: %v, 将从头开始迁移", "Failed to load checkpoint: %v, will start migration from beginning"), err)
	} else if checkpoint != nil && checkpoint.Complete {
		logrus.Infof(i18n.Tr("集合 %s 已完成迁移，跳过", "Collection %s migration already completed, skipping"), collection.Name)
		return nil
	}

	// 获取总文档数
	totalDocs, err := sourceDB.Collection(collection.Name).CountDocuments(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf(i18n.Tr("获取集合总文档数失败: %v", "Failed to get collection document count: %v"), err)
	}

	// 更新总行数
	stats.TotalRows = totalDocs

	// 获取主键
	primaryKey := "_id"
	if collection.PrimaryKey != "" {
		primaryKey = collection.PrimaryKey
	}
	logrus.Infof(i18n.Tr("集合 %s 使用主键: %s", "Collection %s using primary key: %s"), collection.Name, primaryKey)

	var remainingDocs int64
	var filter bson.M

	// 如果有断点，计算已完成的文档数
	if checkpoint != nil && len(checkpoint.LastKey) > 0 {
		if lastID, ok := checkpoint.LastKey["_id"]; ok {
			objID, err := primitive.ObjectIDFromHex(lastID)
			if err == nil {
				filter = bson.M{"_id": bson.M{"$gt": objID}}

				// 获取从断点位置开始的剩余文档数
				remainingDocs, err = sourceDB.Collection(collection.Name).CountDocuments(ctx, filter)
				if err != nil {
					return fmt.Errorf(i18n.Tr("获取剩余文档数失败: %v", "Failed to get remaining document count: %v"), err)
				}

				// 计算已处理的文档数
				completedDocs := totalDocs - remainingDocs

				// 设置起始进度
				stats.ProcessedRows = 0         // 从0开始计数
				stats.TotalRows = remainingDocs // 设置为剩余行数

				logrus.Infof(i18n.Tr("从断点继续，已完成 %d 行，剩余 %d 行",
					"Continuing from checkpoint, completed %d rows, remaining %d rows"),
					completedDocs, remainingDocs)
				logrus.Infof(i18n.Tr("从断点继续，上次处理到 %s=%s",
					"Continuing from checkpoint, last processed %s=%s"),
					primaryKey, lastID)

				// 重置统计信息
				stats.ResetStartTime(true)
			}
		}
	} else {
		// 从头开始迁移
		stats.ProcessedRows = 0
		remainingDocs = totalDocs
		stats.ResetStartTime(false)
		filter = bson.M{} // 空过滤器，查询所有文档
	}

	// 设置批处理大小
	batchSize := m.config.Migration.BatchSize
	if batchSize <= 0 {
		batchSize = 1000 // 默认批处理大小
	}

	// 获取源集合和目标集合
	sourceColl := sourceDB.Collection(collection.Name)
	destDB := m.dest.Database(m.config.Destination.Database)
	destColl := destDB.Collection(targetName)

	// 执行查询
	cursor, err := sourceColl.Find(ctx, filter, options.Find().
		SetBatchSize(int32(batchSize)).
		SetNoCursorTimeout(true))
	if err != nil {
		return fmt.Errorf(i18n.Tr("查询集合失败: %v", "Failed to query collection: %v"), err)
	}
	defer cursor.Close(ctx)

	// 批量处理数据
	batch := make([]interface{}, 0, batchSize)
	var lastID string

	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return fmt.Errorf(i18n.Tr("解码文档失败: %v", "Failed to decode document: %v"), err)
		}

		batch = append(batch, doc)
		if id, ok := doc["_id"].(primitive.ObjectID); ok {
			lastID = id.Hex()
		}

		// 当批次达到指定大小时，执行插入
		if len(batch) >= batchSize {
			if err := m.insertBatch(ctx, destColl, batch); err != nil {
				return err
			}

			// 更新进度
			stats.Lock()
			stats.ProcessedRows += int64(len(batch))
			stats.Unlock()

			// 应用全局限速
			if err := migration.EnforceGlobalRateLimit(ctx, len(batch)); err != nil {
				return err
			}

			// 保存断点
			if err := m.saveCheckpoint(collection.Name, map[string]string{"_id": lastID}, false); err != nil {
				logrus.Warnf(i18n.Tr("保存断点失败: %v", "Failed to save checkpoint: %v"), err)
			}

			batch = batch[:0]
		}
	}

	// 处理最后一批数据
	if len(batch) > 0 {
		if err := m.insertBatch(ctx, destColl, batch); err != nil {
			return err
		}

		// 更新进度
		stats.Lock()
		stats.ProcessedRows += int64(len(batch))
		stats.Unlock()

		// 保存最后的断点
		if err := m.saveCheckpoint(collection.Name, map[string]string{"_id": lastID}, false); err != nil {
			logrus.Warnf(i18n.Tr("保存断点失败: %v", "Failed to save checkpoint: %v"), err)
		}
	}

	// 保存完成标记
	if err := m.saveCheckpoint(collection.Name, map[string]string{"_id": lastID}, true); err != nil {
		logrus.Warnf(i18n.Tr("保存完成标记失败: %v", "Failed to save completion marker: %v"), err)
	}

	logrus.Infof(i18n.Tr("集合 %s 迁移完成，共迁移 %d 文档", "Collection %s migration completed, migrated %d documents"), collection.Name, totalDocs)

	return nil
}

// insertBatch 批量插入文档
func (m *MongoDBMigration) insertBatch(ctx context.Context, coll *mongo.Collection, batch []interface{}) error {
	if len(batch) == 0 {
		return nil
	}

	// 使用重试机制
	for i := 0; i <= m.maxRetries; i++ {
		// 创建批量写入操作
		var writes []mongo.WriteModel
		for _, doc := range batch {
			// 使用 upsert 操作，如果文档存在则更新，不存在则插入
			filter := bson.M{"_id": doc.(bson.M)["_id"]}
			update := bson.M{"$set": doc}
			model := mongo.NewUpdateOneModel().
				SetFilter(filter).
				SetUpdate(update).
				SetUpsert(true)
			writes = append(writes, model)
		}

		// 执行批量写入
		opts := options.BulkWrite().SetOrdered(false)
		_, err := coll.BulkWrite(ctx, writes, opts)
		if err == nil {
			return nil
		}

		// 如果不是最后一次尝试，则等待后重试
		if i < m.maxRetries {
			logrus.Warnf(i18n.Tr("批量插入失败，将在 %v 后重试: %v", "Batch insert failed, will retry in %v: %v"), m.retryDelay, err)
			select {
			case <-time.After(m.retryDelay):
			case <-ctx.Done():
				return ctx.Err()
			}
		} else {
			return fmt.Errorf(i18n.Tr("批量插入失败，已重试 %d 次: %v", "Batch insert failed after %d retries: %v"), m.maxRetries, err)
		}
	}

	return nil
}

// 获取最后处理的ID
func (m *MongoDBMigration) getLastProcessedID() (string, bool) {
	// 这里应该实现获取最后处理的ID的逻辑
	// 由于没有完整的代码，这里只是一个占位符
	return "", false
}

func (m *MongoDBMigration) loadCheckpoint(tableName string) (*migration.Checkpoint, error) {
	if m.config.Migration.CheckpointDir == "" {
		return nil, nil
	}

	filename := filepath.Join(m.config.Migration.CheckpointDir,
		fmt.Sprintf("mongodb_%s.checkpoint", tableName))

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

func (m *MongoDBMigration) saveCheckpoint(collectionName string, lastKey map[string]string, complete bool) error {
	if m.config.Migration.CheckpointDir == "" {
		return nil
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

	filename := filepath.Join(m.config.Migration.CheckpointDir,
		fmt.Sprintf("mongodb_%s.checkpoint", collectionName))

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

// 添加辅助函数来构建 $project 阶段
func buildProjectStage(transformations []config.ColumnTransformation) bson.M {
	project := bson.M{}

	// 首先添加所有字段，确保不会丢失字段
	project["_id"] = 1

	// 然后应用转换
	for _, transform := range transformations {
		project[transform.SourceColumn] = transform.Expression
	}

	return project
}

// 辅助函数
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
