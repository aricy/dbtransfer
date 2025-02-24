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
	"dbtransfer/internal/migration"
)

type MongoDBMigration struct {
	source     *mongo.Client
	dest       *mongo.Client
	config     *config.Config
	limiter    *rate.Limiter
	stats      *migration.MigrationStats
	maxRetries int
	retryDelay time.Duration
	lastID     interface{} // 记录最后处理的ID
}

func NewMigration(config *config.Config) (migration.Migration, error) {
	// 初始化日志
	if err := migration.InitLogger(config.Migration.LogFile, config.Migration.LogLevel); err != nil {
		return nil, fmt.Errorf("初始化日志失败: %v", err)
	}

	ctx := context.Background()

	// 连接源数据库
	sourceClient, err := connectMongoDB(ctx, config.Source)
	if err != nil {
		return nil, fmt.Errorf("连接源数据库失败: %v", err)
	}

	// 连接目标数据库
	destClient, err := connectMongoDB(ctx, config.Destination)
	if err != nil {
		sourceClient.Disconnect(ctx)
		return nil, fmt.Errorf("连接目标数据库失败: %v", err)
	}

	// 创建限流器
	var limiter *rate.Limiter
	if config.Migration.RateLimit > 0 {
		burst := max(config.Migration.BatchSize, config.Migration.RateLimit)
		limiter = rate.NewLimiter(rate.Limit(config.Migration.RateLimit), burst)
	}

	return &MongoDBMigration{
		source:     sourceClient,
		dest:       destClient,
		config:     config,
		limiter:    limiter,
		stats:      &migration.MigrationStats{StartTime: time.Now()},
		maxRetries: 3,
		retryDelay: time.Second * 5,
	}, nil
}

func connectMongoDB(ctx context.Context, config config.DBConfig) (*mongo.Client, error) {
	// 检查配置
	if len(config.Hosts) == 0 {
		return nil, fmt.Errorf("未配置数据库主机地址")
	}
	if config.Database == "" {
		return nil, fmt.Errorf("未配置数据库名")
	}

	uri := fmt.Sprintf("mongodb://%s:%s@%s/%s?authSource=admin",
		config.Username,
		config.Password,
		strings.Join(config.Hosts, ","),
		config.Database)

	logrus.Infof("正在连接 MongoDB: %s", strings.Replace(uri, config.Password, "****", 1))

	opts := options.Client().
		ApplyURI(uri).
		SetAuth(options.Credential{
			Username:   config.Username,
			Password:   config.Password,
			AuthSource: "admin",
		})

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		logrus.Errorf("连接 MongoDB 失败: %v", err)
		return nil, err
	}

	// 设置连接超时
	ctxTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err = client.Ping(ctxTimeout, nil); err != nil {
		logrus.Errorf("Ping MongoDB 失败: %v", err)
		client.Disconnect(ctx)
		return nil, err
	}

	logrus.Info("MongoDB 连接成功")
	return client, nil
}

func (m *MongoDBMigration) Close() {
	ctx := context.Background()
	if m.source != nil {
		m.source.Disconnect(ctx)
	}
	if m.dest != nil {
		m.dest.Disconnect(ctx)
	}
}

func (m *MongoDBMigration) Run(ctx context.Context) error {
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

	// 并发迁移集合
	for _, table := range m.config.Source.Tables {
		wg.Add(1)
		// 获取信号量
		semaphore <- struct{}{}
		go func(table config.TableMapping) {
			defer wg.Done()
			defer func() { <-semaphore }() // 释放信号量
			if err := m.migrateCollection(ctx, table); err != nil {
				errChan <- fmt.Errorf("迁移集合 %s 失败: %v", table.Name, err)
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

func (m *MongoDBMigration) migrateCollection(ctx context.Context, table config.TableMapping) error {
	// 检查数据库连接
	if m.source == nil || m.dest == nil {
		return fmt.Errorf("数据库连接未初始化")
	}

	logrus.Infof("开始迁移集合: %s", table.Name)

	// 获取源集合
	sourceDB := m.source.Database(m.config.Source.Database)
	if sourceDB == nil {
		return fmt.Errorf("源数据库 %s 不存在", m.config.Source.Database)
	}

	sourceColl := sourceDB.Collection(table.Name)
	if sourceColl == nil {
		return fmt.Errorf("源集合 %s 不存在", table.Name)
	}

	// 检查集合是否存在，使用 bson.D
	filter := bson.D{{"name", table.Name}}
	collections, err := sourceDB.ListCollections(ctx, filter)
	if err != nil {
		return fmt.Errorf("检查集合是否存在失败: %v", err)
	}

	exists := false
	for collections.Next(ctx) {
		exists = true
		break
	}
	if !exists {
		logrus.Infof("集合 %s 在源数据库中不存在，跳过", table.Name)
		return fmt.Errorf("集合 %s 不存在", table.Name)
	}

	// 检查断点
	checkpoint, err := m.loadCheckpoint(table.Name)
	if err != nil {
		logrus.Infof("加载断点信息失败: %v, 将从头开始迁移", err)
	} else if checkpoint != nil && checkpoint.Complete {
		logrus.Infof("集合 %s 已完成迁移，跳过", table.Name)
		return nil
	}

	// 确定目标集合名
	targetName := table.Name
	if table.TargetName != "" {
		targetName = table.TargetName
	}

	// 获取目标集合
	destDB := m.dest.Database(m.config.Destination.Database)
	destColl := destDB.Collection(targetName)

	// 检查断点并构建查询条件
	filter = bson.D{}
	if checkpoint != nil && checkpoint.LastKey != nil {
		if id, ok := checkpoint.LastKey["_id"]; ok {
			// 尝试解析 ObjectId
			if strings.HasPrefix(id, "ObjectId('") && strings.HasSuffix(id, "')") {
				// 提取 ObjectId 的实际值
				idStr := strings.TrimPrefix(strings.TrimSuffix(id, "')"), "ObjectId('")
				objID, err := primitive.ObjectIDFromHex(idStr)
				if err != nil {
					return fmt.Errorf("解析 ObjectId 失败: %v", err)
				}
				filter = bson.D{{"_id", bson.D{{"$gt", objID}}}}
				logrus.Infof("从断点位置继续: %v", id)
			} else {
				// 如果不是 ObjectId 格式，可能是其他格式的 ID
				filter = bson.D{{"_id", bson.D{{"$gt", id}}}}
				logrus.Infof("从断点位置继续: %v", id)
			}
		}
	}

	// 获取总文档数
	totalCount, err := sourceColl.CountDocuments(ctx, filter)
	if err != nil {
		return fmt.Errorf("获取总文档数失败: %v", err)
	}
	m.stats.TotalRows = totalCount

	logrus.Infof("剩余文档数: %d", totalCount)

	// 只在真正完成时退出
	if totalCount == 0 && checkpoint != nil && checkpoint.Complete {
		logrus.Infof("集合 %s 已完成迁移，跳过", table.Name)
		return nil
	}

	// 设置查询选项
	findOptions := options.Find().
		SetBatchSize(int32(m.config.Migration.BatchSize)).
		SetSort(bson.D{{"_id", 1}})

	// 开始迁移数据
	cursor, err := sourceColl.Find(ctx, filter, findOptions)
	if err != nil {
		return fmt.Errorf("查询数据失败: %v", err)
	}
	defer cursor.Close(ctx)

	// 辅助函数：将 ID 转换为字符串
	var getIDString = func(id interface{}) string {
		switch v := id.(type) {
		case primitive.ObjectID:
			return fmt.Sprintf("ObjectId('%s')", v.Hex())
		default:
			return fmt.Sprint(v)
		}
	}

	// 设置定期保存断点
	checkpointDelay := time.Duration(m.config.Migration.CheckpointDelay)
	if checkpointDelay == 0 {
		checkpointDelay = 60 // 默认60秒
	}
	checkpointTicker := time.NewTicker(time.Second * checkpointDelay)
	defer checkpointTicker.Stop()

	var mu sync.Mutex
	var lastProcessedID interface{}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-checkpointTicker.C:
				mu.Lock()
				if lastProcessedID != nil {
					lastKey := map[string]string{"_id": getIDString(lastProcessedID)}
					if err := m.saveCheckpoint(table.Name, lastKey, false); err != nil {
						logrus.Errorf("保存断点失败: %v", err)
					} else {
						logrus.Infof("保存断点成功，位置: map[_id:%s]", getIDString(lastProcessedID))
					}
				}
				mu.Unlock()
			}
		}
	}()

	// 创建令牌桶，用于批量获取令牌
	var limiter *rate.Limiter
	if m.config.Migration.RateLimit > 0 {
		burst := max(m.config.Migration.BatchSize, m.config.Migration.RateLimit)
		limiter = rate.NewLimiter(rate.Limit(m.config.Migration.RateLimit), burst)
	}

	batch := make([]interface{}, 0, m.config.Migration.BatchSize)
	count := 0

	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			return fmt.Errorf("解码文档失败: %v", err)
		}

		// 更新最后处理的ID
		for _, elem := range doc {
			if elem.Key == "_id" {
				mu.Lock()
				lastProcessedID = elem.Value
				mu.Unlock()
				break
			}
		}

		batch = append(batch, doc)
		count++

		if count >= m.config.Migration.BatchSize {
			// 执行批量插入前进行限流
			if limiter != nil {
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

			if err := m.insertBatch(ctx, destColl, batch); err != nil {
				return err
			}
			m.stats.Increment(count)
			batch = batch[:0]
			count = 0
		}
	}

	// 处理最后一批数据
	if len(batch) > 0 {
		// 执行批量插入前进行限流
		if limiter != nil {
			reservation := limiter.ReserveN(time.Now(), len(batch))
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

		if err := m.insertBatch(ctx, destColl, batch); err != nil {
			return err
		}
		m.stats.Increment(len(batch))
	}

	// 在断点保存协程中
	if lastProcessedID != nil {
		lastKey := map[string]string{"_id": getIDString(lastProcessedID)}
		if err := m.saveCheckpoint(table.Name, lastKey, false); err != nil {
			logrus.Errorf("保存断点失败: %v", err)
		} else {
			logrus.Infof("保存断点成功，位置: map[_id:%s]", getIDString(lastProcessedID))
		}
	}

	// 在完成时
	if lastProcessedID != nil {
		lastKey := map[string]string{"_id": getIDString(lastProcessedID)}
		if err := m.saveCheckpoint(table.Name, lastKey, true); err != nil {
			logrus.Errorf("保存完成标记失败: %v", err)
		} else {
			logrus.Infof("集合 %s 迁移完成，共迁移 %d 文档", table.Name, m.stats.ProcessedRows)
		}
	}

	return nil
}

func (m *MongoDBMigration) insertBatch(ctx context.Context, coll *mongo.Collection, batch []interface{}) error {
	if m.limiter != nil {
		reservation := m.limiter.ReserveN(time.Now(), len(batch))
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

	// 将批量插入改为批量 upsert
	operations := make([]mongo.WriteModel, len(batch))
	for i, doc := range batch {
		bsonDoc := doc.(bson.D)
		var filter bson.D
		// 提取 _id 作为过滤条件
		for _, elem := range bsonDoc {
			if elem.Key == "_id" {
				filter = bson.D{{"_id", elem.Value}}
				break
			}
		}
		// 创建 upsert 操作
		operations[i] = mongo.NewReplaceOneModel().
			SetFilter(filter).
			SetReplacement(doc).
			SetUpsert(true)
	}

	opts := options.BulkWrite().SetOrdered(false)
	for i := 0; i < m.maxRetries; i++ {
		_, err := coll.BulkWrite(ctx, operations, opts)
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
		return nil, err
	}

	var checkpoint migration.Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, err
	}

	return &checkpoint, nil
}

func (m *MongoDBMigration) saveCheckpoint(tableName string, lastKey map[string]string, completed bool) error {
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
		fmt.Sprintf("mongodb_%s.checkpoint", tableName))

	return os.WriteFile(filename, data, 0644)
}

// 辅助函数
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
