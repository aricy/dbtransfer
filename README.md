# 数据库迁移工具

## 概述

本工具用于在不同数据库系统之间迁移数据，支持 MySQL、PostgreSQL、MongoDB 和 Cassandra。它提供了高效的数据传输能力，支持断点续传、限速和并发控制。

## 功能特点

- **多数据库支持**：MySQL、PostgreSQL、MongoDB、Cassandra
- **断点续传**：支持中断后从上次位置继续迁移
- **并发控制**：可配置并发工作线程数量
- **限速功能**：防止迁移过程对源数据库造成过大压力
- **实时进度报告**：定期显示迁移进度和速率
- **主键检测**：自动检测并使用表的主键作为断点标识

## 安装

## 使用方法

```bash
# 使用默认配置文件 (configs/config.yaml)
./dbtransfer

# 指定配置文件
./dbtransfer -config /path/to/config.yaml

# 指定迁移类型
./dbtransfer -type mysql

# 生成配置文件模板
./dbtransfer -generate-template -type mysql -template-output ./configs/mysql_config.yaml
```

## 迁移过程

1. **连接数据库**：工具首先连接源数据库和目标数据库
2. **表结构迁移**：对于关系型数据库，会先创建目标表结构
3. **数据迁移**：按批次读取源数据并写入目标数据库
4. **断点保存**：每秒保存当前迁移进度，支持断点续传
5. **进度报告**：定期显示迁移进度和速率

## 注意事项

1. **主键要求**：所有要迁移的表必须有主键，否则无法进行迁移
2. **数据类型兼容性**：不同数据库系统之间的数据类型可能不完全兼容，请提前检查
3. **资源消耗**：大数据量迁移可能消耗较多资源，请合理设置批次大小和限速
4. **权限要求**：需要源数据库的读权限和目标数据库的写权限
5. **同构迁移**：当前版本主要支持同类型数据库之间的迁移（如MySQL到MySQL），异构数据库迁移可能需要额外配置

## 故障排除

### 常见错误

1. **连接失败**：检查数据库地址、用户名和密码是否正确
2. **表不存在**：确认源数据库中表名是否正确
3. **无主键错误**：确保所有要迁移的表都有主键
4. **权限不足**：检查数据库用户是否有足够权限

### 日志分析

日志文件默认保存在 `./logs/migration.log`，包含详细的迁移过程信息，可用于排查问题。

## 性能优化

1. **调整批次大小**：`batch_size` 参数影响内存使用和效率
2. **调整并发数**：`workers` 参数控制并发线程数
3. **限速设置**：`rate_limit` 参数可防止对源数据库造成过大压力

## 示例场景

### MySQL 到 MySQL 迁移

```yaml
source:
  type: mysql
  hosts:
    - "mysql-server:3306"
  database: "source_db"
  username: "root"
  password: "mysql_password"
  tables:
    - name: "customers"
    - name: "orders"

destination:
  type: mysql
  hosts:
    - "mysql-target:3306"
  database: "target_db"
  username: "root"
  password: "mysql_password"

migration:
  batch_size: 2000
  workers: 8
  rate_limit: 5000
```

### MongoDB 到 MongoDB 迁移

```yaml
source:
  type: mongodb
  hosts:
    - "mongo-source:27017"
  database: "source_db"
  username: "admin"
  password: "mongo_password"
  tables:
    - name: "users"
    - name: "products"

destination:
  type: mongodb
  hosts:
    - "mongo-dest:27017"
  database: "target_db"
  username: "admin"
  password: "mongo_password"

migration:
  batch_size: 500
  workers: 4
  rate_limit: 2000
```

### Cassandra 到 Cassandra 迁移

```yaml
source:
  type: cassandra
  hosts:
    - "cassandra-source-1:9042"
    - "cassandra-source-2:9042"
  keyspace: "source_keyspace"
  username: "cassandra"
  password: "cassandra_password"
  tables:
    - name: "time_series_data"
    - name: "user_events"

destination:
  type: cassandra
  hosts:
    - "cassandra-dest-1:9042"
    - "cassandra-dest-2:9042"
  keyspace: "target_keyspace"
  username: "cassandra"
  password: "cassandra_password"

migration:
  batch_size: 1000
  workers: 16
  rate_limit: 8000
```

