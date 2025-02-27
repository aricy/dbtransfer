# 一个用于在相同类型数据库系统之间迁移数据的工具。

## 功能特点

- 支持的数据库类型：
  - MongoDB
  - MySQL
  - PostgreSQL
  - Cassandra

支持在相同类型数据库间进行数据迁移：
  - MongoDB -> MongoDB
  - MySQL -> MySQL
  - PostgreSQL -> PostgreSQL
  - Cassandra -> Cassandra
  - Cassandra -> ScyllaDB
  - ScyllaDB -> Cassandra
  - ScyllaDB -> ScyllaDB
  - Cassandra/ScyllaDB 互相迁移（ScyllaDB 兼容 Cassandra 协议）

特性：
- 支持断点续传
- 支持并发迁移
- 支持限速控制
- 支持进度报告
- 支持列数据转换
- 支持国际化

## 安装

### 从源码构建

项目使用 Make 来管理构建过程。以下是可用的构建命令：

```bash
# 构建当前平台的二进制文件
make build

# 构建特定平台的二进制文件
make linux-amd64    # 构建 Linux AMD64 版本
make linux-arm64    # 构建 Linux ARM64 版本
make darwin-amd64   # 构建 macOS AMD64 版本
make darwin-arm64   # 构建 macOS ARM64 版本

# 构建所有平台的二进制文件
make build-all

# 其他常用命令
make clean          # 清理构建文件
make test          # 运行测试
make fmt           # 格式化代码
make vet           # 运行 go vet
make tidy          # 更新依赖
make vendor        # 下载依赖到 vendor 目录
```

构建后的二进制文件将位于 `build` 目录下。

## 配置

配置文件示例 (config.yaml):

```yaml
source:
  type: cassandra  # 数据库类型：cassandra（同样支持 ScyllaDB）
  hosts:
    - 127.0.0.1:9042
  keyspace: source_keyspace
  username: cassandra
  password: cassandra
  tables:
    - name: users
      target_name: users_new
    - name: orders
      column_transformations:
        - source_column: price
          expression: "price * 100"  # 转换价格单位
        - source_column: status
          expression: "UPPER(status)"  # 将状态转为大写

destination:
  type: cassandra  # ScyllaDB 使用相同的类型
  hosts:
    - 127.0.0.1:9042
  keyspace: target_keyspace
  username: cassandra
  password: cassandra

migration:
  workers: 4
  batch_size: 1000
  rate_limit: 5000
  timeout: 30
  checkpoint_dir: ./checkpoints
  log_file: ./logs/migration.log
  log_level: info
  progress_interval: 5
  language: zh  # 可选: zh (中文) 或 en (英文)，不设置则根据系统环境自动选择
  checkpoint_row_threshold: 1000  # 每处理多少行刷新一次断点
  checkpoint_interval: 1  # 断点刷新时间间隔（秒）
```

## 使用方法

```bash
dbtransfer -config config.yaml
```

命令行参数:

- `-config`: 配置文件路径 (默认: config.yaml)
- `-verbose`: 显示详细日志
- `-version`: 显示版本信息
- `-language`: 指定界面语言 (zh: 中文, en: 英文)

## 支持的数据库类型

- MySQL
- PostgreSQL
- MongoDB
- Cassandra

## 列数据转换

您可以在迁移过程中对列数据进行转换，通过在配置文件中指定 `column_transformations` 来实现：

```yaml
column_transformations:
  - source_column: price  # 源列名
    expression: "price * 100"  # 转换表达式
  - source_column: create_time
    expression: "DATE_FORMAT(create_time, '%Y-%m-%d')"  # 日期格式转换
```

表达式使用各数据库原生的函数和运算符，会在查询源数据时应用。这意味着转换直接在数据库层面执行，提高了效率并减少了数据传输量。

### 支持的表达式类型

- **MySQL**: 支持所有 MySQL 函数和表达式，如 `UPPER()`, `CONCAT()`, `DATE_FORMAT()` 等
- **PostgreSQL**: 支持所有 PostgreSQL 函数和表达式，如 `upper()`, `concat()`, `to_char()` 等
- **MongoDB**: 支持 MongoDB 聚合表达式，会自动转换常见函数如 `UPPER()` → `$toUpper`, `LOWER()` → `$toLower` 等
- **Cassandra**: 支持 CQL 支持的函数，如 `UPPER()`, `LOWER()`, 数学运算等

这种设计允许您充分利用各数据库的原生功能和优化能力。

## 国际化支持

工具支持中文和英文两种界面语言，可以通过以下方式指定：

1. 配置文件中设置 `migration.language: "zh"` 或 `migration.language: "en"`
2. 命令行参数 `-language zh` 或 `-language en`
3. 不指定语言时，会根据系统环境自动选择合适的语言

## 断点续传

工具支持断点续传，会在 `checkpoint_dir` 指定的目录中保存迁移进度。如果迁移过程中断，再次启动时会从上次中断的位置继续。

## 许可证

MIT

## 概述

本工具用于在不同数据库系统之间迁移数据，支持 MySQL、PostgreSQL、MongoDB 和 Cassandra。它提供了高效的数据传输能力，支持断点续传、限速和并发控制。

## 功能特点

- **多数据库支持**：MySQL、PostgreSQL、MongoDB、Cassandra
- **断点续传**：支持中断后从上次位置继续迁移
- **并发控制**：可配置并发工作线程数量
- **限速功能**：防止迁移过程对源数据库造成过大压力
- **实时进度报告**：定期显示迁移进度和速率
- **主键检测**：自动检测并使用表的主键作为断点标识
- **列转换**：支持在迁移过程中对列数据进行转换处理

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

