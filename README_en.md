# A tool for migrating data between databases of the same type.

## Overview

This tool is designed for data migration between different database systems, supporting MySQL, PostgreSQL, MongoDB, and Cassandra. It provides efficient data transfer capabilities with features like checkpoint resumption, rate limiting, and concurrent processing.

## Features

- Supported database types:
  - MongoDB
  - MySQL
  - PostgreSQL
  - Cassandra

Supports data migration between databases of the same type:
  - MongoDB -> MongoDB
  - MySQL -> MySQL
  - PostgreSQL -> PostgreSQL
  - Cassandra -> Cassandra
  - Cassandra/ScyllaDB bidirectional migration (ScyllaDB is compatible with Cassandra protocol)

Features:
- Checkpoint resumption
- Concurrent migration
- Rate limiting
- Progress reporting
- Column data transformation
- Internationalization

## Installation

### Building from Source

The project uses Make to manage the build process. Available build commands:

```bash
# Build binary for current platform
make build

# Build for specific platforms
make linux-amd64    # Build for Linux AMD64
make linux-arm64    # Build for Linux ARM64
make darwin-amd64   # Build for macOS AMD64
make darwin-arm64   # Build for macOS ARM64

# Build for all platforms
make build-all

# Other useful commands
make clean          # Clean build files
make test          # Run tests
make fmt           # Format code
make vet           # Run go vet
make tidy          # Update dependencies
make vendor        # Download dependencies to vendor directory
```

Built binaries will be placed in the `build` directory.

## Configuration

Example configuration (config.yaml):

```yaml
source:
  type: cassandra  # Database type: cassandra (also supports ScyllaDB)
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
          expression: "price * 100"  # Convert price unit
        - source_column: status
          expression: "UPPER(status)"  # Convert status to uppercase

destination:
  type: cassandra  # ScyllaDB uses the same type
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
  language: en  # Optional: zh (Chinese) or en (English), auto-detect if not set
```

## Usage

```bash
# Use default configuration file (configs/config.yaml)
./dbtransfer

# Specify configuration file
./dbtransfer -config /path/to/config.yaml

# Specify migration type
./dbtransfer -type mysql

# Generate configuration template
./dbtransfer -generate-template -type mysql -template-output ./configs/mysql_config.yaml
```

## Migration Process

1. **Database Connection**: The tool first connects to source and destination databases
2. **Table Structure Migration**: For relational databases, it creates the target table structure first
3. **Data Migration**: Reads source data in batches and writes to the destination database
4. **Checkpoint Saving**: Saves migration progress every second, supporting checkpoint resumption
5. **Progress Reporting**: Periodically displays migration progress and rate

## Important Notes

1. **Primary Key Requirement**: All tables to be migrated must have a primary key
2. **Data Type Compatibility**: Data types between different database systems may not be fully compatible
3. **Resource Consumption**: Large data migrations may consume significant resources
4. **Permission Requirements**: Requires read permissions on source database and write permissions on destination database
5. **Homogeneous Migration**: The current version primarily supports migration between databases of the same type (e.g., MySQL to MySQL). Heterogeneous database migration may require additional configuration

## Troubleshooting

### Common Errors

1. **Connection Failure**: Check if database address, username, and password are correct
2. **Table Not Found**: Confirm if table names in source database are correct
3. **No Primary Key Error**: Ensure all tables to be migrated have primary keys
4. **Insufficient Permissions**: Check if database user has sufficient permissions

### Log Analysis

Log files are saved in `./logs/migration.log` by default and contain detailed information about the migration process.

## Performance Optimization

1. **Adjust Batch Size**: The `batch_size` parameter affects memory usage and efficiency
2. **Adjust Concurrency**: The `workers` parameter controls the number of concurrent threads
3. **Rate Limiting**: The `rate_limit` parameter prevents excessive pressure on source databases

## Example Scenarios

### MySQL to MySQL Migration

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

### MongoDB to MongoDB Migration

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

### Cassandra to Cassandra Migration

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

## Contribution and Support

For issues or suggestions, please submit an Issue or Pull Request.

## License

MIT
