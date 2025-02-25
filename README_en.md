# Database Migration Tool

## Overview

This tool is designed for data migration between different database systems, supporting MySQL, PostgreSQL, MongoDB, and Cassandra. It provides efficient data transfer capabilities with features like checkpoint resumption, rate limiting, and concurrent processing.

## Features

- **Multiple Database Support**: MySQL, PostgreSQL, MongoDB, Cassandra
- **Checkpoint Resumption**: Continue migration from the last position after interruption
- **Concurrency Control**: Configurable number of concurrent worker threads
- **Rate Limiting**: Prevent excessive pressure on source databases during migration
- **Real-time Progress Reporting**: Periodic display of migration progress and rate
- **Primary Key Detection**: Automatically detect and use table primary keys for checkpoints

## Installation 

## Configuration File

The configuration file uses YAML format and includes settings for source database, destination database, and migration parameters.

### Example Configuration (config.yaml) 

```yaml
source:
  type: mysql  # Database type: mysql, postgresql, mongodb, cassandra
  hosts:
    - "localhost:3306"  # Database host address
  database: "source_db"  # Database name
  schema: "public"  # Schema for PostgreSQL
  username: "root"
  password: "password"
  tables:
    - name: "users"
      target_name: "users_new"  # Optional, specify target table name
    - name: "orders"
    - name: "products"

destination:
  type: mysql
  hosts:
    - "localhost:3307"
  database: "dest_db"
  username: "root"
  password: "password"

migration:
  batch_size: 1000        # Number of records per batch
  workers: 4              # Number of concurrent worker threads
  rate_limit: 10000       # Rate limit (records per second)
  timeout: 30             # Connection timeout (seconds)
  checkpoint_dir: "./data/checkpoints"  # Directory for checkpoint files
  log_file: "./logs/migration.log"  # Log file path
  log_level: "info"       # Log level: debug, info, warn, error
  progress_interval: 10   # Progress report interval (seconds)
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
