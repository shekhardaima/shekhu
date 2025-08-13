# Data Processing Pipeline - Databricks & Delta Lake Optimized

This project has been restructured from a monolithic script into a modular, maintainable Python package for processing time-series data from Databricks Delta tables and writing it to Redis TimeSeries. **Optimized for Databricks runtime 14.3 with Delta Lake support and Databricks Connect for local development.**

## 🏗️ Project Structure

```
├── src/                          # Main source package
│   ├── __init__.py              # Package initialization
│   ├── main.py                  # Main orchestration module (Databricks optimized)
│   ├── config/                  # Configuration modules
│   │   ├── __init__.py
│   │   ├── redis_config.py      # Redis configuration settings
│   │   └── databricks_config.py # Databricks & Delta Lake configuration
│   ├── utils/                   # Utility modules
│   │   ├── __init__.py
│   │   └── logging_config.py    # Logging configuration utilities
│   ├── redis/                   # Redis-related modules
│   │   ├── __init__.py
│   │   ├── connection.py        # Redis connection management
│   │   └── timeseries_operations.py  # TimeSeries operations
│   └── data/                    # Data processing modules
│       ├── __init__.py
│       ├── spark_operations.py  # Spark DataFrame operations (Delta optimized)
│       └── processing.py        # Partition and cycle processing
├── legacy_main.py               # Backward compatibility script (Databricks ready)
├── databricks_example.py        # Databricks notebook examples
├── requirements.txt             # Project dependencies (with Databricks Connect)
├── PROJECT_STRUCTURE.md         # This documentation
├── DATABRICKS_SETUP.md          # Databricks deployment guide
└── example_usage.py             # Usage examples
```

## 📦 Modules Overview

### 1. **Configuration Module** (`src/config/`)
- **`redis_config.py`**: Centralized Redis configuration with environment variable support
- **`databricks_config.py`**: Databricks runtime 14.3 and Delta Lake optimizations
- Supports customization through environment variables and Databricks secrets
- Provides default configuration for backward compatibility
- Automatic Databricks environment detection

### 2. **Utilities Module** (`src/utils/`)
- **`logging_config.py`**: Consistent logging setup across all modules
- Configurable log levels and formats
- Prevents duplicate handlers

### 3. **Redis Module** (`src/redis/`)
- **`connection.py`**: Redis connection management with SSL support and connection pooling
- **`timeseries_operations.py`**: Redis TimeSeries operations for key creation and data writing
- Handles batch operations and error recovery
- Provides both class-based and functional interfaces

### 4. **Data Processing Module** (`src/data/`)
- **`spark_operations.py`**: Spark DataFrame operations optimized for Delta tables (no RDD usage)
- **`processing.py`**: Partition processing and cycle-based data operations
- Handles aggregation, filtering, and caching with Delta Lake optimizations
- Supports Databricks adaptive query execution and auto-compaction

### 5. **Main Orchestration** (`src/main.py`)
- **`DataPipeline`** class: High-level pipeline orchestrator for Databricks
- Combines all modules into a cohesive workflow
- Provides methods for full pipeline execution and single cycle processing
- Automatic Databricks environment detection and optimization
- Delta table connectivity testing and performance monitoring

## 🚀 Usage Examples

### Using the New Databricks-Optimized API

```python
from src.main import DataPipeline

# Create pipeline (auto-detects Databricks environment)
pipeline = DataPipeline(batch_size=5000)

# Run complete pipeline
stats = pipeline.run_pipeline()

# Or process a single cycle
cycle_stats = pipeline.run_single_cycle("cycle_123")

# Get available cycles
cycles = pipeline.get_available_cycles()
```

### Databricks Notebook Usage

```python
# Databricks notebook cell
%pip install redis>=4.5.0

# Import and run
from src.main import DataPipeline

# Initialize with Databricks optimizations
pipeline = DataPipeline(batch_size=10000)  # Larger batch for cluster

# Run pipeline
stats = pipeline.run_pipeline()

# Display results
display(spark.createDataFrame(stats))
```

### Using Individual Modules

```python
from src.data.spark_operations import SparkDataProcessor
from src.redis.connection import RedisConnectionManager
from src.config.redis_config import get_redis_config
from src.config.databricks_config import DatabricksSparkManager

# Databricks Spark operations
spark_processor = SparkDataProcessor()  # Auto-creates Databricks session
df = spark_processor.get_calculations_dataframe()
cycles = spark_processor.get_unique_cycles(df)

# Test Delta connectivity
spark_manager = DatabricksSparkManager()
success = spark_manager.test_delta_connectivity()

# Redis operations
redis_config = get_redis_config()
redis_manager = RedisConnectionManager(redis_config)
client = redis_manager.get_client()
```

### Legacy Compatibility (Databricks Ready)

For backward compatibility, use the legacy script (now optimized for Databricks):

```python
# Run the original script interface with Databricks optimizations
python legacy_main.py
```

### Databricks Connect (Local Development)

```bash
# Set up environment variables
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token"
export DATABRICKS_CLUSTER_ID="your-cluster-id"

# Test connection
databricks-connect test

# Run locally with Databricks Connect
python src/main.py
```

## ⚙️ Configuration

### Environment Variables

Configure for Databricks and Redis using environment variables:

```bash
# Databricks Connect (for local development)
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-personal-access-token"
export DATABRICKS_CLUSTER_ID="your-cluster-id"

# Redis configuration
export REDIS_HOST="your-redis-host.redis.azure.net"
export REDIS_PORT="10000"
export REDIS_PASSWORD="your-password"
export REDIS_SSL="true"
export REDIS_MAX_CONNECTIONS="50"
export REDIS_SOCKET_TIMEOUT="30"

# TimeSeries configuration
export REDIS_TS_RETENTION="2592000000"  # 30 days in milliseconds
export REDIS_TS_CHUNK_SIZE="4096"
export REDIS_TS_DUPLICATE_POLICY="LAST"

# Spark optimizations
export SPARK_CONF_spark_sql_adaptive_enabled="true"
export SPARK_CONF_spark_databricks_delta_optimizeWrite_enabled="true"
```

### Programmatic Configuration

```python
from src.config.redis_config import get_redis_config, get_timeseries_config

# Get configurations
redis_config = get_redis_config()
ts_config = get_timeseries_config()

# Custom configuration
custom_config = {
    "host": "custom-host",
    "port": 6379,
    "password": "custom-password",
    # ... other settings
}
```

## 📊 Features

### ✅ Improvements Over Original Code

1. **Modular Architecture**: Separated concerns into focused modules
2. **Configuration Management**: Centralized and environment-aware configuration
3. **Error Handling**: Better error handling and logging throughout
4. **Testability**: Each module can be tested independently
5. **Reusability**: Components can be reused in different contexts
6. **Maintainability**: Easier to understand, modify, and extend
7. **Documentation**: Comprehensive docstrings and type hints

### 🔧 Key Components

- **Connection Pooling**: Efficient Redis connection management
- **Batch Processing**: Optimized batch operations for better performance
- **Error Recovery**: Automatic key creation and retry logic
- **Statistics Tracking**: Detailed processing statistics and monitoring
- **Caching**: DataFrame caching for improved Spark performance
- **Logging**: Consistent logging across all modules

## 🧪 Development

### Installation

```bash
# Install dependencies
pip install -r requirements.txt

# For development
pip install -r requirements.txt[dev]
```

### Running Tests

```bash
# Run tests (when implemented)
pytest tests/

# Run with coverage
pytest --cov=src tests/
```

### Code Quality

```bash
# Format code
black src/

# Lint code
flake8 src/

# Type checking
mypy src/
```

## 🔄 Migration Guide

### From Monolithic to Modular

1. **Replace direct imports**: Update imports to use the new module structure
2. **Use DataPipeline class**: Replace direct function calls with the pipeline orchestrator
3. **Update configuration**: Move hardcoded values to environment variables
4. **Leverage new features**: Use connection management and improved error handling

### Example Migration

**Before (Monolithic)**:
```python
from original_script import main
main()
```

**After (Modular)**:
```python
from src.main import DataPipeline
pipeline = DataPipeline()
pipeline.run_pipeline()
```

## 🤝 Contributing

1. Follow the modular structure when adding new features
2. Add comprehensive docstrings and type hints
3. Update tests for new functionality
4. Maintain backward compatibility when possible
5. Update documentation for new features

## 📝 License

[Your License Here]

---

This modular architecture provides a solid foundation for scaling and maintaining your data processing pipeline while preserving the original functionality.