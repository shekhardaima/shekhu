# Data Processing Pipeline - Modular Architecture

This project has been restructured from a monolithic script into a modular, maintainable Python package for processing time-series data from Databricks and writing it to Redis TimeSeries.

## 🏗️ Project Structure

```
├── src/                          # Main source package
│   ├── __init__.py              # Package initialization
│   ├── main.py                  # Main orchestration module
│   ├── config/                  # Configuration modules
│   │   ├── __init__.py
│   │   └── redis_config.py      # Redis configuration settings
│   ├── utils/                   # Utility modules
│   │   ├── __init__.py
│   │   └── logging_config.py    # Logging configuration utilities
│   ├── redis/                   # Redis-related modules
│   │   ├── __init__.py
│   │   ├── connection.py        # Redis connection management
│   │   └── timeseries_operations.py  # TimeSeries operations
│   └── data/                    # Data processing modules
│       ├── __init__.py
│       ├── spark_operations.py  # Spark data extraction & transformation
│       └── processing.py        # Partition and cycle processing
├── legacy_main.py               # Backward compatibility script
├── requirements.txt             # Project dependencies
└── PROJECT_STRUCTURE.md         # This documentation
```

## 📦 Modules Overview

### 1. **Configuration Module** (`src/config/`)
- **`redis_config.py`**: Centralized Redis configuration with environment variable support
- Supports customization through environment variables
- Provides default configuration for backward compatibility

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
- **`spark_operations.py`**: Spark DataFrame operations for data extraction and transformation
- **`processing.py`**: Partition processing and cycle-based data operations
- Handles aggregation, filtering, and caching

### 5. **Main Orchestration** (`src/main.py`)
- **`DataPipeline`** class: High-level pipeline orchestrator
- Combines all modules into a cohesive workflow
- Provides methods for full pipeline execution and single cycle processing

## 🚀 Usage Examples

### Using the New Modular API

```python
from pyspark.sql import SparkSession
from src.main import DataPipeline

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Create and run pipeline
pipeline = DataPipeline(spark_session=spark, batch_size=5000)

# Run complete pipeline
stats = pipeline.run_pipeline()

# Or process a single cycle
cycle_stats = pipeline.run_single_cycle("cycle_123")

# Get available cycles
cycles = pipeline.get_available_cycles()
```

### Using Individual Modules

```python
from src.data.spark_operations import SparkDataProcessor
from src.redis.connection import RedisConnectionManager
from src.config.redis_config import get_redis_config

# Spark operations
spark_processor = SparkDataProcessor()
df = spark_processor.get_calculations_dataframe()
cycles = spark_processor.get_unique_cycles(df)

# Redis operations
redis_config = get_redis_config()
redis_manager = RedisConnectionManager(redis_config)
client = redis_manager.get_client()
```

### Legacy Compatibility

For backward compatibility, use the legacy script:

```python
# Run the original script interface
python legacy_main.py
```

## ⚙️ Configuration

### Environment Variables

Configure Redis connection using environment variables:

```bash
export REDIS_HOST="your-redis-host"
export REDIS_PORT="10000"
export REDIS_PASSWORD="your-password"
export REDIS_SSL="true"
export REDIS_MAX_CONNECTIONS="50"
export REDIS_SOCKET_TIMEOUT="30"
export REDIS_TS_RETENTION="2592000000"  # 30 days in milliseconds
export REDIS_TS_CHUNK_SIZE="4096"
export REDIS_TS_DUPLICATE_POLICY="LAST"
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