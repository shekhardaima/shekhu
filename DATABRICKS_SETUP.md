# Databricks Setup Guide

This guide explains how to set up and deploy the data processing pipeline for Databricks runtime 14.3 with Delta Lake support.

## 🏗️ Environment Setup

### Local Development with Databricks Connect

#### 1. Install Dependencies

```bash
# Install the project dependencies
pip install -r requirements.txt

# Verify Databricks Connect installation
databricks-connect test
```

#### 2. Configure Databricks Connect

Set up your environment variables for Databricks Connect:

```bash
# Required environment variables
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-personal-access-token"
export DATABRICKS_CLUSTER_ID="your-cluster-id"

# Optional Spark configuration
export SPARK_CONF_spark_sql_adaptive_enabled="true"
export SPARK_CONF_spark_sql_adaptive_coalescePartitions_enabled="true"
```

#### 3. Test Connection

```python
from src.config.databricks_config import DatabricksSparkManager

# Test Databricks connection
spark_manager = DatabricksSparkManager()
spark = spark_manager.get_spark_session()

# Test Delta table access
success = spark_manager.test_delta_connectivity("clearview_prod.silver.calculations")
print(f"Delta connectivity: {'✅ Success' if success else '❌ Failed'}")
```

### Databricks Cluster Configuration

#### Recommended Cluster Settings for Runtime 14.3

```yaml
Cluster Configuration:
  Runtime Version: 14.3 LTS (includes Apache Spark 3.5.0, Scala 2.12)
  Worker Type: Standard_DS3_v2 (4 cores, 14 GB RAM) or higher
  Driver Type: Standard_DS3_v2 (4 cores, 14 GB RAM) or higher
  Workers: 2-10 (depending on data volume)
  
Spark Configuration:
  spark.databricks.delta.optimizeWrite.enabled: true
  spark.databricks.delta.autoCompact.enabled: true
  spark.sql.adaptive.enabled: true
  spark.sql.adaptive.coalescePartitions.enabled: true
  spark.sql.adaptive.skewJoin.enabled: true
  spark.serializer: org.apache.spark.serializer.KryoSerializer
  spark.sql.execution.arrow.pyspark.enabled: true
```

#### Required Libraries

Install these libraries on your Databricks cluster:

```bash
# Python libraries (via PyPI)
redis>=4.5.0

# Or via requirements.txt
%pip install -r /Workspace/path/to/requirements.txt
```

## 🔧 Configuration Management

### Environment Variables

#### For Local Development (Databricks Connect)

```bash
# Databricks connection
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi1234567890abcdef"
export DATABRICKS_CLUSTER_ID="0123-456789-abc123"

# Redis configuration
export REDIS_HOST="your-redis-host.redis.azure.net"
export REDIS_PORT="10000"
export REDIS_PASSWORD="your-redis-password"
export REDIS_SSL="true"
export REDIS_MAX_CONNECTIONS="50"

# Optional Spark tuning
export SPARK_CONF_spark_sql_adaptive_enabled="true"
export SPARK_CONF_spark_databricks_delta_optimizeWrite_enabled="true"
```

#### For Databricks Cluster (using Databricks Secrets)

```python
# In Databricks notebook
redis_config = {
    "host": dbutils.secrets.get(scope="redis-secrets", key="host"),
    "port": int(dbutils.secrets.get(scope="redis-secrets", key="port")),
    "password": dbutils.secrets.get(scope="redis-secrets", key="password"),
    "ssl": True,
    "max_connections": 100,  # Higher for cluster
}
```

### Databricks Secrets Setup

```bash
# Create secret scope
databricks secrets create-scope --scope redis-secrets

# Add secrets
databricks secrets put --scope redis-secrets --key host
databricks secrets put --scope redis-secrets --key port  
databricks secrets put --scope redis-secrets --key password
```

## 🚀 Deployment Options

### Option 1: Databricks Notebook

1. **Upload Source Code**
   ```bash
   # Upload the src/ directory to Databricks workspace
   databricks workspace import-dir ./src /Workspace/Users/your-email/data-pipeline/src
   ```

2. **Create Notebook**
   ```python
   # Databricks notebook cell
   %run /Workspace/Users/your-email/data-pipeline/databricks_example
   
   # Run the pipeline
   stats = databricks_pipeline_example()
   ```

### Option 2: Databricks Jobs

1. **Create Job Configuration**
   ```json
   {
     "name": "Data Processing Pipeline",
     "new_cluster": {
       "spark_version": "14.3.x-scala2.12",
       "node_type_id": "Standard_DS3_v2",
       "num_workers": 4,
       "spark_conf": {
         "spark.databricks.delta.optimizeWrite.enabled": "true",
         "spark.databricks.delta.autoCompact.enabled": "true"
       }
     },
     "libraries": [
       {"pypi": {"package": "redis>=4.5.0"}}
     ],
     "python_task": {
       "python_file": "/Workspace/Users/your-email/data-pipeline/src/main.py"
     }
   }
   ```

2. **Deploy via CLI**
   ```bash
   databricks jobs create --json-file job-config.json
   ```

### Option 3: Databricks Asset Bundles (DABs)

1. **Create `databricks.yml`**
   ```yaml
   bundle:
     name: data-processing-pipeline
   
   resources:
     jobs:
       data_pipeline:
         name: "Data Processing Pipeline"
         job_clusters:
           - job_cluster_key: main_cluster
             new_cluster:
               spark_version: "14.3.x-scala2.12"
               node_type_id: "Standard_DS3_v2"
               num_workers: 4
         tasks:
           - task_key: process_data
             job_cluster_key: main_cluster
             python_task:
               python_file: "./src/main.py"
   ```

2. **Deploy Bundle**
   ```bash
   databricks bundle deploy --target prod
   databricks bundle run data_pipeline --target prod
   ```

## 📊 Performance Optimization

### Delta Table Optimizations

```python
# Enable automatic optimizations
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# Manual optimization (run periodically)
spark.sql("OPTIMIZE clearview_prod.silver.calculations")
spark.sql("VACUUM clearview_prod.silver.calculations RETAIN 168 HOURS")
```

### Cluster Sizing Guidelines

| Data Volume | Worker Count | Worker Type | Driver Type | Batch Size |
|-------------|--------------|-------------|-------------|------------|
| < 1GB | 2-4 | Standard_DS3_v2 | Standard_DS3_v2 | 5,000 |
| 1-10GB | 4-8 | Standard_DS4_v2 | Standard_DS4_v2 | 10,000 |
| 10-100GB | 8-16 | Standard_DS5_v2 | Standard_DS5_v2 | 20,000 |
| > 100GB | 16+ | Standard_DS5_v2 | Standard_DS5_v2 | 50,000 |

## 🔍 Monitoring and Debugging

### Logging Configuration

```python
# Enhanced logging for Databricks
from src.utils.logging_config import setup_logger
import logging

logger = setup_logger(__name__, level=logging.INFO)

# View logs in Databricks
display(spark.sql("SELECT * FROM cluster_log_delivery"))
```

### Performance Monitoring

```python
# Monitor Spark UI
print(f"Spark UI: {spark.sparkContext.uiWebUrl}")

# Check Delta table statistics
spark.sql("DESCRIBE DETAIL clearview_prod.silver.calculations").display()

# Monitor query execution
spark.sql("SELECT * FROM clearview_prod.silver.calculations LIMIT 1").explain(True)
```

### Common Issues and Solutions

#### Issue 1: Connection Timeout
```python
# Solution: Increase timeout values
redis_config = {
    "socket_timeout": 60,
    "socket_connect_timeout": 30,
    "retry_on_timeout": True
}
```

#### Issue 2: Memory Issues
```python
# Solution: Optimize batch size and enable adaptive query execution
pipeline = DataPipeline(batch_size=1000)  # Smaller batches
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

#### Issue 3: Delta Table Lock Issues
```python
# Solution: Use optimistic concurrency control
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

## 🧪 Testing

### Local Testing with Databricks Connect

```python
# Test script
python -m pytest tests/ -v

# Test specific components
from src.config.databricks_config import DatabricksSparkManager
from src.main import DataPipeline

# Test Databricks connectivity
manager = DatabricksSparkManager()
assert manager.test_delta_connectivity()

# Test pipeline
pipeline = DataPipeline(batch_size=100)  # Small batch for testing
cycles = pipeline.get_available_cycles()
assert len(cycles) > 0
```

### Integration Testing

```python
# Run a small subset test
def test_single_cycle():
    pipeline = DataPipeline(batch_size=10)
    cycles = pipeline.get_available_cycles()
    if cycles:
        stats = pipeline.run_single_cycle(cycles[0])
        assert stats['records_attempted'] >= 0
        assert stats['success_rate'] >= 0
```

## 📝 Best Practices

1. **Use Databricks Secrets** for sensitive configuration
2. **Enable Delta optimizations** for better performance
3. **Monitor resource usage** and adjust cluster size accordingly
4. **Use appropriate batch sizes** based on data volume
5. **Implement proper error handling** and retry logic
6. **Cache DataFrames** when reusing them multiple times
7. **Use DataFrame operations** instead of RDD operations
8. **Optimize Delta tables** regularly with VACUUM and OPTIMIZE

## 🔗 Useful Links

- [Databricks Runtime 14.3 Release Notes](https://docs.databricks.com/release-notes/runtime/14.3.html)
- [Databricks Connect Documentation](https://docs.databricks.com/dev-tools/databricks-connect.html)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/)

---

This setup guide provides everything needed to deploy and run the data processing pipeline in Databricks environment with optimal performance and reliability.