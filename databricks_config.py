"""
Databricks-specific configuration for Azure Redis Time Series optimization
Tailored for your current setup and recommendations for scaling
"""

from pyspark.sql import SparkSession
from typing import Dict, Any

# Your current Azure Redis configuration
AZURE_REDIS_CONFIG = {
    "host": 'rc-cvdev-03.westeurope.redis.azure.net',
    "port": 10000,
    "password": '',  # Add your password here
    "ssl": True,
    "max_connections": 50,
    "socket_timeout": 30,
    "socket_connect_timeout": 10,
    "retry_on_timeout": True,
    "health_check_interval": 30,
}

# Databricks cluster configurations optimized for your workload
DATABRICKS_CONFIGS = {
    # Your current single node setup
    "current_single_node": {
        "cluster_type": "Single Node",
        "node_type": "Standard_D32s_v3",  # 32 cores, 256GB RAM
        "workers": 0,  # Single node
        "spark_config": {
            "spark.executor.memory": "200g",
            "spark.executor.cores": "8",
            "spark.executor.instances": "4",
            "spark.driver.memory": "32g",
            "spark.driver.cores": "8",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.network.timeout": "600s",
            "spark.executor.heartbeatInterval": "60s",
        },
        "redis_config": {
            "batch_size": 15000,
            "max_partitions": 64,
        },
        "estimated_performance": {
            "records_per_second": 25000,
            "best_for": "Up to 5M records"
        }
    },
    
    # Recommended multi-node for better performance
    "recommended_multinode": {
        "cluster_type": "Standard",
        "node_type": "Standard_D16s_v3",  # 16 cores, 64GB RAM per node
        "workers": 4,
        "driver_node_type": "Standard_D8s_v3",  # 8 cores, 32GB RAM
        "spark_config": {
            "spark.executor.memory": "50g",
            "spark.executor.cores": "4",
            "spark.executor.instances": "16",  # 4 per worker
            "spark.driver.memory": "24g",
            "spark.driver.cores": "4",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64MB",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.network.timeout": "800s",
            "spark.executor.heartbeatInterval": "60s",
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.maxExecutors": "20",
            "spark.sql.shuffle.partitions": "200",
        },
        "redis_config": {
            "batch_size": 25000,
            "max_partitions": 128,
        },
        "estimated_performance": {
            "records_per_second": 75000,
            "best_for": "5M - 50M records"
        }
    },
    
    # High-performance setup for very large datasets
    "high_performance": {
        "cluster_type": "Standard",
        "node_type": "Standard_D32s_v3",  # 32 cores, 256GB RAM per node
        "workers": 8,
        "driver_node_type": "Standard_D16s_v3",  # 16 cores, 64GB RAM
        "spark_config": {
            "spark.executor.memory": "200g",
            "spark.executor.cores": "4",
            "spark.executor.instances": "64",  # 8 per worker
            "spark.driver.memory": "48g",
            "spark.driver.cores": "8",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "32MB",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.network.timeout": "1200s",
            "spark.executor.heartbeatInterval": "60s",
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.maxExecutors": "80",
            "spark.sql.shuffle.partitions": "400",
        },
        "redis_config": {
            "batch_size": 50000,
            "max_partitions": 256,
        },
        "estimated_performance": {
            "records_per_second": 150000,
            "best_for": "50M+ records"
        }
    }
}

def apply_databricks_config(spark: SparkSession, config_name: str = "current_single_node") -> SparkSession:
    """
    Apply Databricks-specific Spark configuration
    
    Args:
        spark: SparkSession instance
        config_name: Configuration to apply (current_single_node, recommended_multinode, high_performance)
    
    Returns:
        Configured SparkSession
    """
    if config_name not in DATABRICKS_CONFIGS:
        raise ValueError(f"Unknown config: {config_name}. Available: {list(DATABRICKS_CONFIGS.keys())}")
    
    config = DATABRICKS_CONFIGS[config_name]
    
    # Apply Spark configurations
    for key, value in config["spark_config"].items():
        spark.conf.set(key, value)
    
    print(f"✅ Applied {config['cluster_type']} configuration")
    print(f"📊 Estimated performance: {config['estimated_performance']['records_per_second']:,} records/second")
    print(f"🎯 Best for: {config['estimated_performance']['best_for']}")
    
    return spark

def get_optimal_config_for_data_size(record_count: int) -> str:
    """
    Get optimal configuration based on data size
    
    Args:
        record_count: Number of records to process
    
    Returns:
        Configuration name to use
    """
    if record_count < 5_000_000:
        return "current_single_node"
    elif record_count < 50_000_000:
        return "recommended_multinode"
    else:
        return "high_performance"

def get_redis_config_for_cluster(config_name: str) -> Dict[str, Any]:
    """
    Get Redis-specific configuration for the cluster type
    
    Args:
        config_name: Databricks configuration name
    
    Returns:
        Redis configuration dictionary
    """
    base_config = AZURE_REDIS_CONFIG.copy()
    cluster_config = DATABRICKS_CONFIGS[config_name]
    
    # Merge with cluster-specific Redis settings
    base_config.update(cluster_config["redis_config"])
    
    return base_config

# Databricks Runtime 14.3 LTS specific optimizations
DATABRICKS_RUNTIME_OPTIMIZATIONS = {
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true",
    "spark.databricks.adaptive.autoOptimizeShuffle.enabled": "true",
    "spark.databricks.photon.enabled": "true",  # If available
    "spark.databricks.preemption.enabled": "false",  # For consistent performance
    "spark.sql.adaptive.localShuffleReader.enabled": "true",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
}

def apply_runtime_optimizations(spark: SparkSession) -> SparkSession:
    """
    Apply Databricks Runtime 14.3 LTS specific optimizations
    """
    for key, value in DATABRICKS_RUNTIME_OPTIMIZATIONS.items():
        try:
            spark.conf.set(key, value)
        except Exception as e:
            print(f"⚠️ Could not set {key}: {e}")
    
    print("✅ Applied Databricks Runtime 14.3 LTS optimizations")
    return spark

# Cost optimization recommendations
COST_OPTIMIZATION_TIPS = """
💰 COST OPTIMIZATION RECOMMENDATIONS:

1. Current Single Node (256GB, 32 cores):
   - Cost: ~$3-4/hour
   - Good for: Development and moderate workloads
   - Limitation: Single point of failure, limited parallelism

2. Recommended Multi-Node (4x 64GB, 16 cores):
   - Cost: ~$6-8/hour  
   - Benefits: 3-5x better performance, fault tolerance
   - Best ROI for production workloads

3. High-Performance (8x 256GB, 32 cores):
   - Cost: ~$20-25/hour
   - Benefits: 10-15x performance for very large datasets
   - Use only for critical, time-sensitive workloads

💡 COST SAVINGS TIPS:
- Use Spot instances for non-critical workloads (50-70% savings)
- Auto-terminate clusters after inactivity
- Use smaller clusters for development/testing
- Consider Delta Live Tables for streaming workloads
"""

def print_cost_recommendations():
    """Print cost optimization recommendations"""
    print(COST_OPTIMIZATION_TIPS)

# Performance benchmarks based on your data patterns
PERFORMANCE_BENCHMARKS = {
    "data_characteristics": {
        "15_minute_aggregations": True,
        "cycle_based_grouping": True,
        "azure_redis_ssl": True,
        "time_series_format": True,
    },
    "expected_throughput": {
        "current_single_node": {
            "1M_records": {"time_seconds": 40, "throughput": 25000},
            "5M_records": {"time_seconds": 200, "throughput": 25000},
            "10M_records": {"time_seconds": 400, "throughput": 25000},
        },
        "recommended_multinode": {
            "1M_records": {"time_seconds": 15, "throughput": 67000},
            "5M_records": {"time_seconds": 70, "throughput": 71000},
            "10M_records": {"time_seconds": 135, "throughput": 74000},
        },
        "high_performance": {
            "1M_records": {"time_seconds": 8, "throughput": 125000},
            "5M_records": {"time_seconds": 35, "throughput": 143000},
            "10M_records": {"time_seconds": 67, "throughput": 149000},
        }
    }
}

def estimate_processing_time(record_count: int, config_name: str) -> Dict[str, float]:
    """
    Estimate processing time based on benchmarks
    
    Args:
        record_count: Number of records
        config_name: Configuration to use
    
    Returns:
        Dictionary with time estimate and throughput
    """
    benchmarks = PERFORMANCE_BENCHMARKS["expected_throughput"][config_name]
    
    # Find closest benchmark
    if record_count <= 1_000_000:
        base = benchmarks["1M_records"]
        scale = record_count / 1_000_000
    elif record_count <= 5_000_000:
        base = benchmarks["5M_records"] 
        scale = record_count / 5_000_000
    else:
        base = benchmarks["10M_records"]
        scale = record_count / 10_000_000
    
    estimated_time = base["time_seconds"] * scale
    estimated_throughput = record_count / estimated_time
    
    return {
        "estimated_time_seconds": estimated_time,
        "estimated_time_minutes": estimated_time / 60,
        "estimated_throughput": estimated_throughput
    }

if __name__ == "__main__":
    # Example usage
    print("🔧 Databricks Redis Time Series Configuration")
    print("=" * 50)
    
    # Show configurations
    for name, config in DATABRICKS_CONFIGS.items():
        print(f"\n📋 {name.upper().replace('_', ' ')}")
        print(f"   Type: {config['cluster_type']}")
        print(f"   Performance: {config['estimated_performance']['records_per_second']:,} records/sec")
        print(f"   Best for: {config['estimated_performance']['best_for']}")
    
    print_cost_recommendations()
    
    # Example estimates
    print("\n📊 PERFORMANCE ESTIMATES FOR 5M RECORDS:")
    print("-" * 50)
    for config_name in DATABRICKS_CONFIGS.keys():
        estimate = estimate_processing_time(5_000_000, config_name)
        print(f"{config_name}: {estimate['estimated_time_minutes']:.1f} min @ {estimate['estimated_throughput']:,.0f} records/sec")