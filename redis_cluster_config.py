"""
Redis Cluster Configuration for High-Performance Time Series Operations
"""

# Single Node Databricks Configuration (Current)
SINGLE_NODE_CONFIG = {
    "cluster_type": "single_node",
    "memory_gb": 256,
    "cores": 32,
    "spark_config": {
        "spark.executor.memory": "200g",
        "spark.executor.cores": "8",
        "spark.executor.instances": "4",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.network.timeout": "600s"
    },
    "redis_writer_config": {
        "batch_size": 32000,  # Optimized for single node
        "max_workers": 32,
        "pipeline_size": 2000
    }
}

# Recommended Multi-Node Configuration for Better Performance
MULTI_NODE_CONFIG = {
    "cluster_type": "multi_node",
    "driver_memory_gb": 32,
    "worker_memory_gb": 64,
    "num_workers": 4,
    "cores_per_worker": 16,
    "spark_config": {
        "spark.executor.memory": "50g",
        "spark.executor.cores": "5",
        "spark.executor.instances": "12",  # 3 per worker
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.network.timeout": "600s",
        "spark.executor.heartbeatInterval": "60s",
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.maxExecutors": "16"
    },
    "redis_writer_config": {
        "batch_size": 50000,  # Larger batches for multi-node
        "max_workers": 64,
        "pipeline_size": 3000
    }
}

# High-Performance Multi-Node Configuration for Million+ Records
HIGH_PERFORMANCE_CONFIG = {
    "cluster_type": "high_performance",
    "driver_memory_gb": 64,
    "worker_memory_gb": 128,
    "num_workers": 8,
    "cores_per_worker": 16,
    "spark_config": {
        "spark.executor.memory": "100g",
        "spark.executor.cores": "4",  # Lower cores per executor for better parallelism
        "spark.executor.instances": "32",  # 4 per worker
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64MB",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.network.timeout": "800s",
        "spark.executor.heartbeatInterval": "60s",
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.maxExecutors": "40",
        "spark.sql.shuffle.partitions": "400"
    },
    "redis_writer_config": {
        "batch_size": 100000,  # Very large batches
        "max_workers": 128,
        "pipeline_size": 5000
    }
}

# Redis Cluster Configurations
REDIS_CONFIGS = {
    "single_instance": {
        "host": "localhost",
        "port": 6379,
        "db": 0,
        "use_cluster": False,
        "max_connections": 100
    },
    
    "redis_cluster": {
        "host": "redis-cluster-endpoint",
        "port": 7000,
        "use_cluster": True,
        "skip_full_coverage_check": True,
        "max_connections_per_node": 50,
        "startup_nodes": [
            {"host": "redis-node-1", "port": 7000},
            {"host": "redis-node-2", "port": 7000},
            {"host": "redis-node-3", "port": 7000}
        ]
    },
    
    "redis_sentinel": {
        "sentinels": [
            ("sentinel-1", 26379),
            ("sentinel-2", 26379),
            ("sentinel-3", 26379)
        ],
        "service_name": "mymaster",
        "use_cluster": False,
        "max_connections": 100
    }
}

def get_optimal_config(data_size_millions: float, 
                      redis_type: str = "redis_cluster") -> dict:
    """
    Get optimal configuration based on data size and Redis setup
    
    Args:
        data_size_millions: Size of data in millions of records
        redis_type: Type of Redis setup (single_instance, redis_cluster, redis_sentinel)
    
    Returns:
        Optimal configuration dictionary
    """
    
    if data_size_millions < 1:
        cluster_config = SINGLE_NODE_CONFIG
    elif data_size_millions < 10:
        cluster_config = MULTI_NODE_CONFIG
    else:
        cluster_config = HIGH_PERFORMANCE_CONFIG
    
    redis_config = REDIS_CONFIGS[redis_type]
    
    return {
        "cluster_config": cluster_config,
        "redis_config": redis_config,
        "estimated_performance": {
            "records_per_second": estimate_throughput(data_size_millions, cluster_config),
            "estimated_duration_minutes": estimate_duration(data_size_millions, cluster_config)
        }
    }

def estimate_throughput(data_size_millions: float, cluster_config: dict) -> int:
    """Estimate throughput based on configuration"""
    base_throughput = 10000  # records per second baseline
    
    if cluster_config["cluster_type"] == "single_node":
        multiplier = 3
    elif cluster_config["cluster_type"] == "multi_node":
        multiplier = 8
    else:  # high_performance
        multiplier = 15
    
    return int(base_throughput * multiplier)

def estimate_duration(data_size_millions: float, cluster_config: dict) -> float:
    """Estimate processing duration in minutes"""
    throughput = estimate_throughput(data_size_millions, cluster_config)
    total_records = data_size_millions * 1_000_000
    duration_seconds = total_records / throughput
    return duration_seconds / 60