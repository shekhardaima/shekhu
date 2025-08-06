import redis
import redis.sentinel
from redis.cluster import RedisCluster
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, collect_list, struct, hash as spark_hash
from pyspark.sql.types import *
import logging
from typing import List, Dict, Any, Iterator, Tuple
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OptimizedRedisTimeSeriesWriter:
    """
    High-performance Redis Time Series writer optimized for multi-million records
    with cross-slot error prevention and bulk operations using TS.MADD
    """
    
    def __init__(self, 
                 redis_config: Dict[str, Any],
                 batch_size: int = 10000,
                 max_workers: int = 32,
                 pipeline_size: int = 1000,
                 use_cluster: bool = True):
        """
        Initialize the Redis Time Series writer
        
        Args:
            redis_config: Redis connection configuration
            batch_size: Number of records per batch for TS.MADD
            max_workers: Number of concurrent threads
            pipeline_size: Size of Redis pipeline
            use_cluster: Whether to use Redis Cluster
        """
        self.redis_config = redis_config
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.pipeline_size = pipeline_size
        self.use_cluster = use_cluster
        self._connection_pool = {}
        self._lock = threading.Lock()
        
    def _get_redis_connection(self, cycle_id: str = None) -> redis.Redis:
        """
        Get Redis connection, ensuring same slot for same CycleId
        """
        thread_id = threading.current_thread().ident
        
        if thread_id not in self._connection_pool:
            with self._lock:
                if thread_id not in self._connection_pool:
                    if self.use_cluster:
                        # For Redis Cluster, use cycle_id to determine connection
                        self._connection_pool[thread_id] = RedisCluster(
                            host=self.redis_config.get('host', 'localhost'),
                            port=self.redis_config.get('port', 7000),
                            decode_responses=False,
                            skip_full_coverage_check=True,
                            max_connections_per_node=50
                        )
                    else:
                        self._connection_pool[thread_id] = redis.Redis(
                            host=self.redis_config.get('host', 'localhost'),
                            port=self.redis_config.get('port', 6379),
                            db=self.redis_config.get('db', 0),
                            decode_responses=False,
                            max_connections=50
                        )
        
        return self._connection_pool[thread_id]
    
    def _create_timeseries_keys(self, cycle_id: str, metrics: List[str]) -> List[str]:
        """
        Create time series keys ensuring they hash to the same slot
        """
        # Use hash tags to ensure all keys for same CycleId go to same slot
        return [f"ts:{{{cycle_id}}}:{metric}" for metric in metrics]
    
    def _ensure_timeseries_exist(self, redis_conn: redis.Redis, keys: List[str]):
        """
        Ensure time series exist, create if not
        """
        pipe = redis_conn.pipeline()
        
        for key in keys:
            try:
                # Try to create time series with retention and labels
                pipe.execute_command(
                    'TS.CREATE', key,
                    'RETENTION', 86400000,  # 24 hours in milliseconds
                    'CHUNK_SIZE', 4096,
                    'DUPLICATE_POLICY', 'LAST'
                )
            except Exception:
                # Key might already exist, ignore error
                pass
        
        try:
            pipe.execute()
        except Exception as e:
            # Some keys might already exist, which is fine
            logger.debug(f"Some time series already exist: {e}")
    
    def _write_batch_tsmadd(self, redis_conn: redis.Redis, 
                           cycle_data: List[Dict[str, Any]]) -> int:
        """
        Write batch using TS.MADD for maximum performance
        """
        if not cycle_data:
            return 0
            
        try:
            # Group by CycleId to ensure same slot operations
            cycle_groups = defaultdict(list)
            for record in cycle_data:
                cycle_groups[record['CycleId']].append(record)
            
            total_written = 0
            
            for cycle_id, records in cycle_groups.items():
                # Prepare TS.MADD command arguments
                madd_args = []
                
                # Get all unique metrics for this cycle
                all_metrics = set()
                for record in records:
                    all_metrics.update(record.get('metrics', {}).keys())
                
                # Ensure time series exist
                ts_keys = self._create_timeseries_keys(cycle_id, list(all_metrics))
                self._ensure_timeseries_exist(redis_conn, ts_keys)
                
                # Build TS.MADD arguments
                for record in records:
                    timestamp = record['timestamp']
                    metrics = record.get('metrics', {})
                    
                    for metric, value in metrics.items():
                        key = f"ts:{{{cycle_id}}}:{metric}"
                        madd_args.extend([key, timestamp, value])
                
                # Execute TS.MADD in batches to avoid too large commands
                batch_size = self.pipeline_size * 3  # 3 args per time series point
                
                for i in range(0, len(madd_args), batch_size):
                    batch_args = madd_args[i:i + batch_size]
                    if batch_args:
                        result = redis_conn.execute_command('TS.MADD', *batch_args)
                        total_written += len(batch_args) // 3
                        
            return total_written
            
        except Exception as e:
            logger.error(f"Error writing batch: {e}")
            return 0
    
    def _process_partition(self, partition_data: Iterator[Dict[str, Any]]) -> Iterator[int]:
        """
        Process a partition of data
        """
        redis_conn = self._get_redis_connection()
        batch = []
        total_written = 0
        
        try:
            for record in partition_data:
                batch.append(record)
                
                if len(batch) >= self.batch_size:
                    written = self._write_batch_tsmadd(redis_conn, batch)
                    total_written += written
                    batch = []
            
            # Process remaining records
            if batch:
                written = self._write_batch_tsmadd(redis_conn, batch)
                total_written += written
                
        except Exception as e:
            logger.error(f"Error processing partition: {e}")
        
        yield total_written
    
    def write_dataframe(self, df: DataFrame, 
                       cycle_id_col: str = 'CycleId',
                       timestamp_col: str = 'timestamp',
                       metric_cols: List[str] = None) -> Dict[str, Any]:
        """
        Write Spark DataFrame to Redis Time Series
        
        Args:
            df: Spark DataFrame with time series data
            cycle_id_col: Column name for CycleId
            timestamp_col: Column name for timestamp
            metric_cols: List of metric column names
        
        Returns:
            Dictionary with write statistics
        """
        start_time = time.time()
        
        # Auto-detect metric columns if not provided
        if metric_cols is None:
            metric_cols = [col for col in df.columns 
                          if col not in [cycle_id_col, timestamp_col]]
        
        logger.info(f"Writing {df.count()} records with metrics: {metric_cols}")
        
        # Optimize DataFrame for Redis operations
        # 1. Repartition by CycleId to ensure same cycle data goes to same partition
        # 2. This prevents cross-slot errors and improves batching efficiency
        
        # Calculate optimal number of partitions based on cluster resources
        # For 32 cores, use slightly more partitions to balance load
        num_partitions = min(64, max(8, df.rdd.getNumPartitions()))
        
        # Repartition by CycleId hash to group same cycles together
        df_repartitioned = df.repartition(num_partitions, spark_hash(col(cycle_id_col)))
        
        # Transform DataFrame to the format expected by Redis writer
        df_transformed = df_repartitioned.select(
            col(cycle_id_col).alias('CycleId'),
            col(timestamp_col).alias('timestamp'),
            struct(*[col(metric_col).alias(metric_col) for metric_col in metric_cols]).alias('metrics')
        )
        
        # Convert to RDD and process partitions
        rdd_transformed = df_transformed.rdd.map(
            lambda row: {
                'CycleId': row['CycleId'],
                'timestamp': int(row['timestamp']),
                'metrics': row['metrics'].asDict()
            }
        )
        
        # Process partitions in parallel
        results = rdd_transformed.mapPartitions(self._process_partition).collect()
        
        total_written = sum(results)
        end_time = time.time()
        duration = end_time - start_time
        
        stats = {
            'total_records_written': total_written,
            'duration_seconds': duration,
            'records_per_second': total_written / duration if duration > 0 else 0,
            'partitions_processed': len(results),
            'batch_size': self.batch_size
        }
        
        logger.info(f"Write completed: {stats}")
        return stats


class SparkRedisTimeSeriesOptimizer:
    """
    Spark-optimized Redis Time Series writer with advanced performance tuning
    """
    
    @staticmethod
    def optimize_spark_config(spark: SparkSession, 
                             memory_gb: int = 256, 
                             cores: int = 32) -> SparkSession:
        """
        Optimize Spark configuration for Redis time series operations
        """
        # Calculate optimal settings based on cluster resources
        executor_memory = f"{int(memory_gb * 0.8)}g"  # Leave 20% for OS
        executor_cores = min(5, cores // 4)  # Don't use too many cores per executor
        max_executors = cores // executor_cores
        
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        spark.conf.set("spark.executor.memory", executor_memory)
        spark.conf.set("spark.executor.cores", str(executor_cores))
        spark.conf.set("spark.dynamicAllocation.maxExecutors", str(max_executors))
        spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
        
        # Network optimizations for Redis connections
        spark.conf.set("spark.network.timeout", "600s")
        spark.conf.set("spark.executor.heartbeatInterval", "60s")
        
        return spark
    
    @staticmethod
    def create_optimized_writer(redis_config: Dict[str, Any], 
                               cluster_cores: int = 32) -> OptimizedRedisTimeSeriesWriter:
        """
        Create optimized Redis writer based on cluster configuration
        """
        # Optimize batch size based on available cores and memory
        batch_size = min(50000, max(5000, cluster_cores * 1000))
        max_workers = min(cluster_cores, 64)  # Don't exceed reasonable thread count
        pipeline_size = 2000  # Larger pipeline for better throughput
        
        return OptimizedRedisTimeSeriesWriter(
            redis_config=redis_config,
            batch_size=batch_size,
            max_workers=max_workers,
            pipeline_size=pipeline_size,
            use_cluster=True
        )


# Example usage and configuration
def example_usage():
    """
    Example of how to use the optimized Redis Time Series writer
    """
    # Initialize Spark with optimizations
    spark = SparkSession.builder \
        .appName("OptimizedRedisTimeSeriesWriter") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    # Optimize Spark for your cluster (256GB, 32 cores)
    spark = SparkRedisTimeSeriesOptimizer.optimize_spark_config(
        spark, memory_gb=256, cores=32
    )
    
    # Redis configuration
    redis_config = {
        'host': 'your-redis-cluster-host',
        'port': 7000,  # Redis Cluster port
        'password': 'your-password',  # if needed
    }
    
    # Create optimized writer
    writer = SparkRedisTimeSeriesOptimizer.create_optimized_writer(
        redis_config, cluster_cores=32
    )
    
    # Example DataFrame (replace with your actual data)
    # Assuming your data has columns: CycleId, timestamp, metric1, metric2, etc.
    df = spark.read.parquet("your-data-path")
    
    # Write to Redis Time Series
    stats = writer.write_dataframe(
        df=df,
        cycle_id_col='CycleId',
        timestamp_col='timestamp',
        metric_cols=['metric1', 'metric2', 'metric3']  # your metric columns
    )
    
    print(f"Successfully wrote {stats['total_records_written']} records")
    print(f"Performance: {stats['records_per_second']:.0f} records/second")

if __name__ == "__main__":
    example_usage()