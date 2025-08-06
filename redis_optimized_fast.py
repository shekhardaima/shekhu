import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    floor,
    unix_timestamp,
    concat,
    lit,
    avg,
    from_unixtime,
    max,
    hash as spark_hash,
)
import logging
import time
import sys
from collections import defaultdict
import math
import threading
from typing import Iterator, Dict, Any, List, Tuple

# Enhanced logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Your Azure Redis configuration
redis_config = {
    "host": 'rc-cvdev-03.westeurope.redis.azure.net',
    "port": 10000,
    "password": '',
    "ssl": True,
    "max_connections": 50,
    "socket_timeout": 30,
    "socket_connect_timeout": 10,
    "retry_on_timeout": True,
    "health_check_interval": 30,
}

# Thread-local storage for Redis connections
thread_local = threading.local()

def get_redis_client(redis_config: Dict[str, Any]) -> redis.Redis:
    """
    Get thread-local Redis client with optimized connection pooling
    """
    if not hasattr(thread_local, 'redis_client'):
        connection_pool = redis.ConnectionPool(
            host=redis_config["host"],
            port=redis_config["port"],
            password=redis_config["password"],
            connection_class=redis.SSLConnection,
            max_connections=redis_config.get("max_connections", 50),
            socket_timeout=redis_config.get("socket_timeout", 30),
            socket_connect_timeout=redis_config.get("socket_connect_timeout", 10),
            retry_on_timeout=redis_config.get("retry_on_timeout", True),
            health_check_interval=redis_config.get("health_check_interval", 30),
        )
        thread_local.redis_client = redis.StrictRedis(
            connection_pool=connection_pool, 
            decode_responses=True
        )
    return thread_local.redis_client

def get_optimized_dataframe_no_repartition(spark: SparkSession) -> "DataFrame":
    """
    Get the optimized DataFrame WITHOUT expensive repartitioning
    Uses coalesce and natural partitioning instead
    """
    logger.info("🚀 Creating optimized DataFrame without repartitioning...")
    
    # Your original data source
    df = spark.table("clearview_prod.silver.measurements_new")
    
    df = df.select(
        "CycleId",
        "CustomerTag", 
        "StartTimestamp",
        "Value",
        "_ProcessedTime",
    )

    # Your original aggregation logic
    aggregated_df = df.groupBy(
        "CycleId",
        "CustomerTag",
        floor(unix_timestamp("StartTimestamp") / 900).alias("IntervalStartUnix"),
    ).agg(
        from_unixtime(floor(unix_timestamp("StartTimestamp") / 900) * 900).alias("StartTimestamp"),
        avg("Value").alias("Value"),
        max("_ProcessedTime").alias("_ProcessedTime"),
    )

    # Create time series format with hash tags for same-slot operations
    df_ts = aggregated_df.select(
        concat(lit("{"), col("CycleId"), lit("}"), col("CustomerTag")).alias("tag_id"),
        col("IntervalStartUnix").alias("timestamp"),
        col("Value").alias("value"),
        col("CycleId")
    )
    
    # OPTIMIZATION 1: Use coalesce instead of repartition (much faster)
    # This reduces partitions without shuffling data
    current_partitions = df_ts.rdd.getNumPartitions()
    optimal_partitions = min(current_partitions, max(32, current_partitions // 4))
    
    logger.info(f"📊 Partition optimization: {current_partitions} → {optimal_partitions} (coalesce)")
    df_ts_optimized = df_ts.coalesce(optimal_partitions)
    
    return df_ts_optimized

def get_optimized_dataframe_smart_partition(spark: SparkSession) -> "DataFrame":
    """
    Smart partitioning that only repartitions if absolutely necessary
    """
    logger.info("🧠 Using smart partitioning strategy...")
    
    # Your original data source
    df = spark.table("clearview_prod.silver.measurements_new")
    
    df = df.select(
        "CycleId",
        "CustomerTag", 
        "StartTimestamp",
        "Value",
        "_ProcessedTime",
    )

    # Your original aggregation logic WITH partitioning hint
    aggregated_df = df.groupBy(
        "CycleId",
        "CustomerTag",
        floor(unix_timestamp("StartTimestamp") / 900).alias("IntervalStartUnix"),
    ).agg(
        from_unixtime(floor(unix_timestamp("StartTimestamp") / 900) * 900).alias("StartTimestamp"),
        avg("Value").alias("Value"),
        max("_ProcessedTime").alias("_ProcessedTime"),
    )

    # Create time series format
    df_ts = aggregated_df.select(
        concat(lit("{"), col("CycleId"), lit("}"), col("CustomerTag")).alias("tag_id"),
        col("IntervalStartUnix").alias("timestamp"),
        col("Value").alias("value"),
        col("CycleId")
    )
    
    # OPTIMIZATION 2: Smart partitioning decision
    current_partitions = df_ts.rdd.getNumPartitions()
    
    # Check if data is already well distributed
    partition_sizes = df_ts.rdd.mapPartitionsWithIndex(
        lambda i, partition: [sum(1 for _ in partition)]
    ).collect()
    
    max_partition_size = max(partition_sizes) if partition_sizes else 0
    min_partition_size = min(partition_sizes) if partition_sizes else 0
    avg_partition_size = sum(partition_sizes) / len(partition_sizes) if partition_sizes else 0
    
    # Calculate skew ratio
    skew_ratio = max_partition_size / avg_partition_size if avg_partition_size > 0 else 1
    
    logger.info(f"📊 Partition analysis: {current_partitions} partitions, "
               f"sizes: min={min_partition_size}, max={max_partition_size}, "
               f"avg={avg_partition_size:.0f}, skew={skew_ratio:.2f}")
    
    # Only repartition if there's significant skew OR too many small partitions
    if skew_ratio > 3.0 or current_partitions > 500 or avg_partition_size < 1000:
        logger.info("⚡ Repartitioning due to skew/fragmentation...")
        optimal_partitions = min(200, max(20, current_partitions // 2))
        df_ts_optimized = df_ts.repartition(optimal_partitions, spark_hash(col("CycleId")))
    else:
        logger.info("✅ Data already well distributed, using coalesce...")
        optimal_partitions = min(current_partitions, max(32, current_partitions // 2))
        df_ts_optimized = df_ts.coalesce(optimal_partitions)
    
    return df_ts_optimized

def add_ts_data_partition_tsmadd_optimized_fast(partition: Iterator[Dict], 
                                               redis_config: Dict[str, Any], 
                                               batch_size: int = 10000) -> Iterator[Dict]:
    """
    FASTEST TS.MADD implementation with cycle-aware batching
    Groups by CycleId WITHIN partition to prevent cross-slot errors
    """
    try:
        redis_client = get_redis_client(redis_config)
    except Exception as e:
        logger.error(f"❌ Redis connection failed in partition: {e}")
        raise

    # OPTIMIZATION 3: Group by CycleId within partition (no global shuffle needed)
    cycle_groups = defaultdict(list)
    total_rows = 0
    
    for row in partition:
        cycle_id = row['CycleId']
        tag_id = row['tag_id']
        timestamp = int(row['timestamp'] * 1000)  # Convert to milliseconds
        value = row['value']
        
        if value is not None and not math.isnan(float(value)):
            cycle_groups[cycle_id].append((tag_id, timestamp, float(value)))
            total_rows += 1

    attempted = 0
    successful = 0
    failed = 0
    
    logger.info(f"📊 Processing {total_rows} data points across {len(cycle_groups)} cycles in partition")

    # OPTIMIZATION 4: Process each CycleId group separately (prevents cross-slot errors)
    for cycle_id, data_points in cycle_groups.items():
        try:
            # Process this cycle's data in batches
            for i in range(0, len(data_points), batch_size):
                batch = data_points[i:i + batch_size]
                
                # Prepare TS.MADD arguments for this cycle only
                madd_args = []
                for tag_id, timestamp, value in batch:
                    madd_args.extend([tag_id, timestamp, value])
                
                if madd_args:
                    try:
                        # Execute TS.MADD for this cycle (all same slot)
                        result = redis_client.execute_command('TS.MADD', *madd_args)
                        batch_attempted = len(batch)
                        attempted += batch_attempted
                        
                        # TS.MADD returns array of timestamps or errors
                        if isinstance(result, list):
                            batch_successful = sum(1 for r in result if isinstance(r, int))
                            successful += batch_successful
                            batch_failed = batch_attempted - batch_successful
                            failed += batch_failed
                            
                            if batch_failed > 0:
                                logger.warning(f"⚠️ Cycle {cycle_id} batch had {batch_failed}/{batch_attempted} failures")
                        else:
                            successful += batch_attempted
                            
                    except Exception as batch_error:
                        batch_attempted = len(batch)
                        attempted += batch_attempted
                        failed += batch_attempted
                        logger.error(f"❌ TS.MADD batch failed for cycle_id={cycle_id}: {batch_error}")
                        continue
                        
        except Exception as e:
            cycle_failed = len(data_points)
            attempted += cycle_failed
            failed += cycle_failed
            logger.error(f"❌ Cycle processing failed for cycle_id={cycle_id}: {e}")
            continue

    success_rate = (successful / attempted * 100) if attempted > 0 else 0
    logger.info(f"✅ Partition TS.MADD Summary: {attempted} attempted, {successful} successful, "
               f"{failed} failed, {success_rate:.1f}% success rate")
    
    yield {"attempted": attempted, "successful": successful, "failed": failed, "success_rate": success_rate}

def write_timeseries_data_no_repartition(df_ts: "DataFrame", 
                                        redis_config: Dict[str, Any], 
                                        batch_size: int = 15000,
                                        strategy: str = "no_repartition") -> Dict[str, Any]:
    """
    Write time series data with different partitioning strategies
    
    Args:
        df_ts: DataFrame with time series data
        redis_config: Redis configuration
        batch_size: Batch size for TS.MADD operations
        strategy: "no_repartition", "coalesce_only", or "smart_partition"
    """
    logger.info(f"📊 Starting optimized write with strategy: {strategy}")
    start_time = time.time()
    
    # Apply partitioning strategy
    if strategy == "no_repartition":
        # Use DataFrame as-is, rely on cycle grouping within partitions
        df_final = df_ts
        logger.info("🚀 Using no repartition - fastest startup")
        
    elif strategy == "coalesce_only":
        # Only coalesce to reduce partition count (no shuffle)
        current_partitions = df_ts.rdd.getNumPartitions()
        optimal_partitions = min(current_partitions, max(32, current_partitions // 4))
        df_final = df_ts.coalesce(optimal_partitions)
        logger.info(f"⚡ Coalesced {current_partitions} → {optimal_partitions} partitions")
        
    else:  # smart_partition
        # Analyze and conditionally repartition
        df_final = df_ts  # This would use the smart partitioning logic
        logger.info("🧠 Using smart partitioning")
    
    # Get data count for progress tracking
    total_records = df_final.count()
    logger.info(f"📈 Writing {total_records:,} time series records")
    
    # Execute optimized TS.MADD operations
    results = df_final.rdd.mapPartitions(
        lambda partition: add_ts_data_partition_tsmadd_optimized_fast(partition, redis_config, batch_size)
    ).collect()
    
    # Aggregate results
    total_stats = {
        "total_records": total_records,
        "attempted": sum(r["attempted"] for r in results),
        "successful": sum(r["successful"] for r in results),
        "failed": sum(r["failed"] for r in results),
        "partitions_processed": len(results),
        "strategy_used": strategy
    }
    
    duration = time.time() - start_time
    records_per_second = total_stats["successful"] / duration if duration > 0 else 0
    
    total_stats.update({
        "duration_seconds": duration,
        "records_per_second": records_per_second,
        "success_rate": (total_stats["successful"] / total_stats["attempted"] * 100) if total_stats["attempted"] > 0 else 0
    })
    
    logger.info(f"🏁 Write completed in {duration:.2f}s using {strategy}")
    logger.info(f"📊 Performance: {records_per_second:,.0f} records/second")
    logger.info(f"✅ Success rate: {total_stats['success_rate']:.1f}%")
    
    return total_stats

def main_fast():
    """
    Main execution function with FAST partitioning strategies
    """
    logger.info("🚀 Starting FAST Redis time series processing...")
    
    # Initialize Spark with optimizations
    spark = SparkSession.builder \
        .appName("FastRedisTimeSeriesWriter") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .getOrCreate()
    
    try:
        # STRATEGY 1: No repartition (fastest startup)
        logger.info("=" * 60)
        logger.info("🚀 STRATEGY 1: NO REPARTITION (FASTEST)")
        logger.info("=" * 60)
        
        df_ts = get_optimized_dataframe_no_repartition(spark)
        df_ts.cache()
        
        stats_fast = write_timeseries_data_no_repartition(
            df_ts, 
            redis_config, 
            batch_size=20000,  # Larger batch since we're not repartitioning
            strategy="no_repartition"
        )
        
        logger.info("🎯 NO REPARTITION RESULTS:")
        logger.info(f"   Throughput: {stats_fast['records_per_second']:,.0f} records/second")
        logger.info(f"   Duration: {stats_fast['duration_seconds']:.2f} seconds")
        logger.info(f"   Success rate: {stats_fast['success_rate']:.1f}%")
        
        # Uncomment to test other strategies
        """
        # STRATEGY 2: Coalesce only (faster than repartition)
        logger.info("=" * 60)
        logger.info("⚡ STRATEGY 2: COALESCE ONLY")
        logger.info("=" * 60)
        
        stats_coalesce = write_timeseries_data_no_repartition(
            df_ts, 
            redis_config, 
            batch_size=15000,
            strategy="coalesce_only"
        )
        
        # STRATEGY 3: Smart partitioning (conditional repartition)
        logger.info("=" * 60)
        logger.info("🧠 STRATEGY 3: SMART PARTITIONING")
        logger.info("=" * 60)
        
        df_ts_smart = get_optimized_dataframe_smart_partition(spark)
        df_ts_smart.cache()
        
        stats_smart = write_timeseries_data_no_repartition(
            df_ts_smart, 
            redis_config, 
            batch_size=15000,
            strategy="smart_partition"
        )
        
        # Compare strategies
        logger.info("=" * 80)
        logger.info("📊 STRATEGY COMPARISON")
        logger.info("=" * 80)
        logger.info(f"No Repartition:  {stats_fast['records_per_second']:8,.0f} records/sec in {stats_fast['duration_seconds']:6.1f}s")
        logger.info(f"Coalesce Only:   {stats_coalesce['records_per_second']:8,.0f} records/sec in {stats_coalesce['duration_seconds']:6.1f}s")
        logger.info(f"Smart Partition: {stats_smart['records_per_second']:8,.0f} records/sec in {stats_smart['duration_seconds']:6.1f}s")
        """
        
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main_fast()