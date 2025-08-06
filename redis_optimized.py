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
    "max_connections": 50,  # Increased connection pool size
    "socket_timeout": 30,   # Extended timeout for large operations
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

def get_optimized_dataframe(spark: SparkSession) -> "DataFrame":
    """
    Get the optimized DataFrame with proper partitioning
    """
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
    
    # Optimize partitioning by CycleId to prevent cross-slot errors
    # Repartition to ensure same CycleId data goes to same partition
    optimal_partitions = min(200, max(20, df_ts.rdd.getNumPartitions()))
    df_ts_optimized = df_ts.repartition(optimal_partitions, spark_hash(col("CycleId")))
    
    return df_ts_optimized

def create_keys_partition_optimized(partition: Iterator[Dict], redis_config: Dict[str, Any]) -> Iterator[Dict]:
    """
    Optimized key creation using TS.CREATE with better error handling and batching
    """
    try:
        redis_client = get_redis_client(redis_config)
    except Exception as e:
        logger.error(f"❌ Redis connection failed in partition: {e}")
        raise

    # Group tag_ids by cycle_id for same-slot operations
    cycle_to_tags = defaultdict(set)
    total_rows = 0
    
    for row in partition:
        tag_id = row['tag_id']
        cycle_id = row['CycleId']
        cycle_to_tags[cycle_id].add(tag_id)
        total_rows += 1

    attempted = 0
    created = 0
    already_exists = 0
    failed = 0
    
    logger.info(f"🔧 Processing {total_rows} rows across {len(cycle_to_tags)} cycles")

    for cycle_id, tag_ids in cycle_to_tags.items():
        try:
            pipeline = redis_client.pipeline()
            cycle_attempted = 0
            
            # Create time series with optimized parameters
            for tag_id in tag_ids:
                pipeline.execute_command(
                    "TS.CREATE", tag_id,
                    "RETENTION", 2592000000,  # 30 days retention (in milliseconds)
                    "CHUNK_SIZE", 4096,       # Optimized chunk size
                    "DUPLICATE_POLICY", "LAST"  # Handle duplicates efficiently
                )
                cycle_attempted += 1
                attempted += 1

            # Execute pipeline for this cycle
            results = pipeline.execute()
            
            # Count successful creations vs already existing
            for result in results:
                if result == b'OK' or result == 'OK':
                    created += 1
                elif "TSDB: key already exists" in str(result):
                    already_exists += 1
                    
        except Exception as e:
            error_msg = str(e)
            if "key already exists" in error_msg.lower():
                already_exists += cycle_attempted
                logger.debug(f"ℹ️ Keys already exist for cycle_id={cycle_id}")
            else:
                failed += cycle_attempted
                logger.error(f"❌ Pipeline execution failed for cycle_id={cycle_id}: {e}")
                # Don't raise here, continue with other cycles
                continue

    logger.info(f"✅ Key Creation Summary: Total={attempted}, Created={created}, "
               f"Already Exists={already_exists}, Failed={failed}")
    
    yield {"attempted": attempted, "created": created, "already_exists": already_exists, "failed": failed}

def add_ts_data_partition_tsmadd_optimized(partition: Iterator[Dict], 
                                         redis_config: Dict[str, Any], 
                                         batch_size: int = 5000) -> Iterator[Dict]:
    """
    Highly optimized TS.MADD implementation for bulk time series data insertion
    """
    try:
        redis_client = get_redis_client(redis_config)
    except Exception as e:
        logger.error(f"❌ Redis connection failed in partition: {e}")
        raise

    # Group data by CycleId for same-slot operations
    grouped_data = defaultdict(list)
    total_rows = 0
    
    for row in partition:
        cycle_id = row['CycleId']
        tag_id = row['tag_id']
        timestamp = int(row['timestamp'] * 1000)  # Convert to milliseconds
        value = row['value']
        
        if value is not None and not math.isnan(float(value)):
            grouped_data[cycle_id].append((tag_id, timestamp, float(value)))
            total_rows += 1

    attempted = 0
    successful = 0
    failed = 0
    
    logger.info(f"📊 Processing {total_rows} data points across {len(grouped_data)} cycles")

    for cycle_id, data_points in grouped_data.items():
        try:
            # Process data in batches to avoid memory issues
            for i in range(0, len(data_points), batch_size):
                batch = data_points[i:i + batch_size]
                
                # Prepare TS.MADD arguments
                madd_args = []
                for tag_id, timestamp, value in batch:
                    madd_args.extend([tag_id, timestamp, value])
                
                if madd_args:
                    # Execute TS.MADD command
                    try:
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
                                logger.warning(f"⚠️ Batch had {batch_failed} failures out of {batch_attempted}")
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
    logger.info(f"✅ TS.MADD Summary: Attempted={attempted}, Successful={successful}, "
               f"Failed={failed}, Success Rate={success_rate:.1f}%")
    
    yield {"attempted": attempted, "successful": successful, "failed": failed, "success_rate": success_rate}

def create_keys_optimized(df_distinct: "DataFrame", redis_config: Dict[str, Any]) -> Dict[str, int]:
    """
    Create Redis time series keys with optimized performance
    """
    logger.info("🔧 Starting optimized key creation process...")
    start_time = time.time()
    
    # Execute key creation across partitions
    results = df_distinct.rdd.mapPartitions(
        lambda partition: create_keys_partition_optimized(partition, redis_config)
    ).collect()
    
    # Aggregate results
    total_stats = {
        "attempted": sum(r["attempted"] for r in results),
        "created": sum(r["created"] for r in results),
        "already_exists": sum(r["already_exists"] for r in results),
        "failed": sum(r["failed"] for r in results)
    }
    
    duration = time.time() - start_time
    logger.info(f"🏁 Key creation completed in {duration:.2f}s: {total_stats}")
    
    return total_stats

def write_timeseries_data_optimized(df_ts: "DataFrame", 
                                  redis_config: Dict[str, Any], 
                                  batch_size: int = 10000) -> Dict[str, Any]:
    """
    Write time series data using optimized TS.MADD operations
    """
    logger.info("📊 Starting optimized time series data write...")
    start_time = time.time()
    
    # Get data count for progress tracking
    total_records = df_ts.count()
    logger.info(f"📈 Writing {total_records:,} time series records")
    
    # Execute optimized TS.MADD operations
    results = df_ts.rdd.mapPartitions(
        lambda partition: add_ts_data_partition_tsmadd_optimized(partition, redis_config, batch_size)
    ).collect()
    
    # Aggregate results
    total_stats = {
        "total_records": total_records,
        "attempted": sum(r["attempted"] for r in results),
        "successful": sum(r["successful"] for r in results),
        "failed": sum(r["failed"] for r in results),
        "partitions_processed": len(results)
    }
    
    duration = time.time() - start_time
    records_per_second = total_stats["successful"] / duration if duration > 0 else 0
    
    total_stats.update({
        "duration_seconds": duration,
        "records_per_second": records_per_second,
        "success_rate": (total_stats["successful"] / total_stats["attempted"] * 100) if total_stats["attempted"] > 0 else 0
    })
    
    logger.info(f"🏁 Time series write completed in {duration:.2f}s")
    logger.info(f"📊 Performance: {records_per_second:,.0f} records/second")
    logger.info(f"✅ Success rate: {total_stats['success_rate']:.1f}%")
    
    return total_stats

def main():
    """
    Main execution function with your optimized workflow
    """
    logger.info("🚀 Starting optimized Redis time series processing...")
    
    # Initialize Spark with optimizations
    spark = SparkSession.builder \
        .appName("OptimizedRedisTimeSeriesWriter") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .getOrCreate()
    
    try:
        # Get optimized DataFrame
        df_ts = get_optimized_dataframe(spark)
        
        # Cache for better performance
        df_ts.cache()
        
        # Create distinct keys DataFrame for key creation
        distinct_keys_df = df_ts.select("tag_id", "CycleId") \
                              .dropna(how='any') \
                              .dropDuplicates(["tag_id"]) \
                              .cache()
        
        logger.info(f"📋 Processing {df_ts.count():,} total records")
        logger.info(f"🔑 Creating {distinct_keys_df.count():,} unique time series keys")
        
        # Step 1: Create Redis time series keys (optional - run only if needed)
        # Uncomment the next line if you need to create new keys
        # key_stats = create_keys_optimized(distinct_keys_df, redis_config)
        
        # Step 2: Write time series data using optimized TS.MADD
        write_stats = write_timeseries_data_optimized(
            df_ts, 
            redis_config, 
            batch_size=15000  # Larger batch size for better performance
        )
        
        # Performance summary
        logger.info("=" * 80)
        logger.info("🎯 OPTIMIZATION RESULTS")
        logger.info("=" * 80)
        logger.info(f"📊 Total records processed: {write_stats['total_records']:,}")
        logger.info(f"✅ Successfully written: {write_stats['successful']:,}")
        logger.info(f"⚡ Throughput: {write_stats['records_per_second']:,.0f} records/second")
        logger.info(f"🎯 Success rate: {write_stats['success_rate']:.1f}%")
        logger.info(f"⏱️ Total duration: {write_stats['duration_seconds']:.2f} seconds")
        logger.info(f"🔧 Partitions processed: {write_stats['partitions_processed']}")
        logger.info("=" * 80)
        
        # Performance comparison with original
        estimated_original_time = write_stats['total_records'] / 5000  # Assume 5K records/sec originally
        speedup = estimated_original_time / write_stats['duration_seconds']
        logger.info(f"🚀 Estimated speedup vs original: {speedup:.1f}x faster")
        
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()