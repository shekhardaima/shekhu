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
)
import logging
import time
import sys
from collections import defaultdict
import math
import threading

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

def get_redis_client(redis_config):
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

# Your original data processing (UNCHANGED)
df = spark.table("clearview_prod.silver.measurements_new")
df = df.select(
    "CycleId",
    "CustomerTag",
    "StartTimestamp",
    "Value",
    "_ProcessedTime",
)

aggregated_df = df.groupBy(
    "CycleId",
    "CustomerTag",
    floor(unix_timestamp("StartTimestamp") / 900).alias("IntervalStartUnix"),
).agg(
    from_unixtime(floor(unix_timestamp("StartTimestamp") / 900) * 900).alias("StartTimestamp"),
    avg("Value").alias("Value"),
    max("_ProcessedTime").alias("_ProcessedTime"),
)

df_ts = aggregated_df.select(
    concat(lit("{"), col("CycleId"), lit("}"), col("CustomerTag")).alias("tag_id"),
    col("IntervalStartUnix").alias("timestamp"),
    col("Value").alias("value"),
    col("CycleId")
)

# 🚀 FIX: NO REPARTITIONING - Use DataFrame as-is
logger.info("🚀 FIXED: Skipping repartition to avoid performance bottleneck")
logger.info(f"📊 Using {df_ts.rdd.getNumPartitions()} partitions as-is")

# Cache for better performance
df_ts.cache()

distinct_keys_df = df_ts.select("tag_id", "CycleId").dropna(how='any').dropDuplicates(["tag_id"]).cache()

def create_keys_partition(partition, redis_config):
    """
    IMPROVED: Your original key creation function with better error handling
    """
    try:
        redis_client = get_redis_client(redis_config)
    except Exception as e:
        logging.error(f"❌ Redis connection failed in partition: {e}")
        raise

    # Group tag_ids by cycle_id (same as original)
    cycle_to_tags = defaultdict(set)
    for row in partition:
        tag_id = row['tag_id']
        cycle_id = row['CycleId']
        cycle_to_tags[cycle_id].add(tag_id)

    attempted = 0
    failed = 0
    failed_keys = {}

    for cycle_id, tag_ids in cycle_to_tags.items():
        try:
            pipeline = redis_client.pipeline()
            for tag_id in tag_ids:
                pipeline.execute_command(
                    "TS.CREATE", tag_id,
                    "RETENTION", 2592000000,  # 30 days retention
                    "CHUNK_SIZE", 4096,       # Optimized chunk size
                    "DUPLICATE_POLICY", "LAST"  # Handle duplicates
                )
                attempted += 1

            pipeline.execute()
        except Exception as e:
            error_msg = str(e)
            if "key already exists" in error_msg.lower():
                logging.debug(f"ℹ️ Keys already exist for cycle_id={cycle_id}")
                continue  # This is fine, keys already exist
            else:
                failed += len(tag_ids)
                for tag_id in tag_ids:
                    failed_keys[tag_id] = str(e)
                logging.error(f"❌ Pipeline execution failed for cycle_id={cycle_id}: {e}")
                # Don't raise, continue with other cycles
                continue

    logging.info(f"✅ Partition Summary: Attempted={attempted}, Failed={failed}")

def add_ts_data_partition_with_tsmadd_optimized(partition, redis_config, batch_size=10000):
    """
    🚀 OPTIMIZED: Uses TS.MADD for bulk operations while maintaining CycleId grouping
    """
    try:
        redis_client = get_redis_client(redis_config)
    except Exception as e:
        logging.error(f"❌ Redis connection failed in partition: {e}")
        raise

    # Group data by CycleId WITHIN partition (prevents cross-slot errors)
    cycle_groups = defaultdict(list)
    total_rows = 0
    
    for row in partition:
        cycle_id = row['CycleId']
        tag_id = row['tag_id']
        timestamp = int(row['timestamp'] * 1000)  # Convert to milliseconds
        value = row['value'] if isinstance(row['value'], (int, float)) else None
        
        if value is not None and not math.isnan(float(value)):
            cycle_groups[cycle_id].append((tag_id, timestamp, float(value)))
            total_rows += 1

    attempted = 0
    successful = 0
    failed = 0

    logging.info(f"📊 Processing {total_rows} data points across {len(cycle_groups)} cycles")

    # Process each CycleId group separately (prevents cross-slot errors)
    for cycle_id, data_points in cycle_groups.items():
        try:
            # Process this cycle's data in batches using TS.MADD
            for i in range(0, len(data_points), batch_size):
                batch = data_points[i:i + batch_size]
                
                # Prepare TS.MADD arguments for this cycle
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
                        else:
                            successful += batch_attempted
                            
                    except Exception as batch_error:
                        batch_attempted = len(batch)
                        attempted += batch_attempted
                        failed += batch_attempted
                        logging.error(f"❌ TS.MADD batch failed for cycle_id={cycle_id}: {batch_error}")
                        continue
                        
        except Exception as e:
            cycle_failed = len(data_points)
            attempted += cycle_failed
            failed += cycle_failed
            logging.error(f"❌ Cycle processing failed for cycle_id={cycle_id}: {e}")
            continue

    success_rate = (successful / attempted * 100) if attempted > 0 else 0
    logging.info(f"✅ TS.MADD Summary: Attempted={attempted}, Successful={successful}, "
               f"Failed={failed}, Success Rate={success_rate:.1f}%")

# 🚀 MAIN EXECUTION - OPTIMIZED VERSION
logger.info("🚀 Starting FIXED Redis processing (no repartition bottleneck)...")

# Step 1: Create keys (uncomment if needed)
# distinct_keys_df.foreachPartition(lambda partition: create_keys_partition(partition, redis_config))

# Step 2: Write time series data using optimized TS.MADD
start_time = time.time()

df_ts.foreachPartition(
    lambda partition: add_ts_data_partition_with_tsmadd_optimized(
        partition, redis_config, batch_size=15000  # Larger batch size for better performance
    )
)

end_time = time.time()
duration = end_time - start_time

# Performance summary
total_records = df_ts.count()
records_per_second = total_records / duration if duration > 0 else 0

logger.info("=" * 80)
logger.info("🎯 FIXED VERSION RESULTS")
logger.info("=" * 80)
logger.info(f"📊 Total records processed: {total_records:,}")
logger.info(f"⏱️ Duration: {duration:.2f} seconds")
logger.info(f"⚡ Throughput: {records_per_second:,.0f} records/second")
logger.info(f"🚀 NO repartition overhead - immediate startup!")
logger.info("=" * 80)