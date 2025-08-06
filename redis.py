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

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

redis_config = {
    "host": 'rc-cvdev-03.westeurope.redis.azure.net',
    "port": 10000,
    "password": '',
    "ssl": True,
}

redis_pool = None

def get_redis_client(redis_config):
    global redis_pool
    if redis_pool is None:
        redis_pool = redis.ConnectionPool(
            host=redis_config["host"],
            port=redis_config["port"],
            password=redis_config["password"],
            connection_class=redis.SSLConnection
        )
    return redis.StrictRedis(connection_pool=redis_pool, decode_responses=True)

#utility_df = spark.table("clearview_dev.utility.redisconfig")
#processed_time = utility_df.select(col("ProcessedTime")).collect()[0][0]
#print(processed_time)

df = spark.table("clearview_prod.silver.measurements_new")
#.filter(col("_ProcessedTime") >= processed_time)
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
    
#df_ts.cache()

distinct_keys_df = df_ts.select("tag_id","CycleId").dropna(how='any').dropDuplicates(["tag_id"]).cache()

def create_keys_partition(partition, redis_config):
    """
    Function to be executed on each Spark partition.
    Groups tag_ids by cycle_id and creates Redis TS keys using a pipeline.
    """
    try:
        redis_client = get_redis_client(redis_config)
    except Exception as e:
        logging.error(f"❌ Redis connection failed in partition: {e}")
        raise

    # Group tag_ids by cycle_id
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
                    "DUPLICATE_POLICY", "last"
                )
                attempted += 1

            pipeline.execute()
        except Exception as e:
            failed += len(tag_ids)
            for tag_id in tag_ids:
                failed_keys[tag_id] = str(e)
            logging.error(f"❌ Pipeline execution failed for cycle_id={cycle_id}: {e}")
            raise

    logging.info(f"✅ Partition Summary: Attempted={attempted}, Failed={failed}")
    if failed_keys:
        for tag_id, error in failed_keys.items():
            logging.error(f"❌ Failed TS.CREATE for {tag_id}: {error}")
            raise

#distinct_keys_df.foreachPartition(lambda partition: create_keys_partition(partition, redis_config))


def add_ts_data_partition_with_batched_pipeline(partition, redis_config, batch_size=500):
    """
    Sends TS.ADD commands to Redis in batched pipelines, grouped by CycleId.
    Flushes pipeline after 'batch_size' commands to control memory/network usage.
    """
    from collections import defaultdict

    try:
        redis_client = get_redis_client(redis_config)
    except Exception as e:
        logging.error(f"❌ Redis connection failed in partition: {e}")
        raise

    # Group data: CycleId -> tag_id -> [(timestamp, value), ...]
    grouped_data = defaultdict(lambda: defaultdict(list))
    for row in partition:
        cycle_id = row['CycleId']
        tag_id = row['tag_id']
        timestamp = row['timestamp']
        value = row['value'] if isinstance(row['value'], (int, float)) else None
        if value is not None:
            grouped_data[cycle_id][tag_id].append((timestamp, value))

    attempted = 0
    failed = 0

    for cycle_id, tag_map in grouped_data.items():
        pipeline = redis_client.pipeline()
        command_count = 0

        try:
            for tag_id, points in tag_map.items():
                for ts, val in points:
                    pipeline.execute_command("TS.ADD", tag_id, ts, val)
                    command_count += 1
                    attempted += 1

                    # Flush pipeline when batch_size is reached
                    if command_count >= batch_size:
                        pipeline.execute()
                        pipeline = redis_client.pipeline()
                        command_count = 0

            # Flush any remaining commands
            if command_count > 0:
                pipeline.execute()

        except Exception as e:
            failed += 1
            logging.error(f"❌ Redis pipeline failed for cycle_id={cycle_id}: {e}")
            raise

    logging.info(f"✅ TS.ADD Partition Summary: Attempted={attempted}, Failed={failed}")

df_ts.foreachPartition(
    lambda partition: add_ts_data_partition_with_batched_pipeline(partition, redis_config, batch_size=1000)
)


