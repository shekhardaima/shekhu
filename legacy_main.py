"""
Legacy main script for backward compatibility with the original code structure.
This module provides the same interface as the original monolithic script.
Optimized for Databricks runtime 14.3 and Delta Lake.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time
from typing import Dict, Any

# Import from the new modular structure
from src.config.redis_config import get_redis_config
from src.config.databricks_config import create_databricks_spark_session
from src.data.spark_operations import get_calculations_df
from src.data.processing import process_partition_for_cycle
from src.utils.logging_config import get_default_logger

# Set up logging
logger = get_default_logger(__name__)

# Redis connection settings (for backward compatibility)
redis_config = get_redis_config()


def process_all_cycles_no_pandas_foreach(df_ts, batch_size=5000):
    """
    Process all cycles in the DataFrame without pandas (legacy function).
    
    Args:
        df_ts: DataFrame containing time series data
        batch_size: Batch size for processing
    
    Returns:
        List of processing statistics for each cycle
    """
    all_cycles = [row.CycleId for row in df_ts.select("CycleId").distinct().collect()]
    logger.info(f"Found {len(all_cycles)} unique cycles")

    overall_stats = []
    for cycle_id in all_cycles:
        start_time = time.time()
        logger.info(f"▶ Processing CycleId={cycle_id}")

        cycle_df = df_ts.filter(col("CycleId") == cycle_id)

        partition_results = []
        def process_and_collect(partition):
            stats = process_partition_for_cycle(partition, redis_config, batch_size)
            partition_results.append(stats)

        cycle_df.foreachPartition(process_and_collect)

        # Aggregate partition stats
        agg_stats = {}
        for stats in partition_results:
            for k, v in stats.items():
                agg_stats[k] = agg_stats.get(k, 0) + v

        elapsed = time.time() - start_time
        logger.info(f"⏱ Cycle {cycle_id} took {elapsed:.2f}s — {agg_stats}")

        overall_stats.append({"cycle_id": cycle_id, **agg_stats, "duration_sec": elapsed})

    return overall_stats


def main():
    """Main function that replicates the original script behavior for Databricks."""
    # Use Databricks-optimized Spark session
    spark = create_databricks_spark_session("LegacyDataProcessing")
    try:
        df_ts = get_calculations_df(spark).cache()
        stats = process_all_cycles_no_pandas_foreach(df_ts, batch_size=5000)

        logger.info("=== FINAL SUMMARY ===")
        for stat in stats:
            logger.info(stat)
    finally:
        # Only stop if not running in Databricks managed environment
        import os
        if not os.getenv("DATABRICKS_RUNTIME_VERSION"):
            spark.stop()


if __name__ == "__main__":
    main()