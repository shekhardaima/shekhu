"""
Data processing operations for handling partitions and cycle-based data processing.
"""
import time
from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from ..redis.connection import get_redis_client
from ..redis.timeseries_operations import create_keys_for_cycle, write_data_for_cycle
from ..utils.logging_config import get_default_logger

logger = get_default_logger(__name__)


class DataProcessor:
    """Handles data processing operations for cycles and partitions."""
    
    def __init__(self, redis_config: Dict[str, Any], batch_size: int = 5000):
        """
        Initialize the data processor.
        
        Args:
            redis_config: Redis configuration dictionary
            batch_size: Batch size for data processing
        """
        self.redis_config = redis_config
        self.batch_size = batch_size
        logger.info(f"DataProcessor initialized with batch_size={batch_size}")
    
    def process_partition_for_cycle(self, partition_iter, redis_cfg: Dict[str, Any], batch_size: int) -> Dict[str, Any]:
        """
        Process a partition of data for a specific cycle.
        
        Args:
            partition_iter: Iterator over partition data
            redis_cfg: Redis configuration
            batch_size: Batch size for processing
        
        Returns:
            Dictionary with processing statistics
        """
        redis_client = get_redis_client(redis_cfg)
        rows = list(partition_iter)
        
        if not rows:
            return {
                "keys_attempted": 0, "keys_created": 0, "keys_already_exists": 0, "keys_failed": 0,
                "records_attempted": 0, "records_successful": 0, "records_failed": 0,
                "keys_created_during_write": 0, "success_rate": 0
            }
        
        # Create TimeSeries keys
        key_stats = create_keys_for_cycle(rows, redis_client)
        
        # Write data
        data_stats = write_data_for_cycle(rows, redis_client, batch_size)
        
        # Combine statistics
        combined_stats = {**key_stats, **data_stats}
        logger.debug(f"Partition processed: {len(rows)} rows, stats: {combined_stats}")
        
        return combined_stats
    
    def process_single_cycle(self, cycle_df: DataFrame, cycle_id: str) -> Dict[str, Any]:
        """
        Process a single cycle's data.
        
        Args:
            cycle_df: DataFrame containing cycle data
            cycle_id: Cycle identifier
        
        Returns:
            Dictionary with processing statistics
        """
        start_time = time.time()
        logger.info(f"▶ Processing CycleId={cycle_id}")
        
        partition_results = []
        
        def process_and_collect(partition):
            """Closure to process partition and collect results."""
            stats = self.process_partition_for_cycle(partition, self.redis_config, self.batch_size)
            partition_results.append(stats)
        
        # Process all partitions for this cycle
        cycle_df.foreachPartition(process_and_collect)
        
        # Aggregate partition statistics
        aggregated_stats = self._aggregate_partition_stats(partition_results)
        
        elapsed = time.time() - start_time
        aggregated_stats["duration_sec"] = elapsed
        aggregated_stats["cycle_id"] = cycle_id
        
        logger.info(f"⏱ Cycle {cycle_id} took {elapsed:.2f}s — {aggregated_stats}")
        return aggregated_stats
    
    def process_all_cycles(self, df_ts: DataFrame) -> List[Dict[str, Any]]:
        """
        Process all cycles in the DataFrame.
        
        Args:
            df_ts: DataFrame containing time series data
        
        Returns:
            List of processing statistics for each cycle
        """
        # Get all unique cycles
        all_cycles = [row.CycleId for row in df_ts.select("CycleId").distinct().collect()]
        logger.info(f"Found {len(all_cycles)} unique cycles to process")
        
        overall_stats = []
        
        for cycle_id in all_cycles:
            # Filter data for current cycle
            cycle_df = df_ts.filter(col("CycleId") == cycle_id)
            
            # Process the cycle
            cycle_stats = self.process_single_cycle(cycle_df, cycle_id)
            overall_stats.append(cycle_stats)
        
        logger.info("=== PROCESSING COMPLETE ===")
        self._log_final_summary(overall_stats)
        
        return overall_stats
    
    def _aggregate_partition_stats(self, partition_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Aggregate statistics from multiple partitions.
        
        Args:
            partition_results: List of partition statistics
        
        Returns:
            Aggregated statistics dictionary
        """
        if not partition_results:
            return {
                "keys_attempted": 0, "keys_created": 0, "keys_already_exists": 0, "keys_failed": 0,
                "records_attempted": 0, "records_successful": 0, "records_failed": 0,
                "keys_created_during_write": 0, "success_rate": 0
            }
        
        aggregated = {}
        
        # Sum up all numeric statistics
        for stats in partition_results:
            for key, value in stats.items():
                if isinstance(value, (int, float)):
                    aggregated[key] = aggregated.get(key, 0) + value
        
        # Recalculate success rate
        if aggregated.get("records_attempted", 0) > 0:
            aggregated["success_rate"] = (
                aggregated.get("records_successful", 0) / aggregated["records_attempted"] * 100
            )
        else:
            aggregated["success_rate"] = 0
        
        return aggregated
    
    def _log_final_summary(self, overall_stats: List[Dict[str, Any]]):
        """
        Log final summary of all processing statistics.
        
        Args:
            overall_stats: List of cycle processing statistics
        """
        total_cycles = len(overall_stats)
        total_duration = sum(stat.get("duration_sec", 0) for stat in overall_stats)
        total_records = sum(stat.get("records_attempted", 0) for stat in overall_stats)
        total_successful = sum(stat.get("records_successful", 0) for stat in overall_stats)
        
        overall_success_rate = (total_successful / total_records * 100) if total_records > 0 else 0
        
        logger.info("=== FINAL SUMMARY ===")
        logger.info(f"Total cycles processed: {total_cycles}")
        logger.info(f"Total duration: {total_duration:.2f}s")
        logger.info(f"Total records attempted: {total_records}")
        logger.info(f"Total records successful: {total_successful}")
        logger.info(f"Overall success rate: {overall_success_rate:.2f}%")
        
        for stat in overall_stats:
            logger.info(f"Cycle {stat.get('cycle_id')}: {stat}")


def process_partition_for_cycle(partition_iter, redis_cfg: Dict[str, Any], batch_size: int) -> Dict[str, Any]:
    """
    Legacy function for backward compatibility.
    Process a partition of data for a specific cycle.
    """
    processor = DataProcessor(redis_cfg, batch_size)
    return processor.process_partition_for_cycle(partition_iter, redis_cfg, batch_size)


def process_all_cycles_no_pandas_foreach(df_ts: DataFrame, batch_size: int = 5000) -> List[Dict[str, Any]]:
    """
    Legacy function for backward compatibility.
    Process all cycles in the DataFrame without pandas.
    """
    # This function needs redis_config, but it's not passed in the original
    # We'll need to import it or pass it as a parameter
    from ..config.redis_config import get_redis_config
    
    redis_cfg = get_redis_config()
    processor = DataProcessor(redis_cfg, batch_size)
    return processor.process_all_cycles(df_ts)