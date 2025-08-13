"""
Redis TimeSeries operations for key management and data writing.
"""
import redis
from typing import Dict, Any, List, Tuple, Set
from ..utils.logging_config import get_default_logger

logger = get_default_logger(__name__)


class RedisTimeSeriesOperations:
    """Handles Redis TimeSeries operations including key creation and data writing."""
    
    def __init__(self, redis_client: redis.Redis):
        """
        Initialize with a Redis client.
        
        Args:
            redis_client: Redis client instance
        """
        self.redis_client = redis_client
    
    def create_timeseries_keys(self, tag_ids: Set[str], **ts_config) -> Dict[str, int]:
        """
        Create TimeSeries keys for the given tag IDs.
        
        Args:
            tag_ids: Set of tag IDs to create keys for
            **ts_config: TimeSeries configuration (retention, chunk_size, duplicate_policy)
        
        Returns:
            Dictionary with creation statistics
        """
        if not tag_ids:
            return {"keys_attempted": 0, "keys_created": 0, "keys_already_exists": 0, "keys_failed": 0}
        
        # Default TimeSeries configuration
        retention = ts_config.get("retention", 2592000000)  # 30 days in milliseconds
        chunk_size = ts_config.get("chunk_size", 4096)
        duplicate_policy = ts_config.get("duplicate_policy", "LAST")
        
        attempted = created = already_exists = failed = 0
        
        # Check existing keys
        pipeline = self.redis_client.pipeline()
        for tag_id in tag_ids:
            pipeline.exists(tag_id)
        exists_results = pipeline.execute()
        
        existing_keys = [tag for tag, exists in zip(tag_ids, exists_results) if exists]
        missing_keys = [tag for tag, exists in zip(tag_ids, exists_results) if not exists]
        
        already_exists = len(existing_keys)
        attempted = len(missing_keys)
        
        logger.info(f"Found {already_exists} existing keys, creating {attempted} new keys")
        
        if missing_keys:
            pipeline = self.redis_client.pipeline()
            for tag_id in missing_keys:
                pipeline.execute_command(
                    "TS.CREATE", tag_id,
                    "RETENTION", retention,
                    "CHUNK_SIZE", chunk_size,
                    "DUPLICATE_POLICY", duplicate_policy
                )
            
            results = pipeline.execute()
            for result in results:
                if result == b'OK' or result == 'OK':
                    created += 1
                else:
                    failed += 1
                    logger.warning(f"Failed to create key: {result}")
        
        stats = {
            "keys_attempted": attempted,
            "keys_created": created,
            "keys_already_exists": already_exists,
            "keys_failed": failed
        }
        
        logger.info(f"Key creation stats: {stats}")
        return stats
    
    def write_timeseries_data(self, data_points: List[Tuple[str, int, float]], batch_size: int = 5000) -> Dict[str, Any]:
        """
        Write data points to Redis TimeSeries using batched operations.
        
        Args:
            data_points: List of (tag_id, timestamp, value) tuples
            batch_size: Number of data points to write in each batch
        
        Returns:
            Dictionary with write statistics
        """
        attempted = successful = failed = 0
        keys_created_during_write = 0
        
        if not data_points:
            return {
                "records_attempted": 0,
                "records_successful": 0,
                "records_failed": 0,
                "keys_created_during_write": 0,
                "success_rate": 0
            }
        
        logger.info(f"Writing {len(data_points)} data points in batches of {batch_size}")
        
        for i in range(0, len(data_points), batch_size):
            batch = data_points[i:i + batch_size]
            batch_stats = self._write_batch(batch)
            
            attempted += batch_stats["attempted"]
            successful += batch_stats["successful"]
            failed += batch_stats["failed"]
            keys_created_during_write += batch_stats["keys_created"]
        
        success_rate = (successful / attempted * 100) if attempted else 0
        
        stats = {
            "records_attempted": attempted,
            "records_successful": successful,
            "records_failed": failed,
            "keys_created_during_write": keys_created_during_write,
            "success_rate": success_rate
        }
        
        logger.info(f"Write stats: {stats}")
        return stats
    
    def _write_batch(self, batch: List[Tuple[str, int, float]]) -> Dict[str, int]:
        """
        Write a single batch of data points.
        
        Args:
            batch: List of (tag_id, timestamp, value) tuples
        
        Returns:
            Dictionary with batch write statistics
        """
        attempted = successful = failed = keys_created = 0
        
        # Prepare MADD arguments
        madd_args = []
        for tag_id, timestamp, value in batch:
            madd_args.extend([tag_id, timestamp, value])
        
        try:
            result = self.redis_client.execute_command("TS.MADD", *madd_args)
            batch_attempted = len(batch)
            attempted += batch_attempted
            
            if isinstance(result, list):
                batch_successful = sum(1 for r in result if isinstance(r, int))
                successful += batch_successful
                failed += (batch_attempted - batch_successful)
            else:
                successful += batch_attempted
                
        except redis.exceptions.ResponseError as e:
            # Handle missing keys by creating them and retrying individually
            if "does not exist" in str(e):
                logger.warning(f"Some keys missing, creating and retrying individually: {e}")
                individual_stats = self._handle_missing_keys(batch)
                attempted += individual_stats["attempted"]
                successful += individual_stats["successful"]
                failed += individual_stats["failed"]
                keys_created += individual_stats["keys_created"]
            else:
                logger.error(f"TS.MADD batch failed: {e}")
                attempted += len(batch)
                failed += len(batch)
        
        return {
            "attempted": attempted,
            "successful": successful,
            "failed": failed,
            "keys_created": keys_created
        }
    
    def _handle_missing_keys(self, batch: List[Tuple[str, int, float]]) -> Dict[str, int]:
        """
        Handle missing keys by creating them and writing data individually.
        
        Args:
            batch: List of (tag_id, timestamp, value) tuples
        
        Returns:
            Dictionary with individual write statistics
        """
        attempted = successful = failed = keys_created = 0
        
        for tag_id, timestamp, value in batch:
            # Create key if it doesn't exist
            if not self.redis_client.exists(tag_id):
                try:
                    self.redis_client.execute_command(
                        "TS.CREATE", tag_id,
                        "RETENTION", 2592000000,
                        "CHUNK_SIZE", 4096,
                        "DUPLICATE_POLICY", "LAST"
                    )
                    keys_created += 1
                except Exception as create_error:
                    logger.error(f"Failed to create key {tag_id}: {create_error}")
            
            # Write individual data point
            try:
                self.redis_client.execute_command("TS.ADD", tag_id, timestamp, value)
                successful += 1
            except Exception as write_error:
                logger.error(f"Failed to write data point for {tag_id}: {write_error}")
                failed += 1
            
            attempted += 1
        
        return {
            "attempted": attempted,
            "successful": successful,
            "failed": failed,
            "keys_created": keys_created
        }


def create_keys_for_cycle(rows, redis_client) -> Dict[str, int]:
    """
    Legacy function for backward compatibility.
    Create TimeSeries keys for a cycle's data.
    """
    ts_ops = RedisTimeSeriesOperations(redis_client)
    tag_ids = {row.tag_id for row in rows if row.tag_id is not None}
    return ts_ops.create_timeseries_keys(tag_ids)


def write_data_for_cycle(rows, redis_client, batch_size=5000) -> Dict[str, int]:
    """
    Legacy function for backward compatibility.
    Write data for a cycle to Redis TimeSeries.
    """
    ts_ops = RedisTimeSeriesOperations(redis_client)
    data_points = [
        (row.tag_id, row.timestamp, float(row.value))
        for row in rows if row.value is not None
    ]
    return ts_ops.write_timeseries_data(data_points, batch_size)