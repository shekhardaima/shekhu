"""
Spark operations for data extraction and transformation from Databricks Delta tables.
Optimized for Databricks runtime 14.3 and Delta Lake.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, floor, unix_timestamp, concat, lit, avg, from_unixtime, max, count
)
from typing import Optional
from ..utils.logging_config import get_default_logger
from ..config.databricks_config import DatabricksSparkManager, optimize_delta_table_read

logger = get_default_logger(__name__)


class SparkDataProcessor:
    """Handles Spark operations for data extraction and transformation from Delta tables."""
    
    def __init__(self, spark_session: Optional[SparkSession] = None):
        """
        Initialize the Spark data processor.
        
        Args:
            spark_session: Optional SparkSession instance. If None, will create Databricks-optimized session.
        """
        if spark_session is None:
            self.spark_manager = DatabricksSparkManager()
            self.spark = self.spark_manager.get_spark_session()
        else:
            self.spark = spark_session
            self.spark_manager = None
        
        logger.info("SparkDataProcessor initialized for Databricks Delta tables")
    
    def get_calculations_dataframe(self, 
                                 table_name: str = "clearview_prod.silver.calculations",
                                 interval_seconds: int = 900) -> DataFrame:
        """
        Extract and transform calculations data from Databricks Delta table.
        
        Args:
            table_name: Name of the source Delta table
            interval_seconds: Aggregation interval in seconds (default: 15 minutes)
        
        Returns:
            Transformed DataFrame with aggregated data
        """
        logger.info(f"Loading data from Delta table: {table_name}")
        
        # Apply Delta table optimizations
        optimize_delta_table_read(self.spark, table_name)
        
        # Load base data from Delta table
        df = self.spark.table(table_name)
        df = df.select("CycleId", "TopsoeTag", "StartTimestamp", "Value", "_ProcessedTime")
        
        # Get row count efficiently for Delta tables
        row_count = df.agg(count("*").alias("count")).collect()[0]["count"]
        logger.info(f"Loaded {row_count} rows from Delta table: {table_name}")
        
        # Aggregate data by interval using DataFrame operations (no RDD)
        aggregated_df = df.groupBy(
            "CycleId",
            "TopsoeTag",
            floor(unix_timestamp("StartTimestamp") / interval_seconds).alias("IntervalStartUnix"),
        ).agg(
            from_unixtime(floor(unix_timestamp("StartTimestamp") / interval_seconds) * interval_seconds).alias("StartTimestamp"),
            avg("Value").alias("Value"),
            max("_ProcessedTime").alias("_ProcessedTime"),
        )
        
        # Create final output format
        result_df = aggregated_df.select(
            concat(lit("{"), col("CycleId"), lit("}"), col("TopsoeTag")).alias("tag_id"),
            col("IntervalStartUnix").alias("timestamp"),
            col("Value").alias("value"),
            col("CycleId")
        )
        
        # Get aggregated row count
        aggregated_count = result_df.agg(count("*").alias("count")).collect()[0]["count"]
        logger.info(f"Aggregated data into {aggregated_count} rows")
        
        return result_df
    
    def get_unique_cycles(self, df: DataFrame) -> list:
        """
        Get list of unique cycle IDs from the DataFrame.
        
        Args:
            df: Input DataFrame containing CycleId column
        
        Returns:
            List of unique cycle IDs
        """
        cycles = [row.CycleId for row in df.select("CycleId").distinct().collect()]
        logger.info(f"Found {len(cycles)} unique cycles")
        return cycles
    
    def filter_by_cycle(self, df: DataFrame, cycle_id: str) -> DataFrame:
        """
        Filter DataFrame by specific cycle ID.
        
        Args:
            df: Input DataFrame
            cycle_id: Cycle ID to filter by
        
        Returns:
            Filtered DataFrame
        """
        filtered_df = df.filter(col("CycleId") == cycle_id)
        logger.debug(f"Filtered data for cycle {cycle_id}")
        return filtered_df
    
    def cache_dataframe(self, df: DataFrame) -> DataFrame:
        """
        Cache DataFrame for better performance.
        
        Args:
            df: DataFrame to cache
        
        Returns:
            Cached DataFrame
        """
        cached_df = df.cache()
        logger.info("DataFrame cached for improved performance")
        return cached_df
    
    def get_dataframe_stats(self, df: DataFrame) -> dict:
        """
        Get basic statistics about the DataFrame (Delta table optimized).
        
        Args:
            df: Input DataFrame
        
        Returns:
            Dictionary with DataFrame statistics
        """
        try:
            # Use DataFrame operations instead of RDD for Delta compatibility
            row_count = df.agg(count("*").alias("count")).collect()[0]["count"]
            columns = df.columns
            
            # Get partition count using DataFrame API
            try:
                # For Databricks, get partition info from DataFrame
                partitions = len(df.inputFiles()) if hasattr(df, 'inputFiles') else "unknown"
            except:
                partitions = "unknown"
            
            stats = {
                "row_count": row_count,
                "column_count": len(columns),
                "columns": columns,
                "partitions": partitions,
                "is_delta": True  # Assuming Delta tables
            }
            
            logger.info(f"DataFrame stats: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get DataFrame stats: {e}")
            return {"error": str(e)}
    
    def cleanup(self):
        """Clean up resources if needed."""
        if self.spark_manager:
            self.spark_manager.stop_spark_session()
            logger.info("SparkDataProcessor cleaned up")


def get_calculations_df(spark: SparkSession, 
                       table_name: str = "clearview_prod.silver.calculations",
                       interval_seconds: int = 900) -> DataFrame:
    """
    Legacy function for backward compatibility.
    Extract and transform calculations data from Databricks table.
    
    Args:
        spark: SparkSession instance
        table_name: Name of the source table
        interval_seconds: Aggregation interval in seconds
    
    Returns:
        Transformed DataFrame with aggregated data
    """
    processor = SparkDataProcessor(spark)
    return processor.get_calculations_dataframe(table_name, interval_seconds)