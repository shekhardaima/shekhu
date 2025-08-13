"""
Main orchestration module for the data processing pipeline.
"""
from pyspark.sql import SparkSession
from typing import List, Dict, Any, Optional

from .config.redis_config import get_redis_config
from .data.spark_operations import SparkDataProcessor
from .data.processing import DataProcessor
from .redis.connection import RedisConnectionManager
from .utils.logging_config import get_default_logger

logger = get_default_logger(__name__)


class DataPipeline:
    """Main data processing pipeline orchestrator."""
    
    def __init__(self, 
                 spark_session: Optional[SparkSession] = None,
                 redis_config: Optional[Dict[str, Any]] = None,
                 batch_size: int = 5000):
        """
        Initialize the data pipeline.
        
        Args:
            spark_session: Optional SparkSession instance
            redis_config: Optional Redis configuration dictionary
            batch_size: Batch size for data processing
        """
        self.spark_processor = SparkDataProcessor(spark_session)
        self.redis_config = redis_config or get_redis_config()
        self.data_processor = DataProcessor(self.redis_config, batch_size)
        self.batch_size = batch_size
        
        # Test Redis connection
        redis_manager = RedisConnectionManager(self.redis_config)
        if not redis_manager.test_connection():
            raise ConnectionError("Failed to connect to Redis")
        
        logger.info("DataPipeline initialized successfully")
    
    def run_pipeline(self, 
                    table_name: str = "clearview_prod.silver.calculations",
                    interval_seconds: int = 900) -> List[Dict[str, Any]]:
        """
        Run the complete data processing pipeline.
        
        Args:
            table_name: Name of the source table
            interval_seconds: Aggregation interval in seconds
        
        Returns:
            List of processing statistics for each cycle
        """
        logger.info("Starting data processing pipeline")
        
        # Step 1: Extract and transform data from Databricks
        logger.info("Step 1: Extracting and transforming data")
        df_ts = self.spark_processor.get_calculations_dataframe(table_name, interval_seconds)
        df_ts = self.spark_processor.cache_dataframe(df_ts)
        
        # Log DataFrame statistics
        stats = self.spark_processor.get_dataframe_stats(df_ts)
        logger.info(f"DataFrame statistics: {stats}")
        
        # Step 2: Process all cycles
        logger.info("Step 2: Processing cycles and writing to Redis")
        processing_stats = self.data_processor.process_all_cycles(df_ts)
        
        logger.info("Pipeline execution completed successfully")
        return processing_stats
    
    def run_single_cycle(self, 
                        cycle_id: str,
                        table_name: str = "clearview_prod.silver.calculations",
                        interval_seconds: int = 900) -> Dict[str, Any]:
        """
        Run the pipeline for a single cycle.
        
        Args:
            cycle_id: Cycle ID to process
            table_name: Name of the source table
            interval_seconds: Aggregation interval in seconds
        
        Returns:
            Processing statistics for the cycle
        """
        logger.info(f"Processing single cycle: {cycle_id}")
        
        # Extract and transform data
        df_ts = self.spark_processor.get_calculations_dataframe(table_name, interval_seconds)
        df_ts = self.spark_processor.cache_dataframe(df_ts)
        
        # Filter for specific cycle
        cycle_df = self.spark_processor.filter_by_cycle(df_ts, cycle_id)
        
        # Process the cycle
        cycle_stats = self.data_processor.process_single_cycle(cycle_df, cycle_id)
        
        logger.info(f"Single cycle processing completed: {cycle_stats}")
        return cycle_stats
    
    def get_available_cycles(self, 
                           table_name: str = "clearview_prod.silver.calculations",
                           interval_seconds: int = 900) -> List[str]:
        """
        Get list of available cycles in the data.
        
        Args:
            table_name: Name of the source table
            interval_seconds: Aggregation interval in seconds
        
        Returns:
            List of available cycle IDs
        """
        df_ts = self.spark_processor.get_calculations_dataframe(table_name, interval_seconds)
        cycles = self.spark_processor.get_unique_cycles(df_ts)
        logger.info(f"Available cycles: {cycles}")
        return cycles


def main():
    """Main function to run the data processing pipeline."""
    try:
        # Initialize Spark session
        spark = SparkSession.builder.getOrCreate()
        
        # Create and run pipeline
        pipeline = DataPipeline(spark_session=spark, batch_size=5000)
        stats = pipeline.run_pipeline()
        
        # Log final results
        logger.info("=== PIPELINE EXECUTION SUMMARY ===")
        for stat in stats:
            logger.info(f"Cycle {stat.get('cycle_id')}: {stat}")
            
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise
    finally:
        # Clean up Spark session
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()