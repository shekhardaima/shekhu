"""
Databricks-specific example for the data processing pipeline.
This example demonstrates how to use the pipeline in Databricks runtime 14.3 environment.
"""

# Databricks notebook setup
# %pip install redis>=4.5.0

from src.main import DataPipeline
from src.config.databricks_config import get_databricks_environment_info
from src.utils.logging_config import get_default_logger

logger = get_default_logger(__name__)


def databricks_pipeline_example():
    """
    Example of running the pipeline in Databricks environment.
    This function can be run in a Databricks notebook.
    """
    logger.info("=== Databricks Data Processing Pipeline Example ===")
    
    # Display environment information
    env_info = get_databricks_environment_info()
    logger.info(f"Databricks Environment: {env_info}")
    
    try:
        # Initialize pipeline (will automatically detect Databricks environment)
        pipeline = DataPipeline(batch_size=5000)
        
        # Optional: Check available cycles first
        cycles = pipeline.get_available_cycles()
        logger.info(f"Available cycles: {cycles[:5]}...")  # Show first 5
        
        # Run the complete pipeline
        logger.info("Starting pipeline execution...")
        stats = pipeline.run_pipeline()
        
        # Display results
        logger.info("=== PIPELINE RESULTS ===")
        total_records = sum(stat.get('records_successful', 0) for stat in stats)
        total_cycles = len(stats)
        
        logger.info(f"Processed {total_cycles} cycles")
        logger.info(f"Total records processed: {total_records}")
        
        # Show detailed stats for each cycle
        for stat in stats:
            cycle_id = stat.get('cycle_id')
            records = stat.get('records_successful', 0)
            duration = stat.get('duration_sec', 0)
            success_rate = stat.get('success_rate', 0)
            
            logger.info(f"Cycle {cycle_id}: {records} records, {duration:.2f}s, {success_rate:.1f}% success")
        
        return stats
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise


def single_cycle_example(cycle_id: str = None):
    """
    Example of processing a single cycle in Databricks.
    
    Args:
        cycle_id: Specific cycle ID to process. If None, will process the first available cycle.
    """
    logger.info("=== Single Cycle Processing Example ===")
    
    try:
        pipeline = DataPipeline(batch_size=2000)
        
        if cycle_id is None:
            # Get first available cycle
            cycles = pipeline.get_available_cycles()
            if not cycles:
                logger.error("No cycles available for processing")
                return
            cycle_id = cycles[0]
        
        logger.info(f"Processing single cycle: {cycle_id}")
        
        # Process the specific cycle
        stats = pipeline.run_single_cycle(cycle_id)
        
        logger.info("=== SINGLE CYCLE RESULTS ===")
        logger.info(f"Cycle {cycle_id} processed:")
        logger.info(f"  Records processed: {stats.get('records_successful', 0)}")
        logger.info(f"  Duration: {stats.get('duration_sec', 0):.2f} seconds")
        logger.info(f"  Success rate: {stats.get('success_rate', 0):.1f}%")
        
        return stats
        
    except Exception as e:
        logger.error(f"Single cycle processing failed: {e}")
        raise


def delta_table_inspection_example():
    """
    Example of inspecting Delta table structure and statistics.
    """
    logger.info("=== Delta Table Inspection Example ===")
    
    try:
        from src.data.spark_operations import SparkDataProcessor
        
        # Initialize Spark processor
        processor = SparkDataProcessor()
        
        # Get DataFrame from Delta table
        df = processor.get_calculations_dataframe()
        
        # Get DataFrame statistics
        stats = processor.get_dataframe_stats(df)
        logger.info(f"Delta table statistics: {stats}")
        
        # Show sample data
        logger.info("Sample data from Delta table:")
        sample_data = df.limit(5).collect()
        for row in sample_data:
            logger.info(f"  {dict(row.asDict())}")
        
        # Get unique cycles
        cycles = processor.get_unique_cycles(df)
        logger.info(f"Total unique cycles: {len(cycles)}")
        
        return stats
        
    except Exception as e:
        logger.error(f"Delta table inspection failed: {e}")
        raise


# Databricks notebook cells
# COMMAND ----------
# Run the full pipeline
# stats = databricks_pipeline_example()

# COMMAND ----------
# Process a single cycle
# single_stats = single_cycle_example()

# COMMAND ----------
# Inspect Delta table
# table_stats = delta_table_inspection_example()

# COMMAND ----------
# Custom configuration example
def custom_config_example():
    """Example with custom Redis configuration for Databricks."""
    
    # Custom Redis config (can be set via Databricks secrets)
    import os
    custom_redis_config = {
        "host": dbutils.secrets.get(scope="redis", key="host"),  # Databricks secret
        "port": int(dbutils.secrets.get(scope="redis", key="port")),
        "password": dbutils.secrets.get(scope="redis", key="password"),
        "ssl": True,
        "max_connections": 100,  # Higher for Databricks cluster
        "socket_timeout": 60,
    }
    
    try:
        pipeline = DataPipeline(
            redis_config=custom_redis_config,
            batch_size=10000  # Larger batch for cluster processing
        )
        
        stats = pipeline.run_pipeline()
        logger.info(f"Custom config pipeline completed: {len(stats)} cycles processed")
        
        return stats
        
    except Exception as e:
        logger.error(f"Custom config pipeline failed: {e}")
        raise


if __name__ == "__main__":
    # This section runs when executed as a script (not in notebook)
    logger.info("Running Databricks pipeline example as script...")
    
    try:
        # Run the main example
        stats = databricks_pipeline_example()
        logger.info("Script execution completed successfully")
        
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        raise