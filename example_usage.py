"""
Example usage of the modular data processing pipeline.
This script demonstrates different ways to use the new modular architecture.
"""
from pyspark.sql import SparkSession
from src.main import DataPipeline
from src.data.spark_operations import SparkDataProcessor
from src.redis.connection import RedisConnectionManager
from src.config.redis_config import get_redis_config
from src.utils.logging_config import get_default_logger

logger = get_default_logger(__name__)


def example_full_pipeline():
    """Example of running the complete pipeline."""
    logger.info("=== Running Full Pipeline Example ===")
    
    # Initialize Spark session
    spark = SparkSession.builder.appName("DataPipelineExample").getOrCreate()
    
    try:
        # Create pipeline with custom batch size
        pipeline = DataPipeline(spark_session=spark, batch_size=2000)
        
        # Run the complete pipeline
        stats = pipeline.run_pipeline()
        
        # Display results
        logger.info("Pipeline completed successfully!")
        for stat in stats:
            logger.info(f"Cycle {stat['cycle_id']}: {stat['records_successful']} records processed")
            
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
    finally:
        spark.stop()


def example_single_cycle():
    """Example of processing a single cycle."""
    logger.info("=== Running Single Cycle Example ===")
    
    spark = SparkSession.builder.appName("SingleCycleExample").getOrCreate()
    
    try:
        pipeline = DataPipeline(spark_session=spark)
        
        # First, get available cycles
        cycles = pipeline.get_available_cycles()
        logger.info(f"Available cycles: {cycles}")
        
        if cycles:
            # Process the first cycle
            cycle_id = cycles[0]
            stats = pipeline.run_single_cycle(cycle_id)
            logger.info(f"Processed cycle {cycle_id}: {stats}")
        else:
            logger.info("No cycles available to process")
            
    except Exception as e:
        logger.error(f"Single cycle processing failed: {e}")
    finally:
        spark.stop()


def example_individual_modules():
    """Example of using individual modules separately."""
    logger.info("=== Using Individual Modules Example ===")
    
    spark = SparkSession.builder.appName("IndividualModulesExample").getOrCreate()
    
    try:
        # 1. Use Spark operations module
        spark_processor = SparkDataProcessor(spark)
        df = spark_processor.get_calculations_dataframe()
        stats = spark_processor.get_dataframe_stats(df)
        logger.info(f"DataFrame stats: {stats}")
        
        # 2. Use Redis connection module
        redis_config = get_redis_config()
        redis_manager = RedisConnectionManager(redis_config)
        
        if redis_manager.test_connection():
            logger.info("Redis connection successful")
            client = redis_manager.get_client()
            # Perform Redis operations here
        else:
            logger.error("Redis connection failed")
            
        # 3. Get unique cycles
        cycles = spark_processor.get_unique_cycles(df)
        logger.info(f"Found {len(cycles)} unique cycles")
        
    except Exception as e:
        logger.error(f"Individual modules example failed: {e}")
    finally:
        spark.stop()


def example_custom_configuration():
    """Example of using custom configuration."""
    logger.info("=== Custom Configuration Example ===")
    
    # Custom Redis configuration
    custom_redis_config = {
        "host": "localhost",
        "port": 6379,
        "password": None,
        "ssl": False,
        "max_connections": 20,
        "socket_timeout": 60,
    }
    
    spark = SparkSession.builder.appName("CustomConfigExample").getOrCreate()
    
    try:
        # Create pipeline with custom configuration
        pipeline = DataPipeline(
            spark_session=spark,
            redis_config=custom_redis_config,
            batch_size=1000
        )
        
        # Test connection with custom config
        logger.info("Testing custom Redis configuration...")
        # Note: This will likely fail with localhost unless you have a local Redis instance
        
    except ConnectionError as e:
        logger.warning(f"Custom Redis connection failed (expected): {e}")
    except Exception as e:
        logger.error(f"Custom configuration example failed: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    logger.info("Starting data pipeline examples...")
    
    # Run examples (comment out as needed)
    try:
        example_full_pipeline()
    except Exception as e:
        logger.error(f"Full pipeline example failed: {e}")
    
    try:
        example_single_cycle()
    except Exception as e:
        logger.error(f"Single cycle example failed: {e}")
    
    try:
        example_individual_modules()
    except Exception as e:
        logger.error(f"Individual modules example failed: {e}")
    
    try:
        example_custom_configuration()
    except Exception as e:
        logger.error(f"Custom configuration example failed: {e}")
    
    logger.info("Examples completed!")