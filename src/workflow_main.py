"""
Main workflow entry point for Databricks Asset Bundles deployment.
This script is designed to run as a Databricks job task, not in a notebook.
"""
import argparse
import sys
import os
from typing import Dict, Any

from .main import DataPipeline
from .config.redis_config import get_redis_config
from .utils.logging_config import get_default_logger

# Set up logging for workflow execution
logger = get_default_logger(__name__)


def get_databricks_secrets_config(environment: str) -> Dict[str, Any]:
    """
    Get Redis configuration from Databricks secrets based on environment.
    
    Args:
        environment: Environment name (dev, staging, prod)
    
    Returns:
        Redis configuration dictionary
    """
    try:
        # Import dbutils for secret access (only available in Databricks runtime)
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.appName("DataProcessingPipeline").getOrCreate()
        
        dbutils = DBUtils(spark)
        
        # Get secrets from appropriate scope based on environment
        scope_name = f"redis-secrets-{environment}"
        
        redis_config = {
            "host": dbutils.secrets.get(scope=scope_name, key="host"),
            "port": int(dbutils.secrets.get(scope=scope_name, key="port")),
            "password": dbutils.secrets.get(scope=scope_name, key="password"),
            "ssl": dbutils.secrets.get(scope=scope_name, key="ssl").lower() == "true",
            "max_connections": int(os.getenv("REDIS_MAX_CONNECTIONS", "100")),
            "socket_timeout": 60,  # Increased for workflow stability
            "socket_connect_timeout": 30,
            "retry_on_timeout": True,
            "health_check_interval": 30,
        }
        
        logger.info(f"Loaded Redis configuration from Databricks secrets for environment: {environment}")
        return redis_config
        
    except Exception as e:
        logger.warning(f"Failed to load secrets from Databricks: {e}")
        logger.info("Falling back to environment variables")
        return get_redis_config()


def parse_arguments():
    """Parse command line arguments for the workflow."""
    parser = argparse.ArgumentParser(description="Data Processing Pipeline Workflow")
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Batch size for data processing (default: 10000)"
    )
    
    parser.add_argument(
        "--environment",
        type=str,
        default="dev",
        choices=["dev", "staging", "prod"],
        help="Deployment environment (default: dev)"
    )
    
    parser.add_argument(
        "--table-name",
        type=str,
        default="clearview_prod.silver.calculations",
        help="Source Delta table name"
    )
    
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=900,
        help="Aggregation interval in seconds (default: 900 = 15 minutes)"
    )
    
    parser.add_argument(
        "--single-cycle",
        type=str,
        default=None,
        help="Process only a specific cycle ID"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform a dry run without writing to Redis"
    )
    
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)"
    )
    
    return parser.parse_args()


def setup_workflow_logging(log_level: str):
    """Set up logging for workflow execution."""
    import logging
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s — %(levelname)s — %(name)s:%(funcName)s:%(lineno)d — %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    logger.info(f"Workflow logging configured at {log_level} level")


def log_environment_info():
    """Log information about the Databricks environment."""
    from .config.databricks_config import get_databricks_environment_info
    
    env_info = get_databricks_environment_info()
    logger.info("=== DATABRICKS ENVIRONMENT INFO ===")
    for key, value in env_info.items():
        logger.info(f"{key}: {value}")


def validate_configuration(redis_config: Dict[str, Any]) -> bool:
    """
    Validate the configuration before starting the pipeline.
    
    Args:
        redis_config: Redis configuration to validate
    
    Returns:
        True if configuration is valid, False otherwise
    """
    try:
        from .redis.connection import RedisConnectionManager
        
        # Test Redis connectivity
        redis_manager = RedisConnectionManager(redis_config)
        if not redis_manager.test_connection():
            logger.error("Redis connection test failed")
            return False
        
        # Test Delta table connectivity
        from .config.databricks_config import DatabricksSparkManager
        spark_manager = DatabricksSparkManager()
        if not spark_manager.test_delta_connectivity():
            logger.error("Delta table connectivity test failed")
            return False
        
        logger.info("Configuration validation passed")
        return True
        
    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        return False


def run_pipeline_workflow(args) -> int:
    """
    Run the main pipeline workflow.
    
    Args:
        args: Parsed command line arguments
    
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    try:
        # Get configuration
        redis_config = get_databricks_secrets_config(args.environment)
        
        # Validate configuration
        if not validate_configuration(redis_config):
            logger.error("Configuration validation failed, aborting pipeline")
            return 1
        
        # Create pipeline with workflow-optimized settings
        pipeline = DataPipeline(
            redis_config=redis_config,
            batch_size=args.batch_size
        )
        
        logger.info("=== STARTING DATA PROCESSING PIPELINE ===")
        logger.info(f"Environment: {args.environment}")
        logger.info(f"Batch size: {args.batch_size}")
        logger.info(f"Table: {args.table_name}")
        logger.info(f"Interval: {args.interval_seconds} seconds")
        
        if args.dry_run:
            logger.info("DRY RUN MODE - No data will be written to Redis")
            # Get available cycles for dry run
            cycles = pipeline.get_available_cycles(args.table_name, args.interval_seconds)
            logger.info(f"Would process {len(cycles)} cycles: {cycles[:5]}...")
            return 0
        
        # Execute pipeline
        if args.single_cycle:
            logger.info(f"Processing single cycle: {args.single_cycle}")
            stats = pipeline.run_single_cycle(
                args.single_cycle,
                args.table_name,
                args.interval_seconds
            )
            results = [stats]
        else:
            logger.info("Processing all cycles")
            results = pipeline.run_pipeline(args.table_name, args.interval_seconds)
        
        # Log results
        logger.info("=== PIPELINE EXECUTION COMPLETED ===")
        total_cycles = len(results)
        total_records = sum(stat.get('records_successful', 0) for stat in results)
        total_duration = sum(stat.get('duration_sec', 0) for stat in results)
        
        logger.info(f"Processed {total_cycles} cycles")
        logger.info(f"Total records processed: {total_records}")
        logger.info(f"Total execution time: {total_duration:.2f} seconds")
        
        # Log detailed stats for each cycle
        for stat in results:
            cycle_id = stat.get('cycle_id')
            records = stat.get('records_successful', 0)
            duration = stat.get('duration_sec', 0)
            success_rate = stat.get('success_rate', 0)
            
            logger.info(f"Cycle {cycle_id}: {records} records, {duration:.2f}s, {success_rate:.1f}% success")
        
        # Check for any failures
        failed_cycles = [stat for stat in results if stat.get('success_rate', 0) < 100]
        if failed_cycles:
            logger.warning(f"Found {len(failed_cycles)} cycles with partial failures")
            for stat in failed_cycles:
                logger.warning(f"Cycle {stat.get('cycle_id')}: {stat.get('success_rate', 0):.1f}% success rate")
        
        logger.info("Pipeline workflow completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"Pipeline workflow failed: {e}")
        logger.exception("Full exception details:")
        return 1


def main():
    """Main entry point for the workflow."""
    # Parse arguments
    args = parse_arguments()
    
    # Set up logging
    setup_workflow_logging(args.log_level)
    
    # Log environment information
    log_environment_info()
    
    # Run the pipeline workflow
    exit_code = run_pipeline_workflow(args)
    
    # Exit with appropriate code
    sys.exit(exit_code)


if __name__ == "__main__":
    main()