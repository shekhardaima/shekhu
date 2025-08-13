"""
Health check script for monitoring the data processing pipeline.
This runs as a separate Databricks job to monitor pipeline health.
"""
import argparse
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, List

from ..config.redis_config import get_redis_config
from ..config.databricks_config import DatabricksSparkManager, get_databricks_environment_info
from ..redis.connection import RedisConnectionManager
from ..utils.logging_config import get_default_logger

logger = get_default_logger(__name__)


def get_databricks_secrets_config(environment: str) -> Dict[str, Any]:
    """Get Redis configuration from Databricks secrets."""
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.appName("PipelineHealthCheck").getOrCreate()
        
        dbutils = DBUtils(spark)
        scope_name = f"redis-secrets-{environment}"
        
        redis_config = {
            "host": dbutils.secrets.get(scope=scope_name, key="host"),
            "port": int(dbutils.secrets.get(scope=scope_name, key="port")),
            "password": dbutils.secrets.get(scope=scope_name, key="password"),
            "ssl": dbutils.secrets.get(scope=scope_name, key="ssl").lower() == "true",
            "max_connections": 10,  # Reduced for health check
            "socket_timeout": 30,
            "socket_connect_timeout": 10,
            "retry_on_timeout": True,
            "health_check_interval": 30,
        }
        
        return redis_config
        
    except Exception as e:
        logger.warning(f"Failed to load secrets: {e}")
        return get_redis_config()


def check_databricks_environment() -> Dict[str, Any]:
    """Check Databricks environment health."""
    logger.info("Checking Databricks environment...")
    
    try:
        env_info = get_databricks_environment_info()
        
        health_status = {
            "databricks_environment": "healthy",
            "runtime_version": env_info.get("runtime_version"),
            "spark_version": env_info.get("spark_version"),
            "delta_version": env_info.get("delta_version"),
            "is_databricks": env_info.get("is_databricks", False)
        }
        
        if not env_info.get("is_databricks"):
            health_status["databricks_environment"] = "warning"
            logger.warning("Not running in Databricks environment")
        
        logger.info("Databricks environment check completed")
        return health_status
        
    except Exception as e:
        logger.error(f"Databricks environment check failed: {e}")
        return {
            "databricks_environment": "unhealthy",
            "error": str(e)
        }


def check_delta_table_connectivity() -> Dict[str, Any]:
    """Check Delta table connectivity and health."""
    logger.info("Checking Delta table connectivity...")
    
    try:
        spark_manager = DatabricksSparkManager()
        
        # Test basic connectivity
        is_connected = spark_manager.test_delta_connectivity("clearview_prod.silver.calculations")
        
        if is_connected:
            # Get table statistics
            spark = spark_manager.get_spark_session()
            
            # Check table exists and get basic info
            table_info = spark.sql("DESCRIBE DETAIL clearview_prod.silver.calculations").collect()
            
            if table_info:
                table_stats = table_info[0].asDict()
                
                health_status = {
                    "delta_connectivity": "healthy",
                    "table_exists": True,
                    "table_format": table_stats.get("format"),
                    "num_files": table_stats.get("numFiles"),
                    "size_in_bytes": table_stats.get("sizeInBytes"),
                    "last_modified": str(table_stats.get("lastModified"))
                }
            else:
                health_status = {
                    "delta_connectivity": "warning",
                    "table_exists": False,
                    "message": "Table exists but no metadata available"
                }
        else:
            health_status = {
                "delta_connectivity": "unhealthy",
                "table_exists": False,
                "error": "Cannot connect to Delta table"
            }
        
        logger.info("Delta table connectivity check completed")
        return health_status
        
    except Exception as e:
        logger.error(f"Delta table connectivity check failed: {e}")
        return {
            "delta_connectivity": "unhealthy",
            "error": str(e)
        }


def check_redis_connectivity(redis_config: Dict[str, Any]) -> Dict[str, Any]:
    """Check Redis connectivity and health."""
    logger.info("Checking Redis connectivity...")
    
    try:
        redis_manager = RedisConnectionManager(redis_config)
        
        # Test basic connectivity
        is_connected = redis_manager.test_connection()
        
        if is_connected:
            client = redis_manager.get_client()
            
            # Get Redis info
            redis_info = client.info()
            
            health_status = {
                "redis_connectivity": "healthy",
                "redis_version": redis_info.get("redis_version"),
                "connected_clients": redis_info.get("connected_clients"),
                "used_memory_human": redis_info.get("used_memory_human"),
                "uptime_in_seconds": redis_info.get("uptime_in_seconds"),
                "total_commands_processed": redis_info.get("total_commands_processed")
            }
            
            # Test TimeSeries functionality
            try:
                # Try to create a test key
                test_key = f"health_check_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                client.execute_command("TS.CREATE", test_key, "RETENTION", 60000)  # 1 minute retention
                client.execute_command("TS.ADD", test_key, "*", 1.0)  # Add test data point
                client.delete(test_key)  # Clean up
                
                health_status["timeseries_functionality"] = "healthy"
                
            except Exception as ts_error:
                health_status["timeseries_functionality"] = "warning"
                health_status["timeseries_error"] = str(ts_error)
                logger.warning(f"TimeSeries functionality test failed: {ts_error}")
        else:
            health_status = {
                "redis_connectivity": "unhealthy",
                "error": "Cannot connect to Redis"
            }
        
        logger.info("Redis connectivity check completed")
        return health_status
        
    except Exception as e:
        logger.error(f"Redis connectivity check failed: {e}")
        return {
            "redis_connectivity": "unhealthy",
            "error": str(e)
        }


def check_recent_pipeline_runs() -> Dict[str, Any]:
    """Check for recent pipeline runs and their status."""
    logger.info("Checking recent pipeline runs...")
    
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.appName("PipelineHealthCheck").getOrCreate()
        
        # Check if we can query the source table for recent data
        recent_data_query = """
        SELECT 
            COUNT(*) as record_count,
            MAX(_ProcessedTime) as latest_processed_time,
            COUNT(DISTINCT CycleId) as unique_cycles
        FROM clearview_prod.silver.calculations 
        WHERE _ProcessedTime >= current_timestamp() - INTERVAL 24 HOURS
        """
        
        result = spark.sql(recent_data_query).collect()[0]
        
        health_status = {
            "recent_data_check": "healthy",
            "records_last_24h": result["record_count"],
            "unique_cycles_last_24h": result["unique_cycles"],
            "latest_processed_time": str(result["latest_processed_time"])
        }
        
        # Check if data is recent enough (within last 2 hours)
        if result["latest_processed_time"]:
            time_diff = datetime.now() - result["latest_processed_time"]
            if time_diff > timedelta(hours=2):
                health_status["recent_data_check"] = "warning"
                health_status["warning"] = f"Latest data is {time_diff} old"
        
        logger.info("Recent pipeline runs check completed")
        return health_status
        
    except Exception as e:
        logger.error(f"Recent pipeline runs check failed: {e}")
        return {
            "recent_data_check": "unhealthy",
            "error": str(e)
        }


def generate_health_report(health_checks: Dict[str, Any]) -> Dict[str, Any]:
    """Generate overall health report."""
    
    # Determine overall health status
    statuses = []
    for check_name, check_result in health_checks.items():
        if isinstance(check_result, dict):
            for key, value in check_result.items():
                if key.endswith('_connectivity') or key.endswith('_environment') or key.endswith('_check'):
                    statuses.append(value)
    
    # Count status types
    healthy_count = statuses.count('healthy')
    warning_count = statuses.count('warning')
    unhealthy_count = statuses.count('unhealthy')
    
    # Determine overall status
    if unhealthy_count > 0:
        overall_status = "unhealthy"
    elif warning_count > 0:
        overall_status = "warning"
    else:
        overall_status = "healthy"
    
    report = {
        "timestamp": datetime.now().isoformat(),
        "overall_status": overall_status,
        "summary": {
            "healthy_checks": healthy_count,
            "warning_checks": warning_count,
            "unhealthy_checks": unhealthy_count,
            "total_checks": len(statuses)
        },
        "detailed_results": health_checks
    }
    
    return report


def main():
    """Main entry point for health check."""
    parser = argparse.ArgumentParser(description="Pipeline Health Check")
    parser.add_argument("--environment", type=str, default="dev", 
                       choices=["dev", "staging", "prod"],
                       help="Environment to check")
    
    args = parser.parse_args()
    
    logger.info(f"Starting health check for environment: {args.environment}")
    
    try:
        # Get configuration
        redis_config = get_databricks_secrets_config(args.environment)
        
        # Run health checks
        health_checks = {}
        
        # Check Databricks environment
        health_checks["databricks"] = check_databricks_environment()
        
        # Check Delta table connectivity
        health_checks["delta_table"] = check_delta_table_connectivity()
        
        # Check Redis connectivity
        health_checks["redis"] = check_redis_connectivity(redis_config)
        
        # Check recent pipeline runs
        health_checks["recent_runs"] = check_recent_pipeline_runs()
        
        # Generate overall report
        report = generate_health_report(health_checks)
        
        # Log report
        logger.info("=== HEALTH CHECK REPORT ===")
        logger.info(f"Overall Status: {report['overall_status'].upper()}")
        logger.info(f"Summary: {report['summary']}")
        
        # Log detailed results
        for check_name, check_result in report["detailed_results"].items():
            logger.info(f"{check_name.upper()}: {check_result}")
        
        # Exit with appropriate code
        if report["overall_status"] == "unhealthy":
            logger.error("Health check failed - critical issues detected")
            sys.exit(1)
        elif report["overall_status"] == "warning":
            logger.warning("Health check completed with warnings")
            sys.exit(0)
        else:
            logger.info("Health check passed - all systems healthy")
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"Health check failed with exception: {e}")
        logger.exception("Full exception details:")
        sys.exit(1)


if __name__ == "__main__":
    main()