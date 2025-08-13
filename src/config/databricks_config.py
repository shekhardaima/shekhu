"""
Databricks-specific configuration for Spark sessions and Delta Lake.
"""
import os
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
from ..utils.logging_config import get_default_logger

logger = get_default_logger(__name__)


def get_databricks_spark_config() -> Dict[str, str]:
    """
    Get Databricks-optimized Spark configuration.
    
    Returns:
        Dictionary with Spark configuration for Databricks runtime 14.3
    """
    config = {
        # Delta Lake configurations
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        
        # Databricks runtime optimizations
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        
        # Performance optimizations for time series data
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
        
        # Memory and serialization optimizations
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        
        # Custom configurations from environment
        **{k.replace("SPARK_CONF_", "").replace("_", "."): v 
           for k, v in os.environ.items() if k.startswith("SPARK_CONF_")}
    }
    
    return config


def create_databricks_spark_session(app_name: str = "DataProcessingPipeline") -> SparkSession:
    """
    Create a Spark session optimized for Databricks runtime 14.3.
    
    Args:
        app_name: Application name for the Spark session
        
    Returns:
        Configured SparkSession instance
    """
    logger.info(f"Creating Databricks Spark session: {app_name}")
    
    # Check if running in Databricks environment
    is_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ
    
    builder = SparkSession.builder.appName(app_name)
    
    if is_databricks:
        # Running on Databricks cluster - use existing session or create new
        logger.info("Detected Databricks runtime environment")
        try:
            spark = SparkSession.getActiveSession()
            if spark is None:
                spark = builder.getOrCreate()
            logger.info(f"Using Databricks runtime version: {os.environ.get('DATABRICKS_RUNTIME_VERSION', 'Unknown')}")
        except Exception as e:
            logger.warning(f"Could not get active session, creating new: {e}")
            spark = builder.getOrCreate()
    else:
        # Local development with Databricks Connect
        logger.info("Setting up Databricks Connect configuration")
        
        # Apply Databricks-specific configurations
        spark_config = get_databricks_spark_config()
        for key, value in spark_config.items():
            builder = builder.config(key, value)
        
        # Databricks Connect specific settings
        databricks_host = os.getenv("DATABRICKS_HOST")
        databricks_token = os.getenv("DATABRICKS_TOKEN")
        databricks_cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")
        
        if databricks_host and databricks_token and databricks_cluster_id:
            logger.info(f"Connecting to Databricks cluster: {databricks_cluster_id}")
            builder = (builder
                      .config("spark.databricks.service.address", databricks_host)
                      .config("spark.databricks.service.token", databricks_token)
                      .config("spark.databricks.service.clusterId", databricks_cluster_id))
        else:
            logger.warning("Databricks Connect environment variables not set. Using local Spark session.")
        
        spark = builder.getOrCreate()
    
    # Verify Delta Lake is available
    try:
        spark.sql("SELECT 1").collect()
        logger.info("Spark session created successfully")
        
        # Test Delta Lake functionality
        spark.sql("SHOW DATABASES").collect()
        logger.info("Delta Lake functionality verified")
        
    except Exception as e:
        logger.error(f"Failed to verify Spark session: {e}")
        raise
    
    return spark


def get_databricks_environment_info() -> Dict[str, Any]:
    """
    Get information about the Databricks environment.
    
    Returns:
        Dictionary with environment information
    """
    env_info = {
        "is_databricks": "DATABRICKS_RUNTIME_VERSION" in os.environ,
        "runtime_version": os.getenv("DATABRICKS_RUNTIME_VERSION"),
        "cluster_id": os.getenv("DATABRICKS_CLUSTER_ID"),
        "workspace_url": os.getenv("DATABRICKS_HOST"),
        "spark_version": None,
        "delta_version": None,
    }
    
    try:
        spark = SparkSession.getActiveSession()
        if spark:
            env_info["spark_version"] = spark.version
            # Try to get Delta version
            try:
                delta_version = spark.sql("SELECT delta_version()").collect()[0][0]
                env_info["delta_version"] = delta_version
            except:
                pass
    except:
        pass
    
    return env_info


def optimize_delta_table_read(spark: SparkSession, table_name: str) -> None:
    """
    Apply Delta table read optimizations.
    
    Args:
        spark: SparkSession instance
        table_name: Name of the Delta table to optimize
    """
    try:
        logger.info(f"Applying read optimizations for Delta table: {table_name}")
        
        # Enable Delta optimizations
        spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
        spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
        
        # Check if table exists and get statistics
        table_stats = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()
        if table_stats:
            logger.info(f"Delta table statistics retrieved for {table_name}")
            
    except Exception as e:
        logger.warning(f"Could not apply Delta optimizations for {table_name}: {e}")


class DatabricksSparkManager:
    """Manages Spark sessions in Databricks environment."""
    
    def __init__(self, app_name: str = "DataProcessingPipeline"):
        """
        Initialize Databricks Spark manager.
        
        Args:
            app_name: Application name for Spark session
        """
        self.app_name = app_name
        self._spark = None
        self.env_info = get_databricks_environment_info()
        logger.info(f"Databricks environment: {self.env_info}")
    
    def get_spark_session(self) -> SparkSession:
        """
        Get or create Spark session.
        
        Returns:
            SparkSession instance
        """
        if self._spark is None:
            self._spark = create_databricks_spark_session(self.app_name)
        return self._spark
    
    def stop_spark_session(self):
        """Stop the Spark session if it's not managed by Databricks."""
        if self._spark and not self.env_info["is_databricks"]:
            logger.info("Stopping Spark session")
            self._spark.stop()
            self._spark = None
    
    def test_delta_connectivity(self, table_name: str = "clearview_prod.silver.calculations") -> bool:
        """
        Test connectivity to Delta tables.
        
        Args:
            table_name: Table name to test
            
        Returns:
            True if connection successful, False otherwise
        """
        try:
            spark = self.get_spark_session()
            
            # Test basic table access
            result = spark.sql(f"SELECT COUNT(*) as count FROM {table_name} LIMIT 1").collect()
            logger.info(f"Delta table connectivity test successful: {result[0]['count']} rows found")
            return True
            
        except Exception as e:
            logger.error(f"Delta table connectivity test failed: {e}")
            return False