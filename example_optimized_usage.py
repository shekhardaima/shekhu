"""
Example usage of the optimized Redis Time Series writer
Demonstrates how to process multi-million records efficiently
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, rand, unix_timestamp, current_timestamp
from pyspark.sql.types import *
import time
import logging

from redis_timeseries_optimizer import OptimizedRedisTimeSeriesWriter, SparkRedisTimeSeriesOptimizer
from redis_cluster_config import get_optimal_config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_sample_data(spark: SparkSession, num_records: int = 1_000_000) -> "DataFrame":
    """
    Create sample time series data for testing
    """
    logger.info(f"Creating sample data with {num_records:,} records")
    
    # Create base DataFrame with the specified number of records
    df = spark.range(num_records).toDF("id")
    
    # Add time series columns
    df = df.withColumn("CycleId", (col("id") % 1000).cast("string")) \
           .withColumn("timestamp", (unix_timestamp(current_timestamp()) * 1000 + col("id")).cast("long")) \
           .withColumn("temperature", (rand() * 100 + 20)) \
           .withColumn("pressure", (rand() * 50 + 10)) \
           .withColumn("vibration", (rand() * 10)) \
           .withColumn("speed", (rand() * 1000 + 500)) \
           .withColumn("power", (rand() * 500 + 100))
    
    return df.select("CycleId", "timestamp", "temperature", "pressure", "vibration", "speed", "power")

def main():
    """
    Main example function
    """
    # Configuration
    DATA_SIZE_MILLIONS = 5.0  # 5 million records
    REDIS_TYPE = "redis_cluster"  # or "single_instance", "redis_sentinel"
    
    # Get optimal configuration
    config = get_optimal_config(DATA_SIZE_MILLIONS, REDIS_TYPE)
    cluster_config = config["cluster_config"]
    redis_config = config["redis_config"]
    
    logger.info(f"Using {cluster_config['cluster_type']} configuration")
    logger.info(f"Estimated performance: {config['estimated_performance']['records_per_second']:,} records/sec")
    logger.info(f"Estimated duration: {config['estimated_performance']['estimated_duration_minutes']:.1f} minutes")
    
    # Initialize Spark with optimized configuration
    spark_builder = SparkSession.builder.appName("OptimizedRedisTimeSeriesExample")
    
    # Apply Spark configuration
    for key, value in cluster_config["spark_config"].items():
        spark_builder = spark_builder.config(key, value)
    
    spark = spark_builder.getOrCreate()
    
    try:
        # Optimize Spark for the cluster
        spark = SparkRedisTimeSeriesOptimizer.optimize_spark_config(
            spark, 
            memory_gb=cluster_config.get("memory_gb", 256),
            cores=cluster_config.get("cores", 32)
        )
        
        # Create sample data
        df = create_sample_data(spark, int(DATA_SIZE_MILLIONS * 1_000_000))
        
        # Cache the DataFrame for better performance
        df.cache()
        
        # Show sample data
        logger.info("Sample data:")
        df.show(5)
        df.printSchema()
        
        # Update Redis configuration with your actual Redis details
        redis_config.update({
            'host': 'your-redis-host',  # Replace with your Redis host
            'port': 7000,  # Replace with your Redis port
            # 'password': 'your-password',  # Add if needed
        })
        
        # Create optimized Redis writer
        writer = OptimizedRedisTimeSeriesWriter(
            redis_config=redis_config,
            **cluster_config["redis_writer_config"]
        )
        
        # Write to Redis Time Series
        logger.info("Starting Redis write operation...")
        start_time = time.time()
        
        stats = writer.write_dataframe(
            df=df,
            cycle_id_col='CycleId',
            timestamp_col='timestamp',
            metric_cols=['temperature', 'pressure', 'vibration', 'speed', 'power']
        )
        
        end_time = time.time()
        
        # Print results
        logger.info("=" * 60)
        logger.info("WRITE OPERATION COMPLETED")
        logger.info("=" * 60)
        logger.info(f"Total records written: {stats['total_records_written']:,}")
        logger.info(f"Duration: {stats['duration_seconds']:.2f} seconds")
        logger.info(f"Throughput: {stats['records_per_second']:,.0f} records/second")
        logger.info(f"Batch size used: {stats['batch_size']:,}")
        logger.info(f"Partitions processed: {stats['partitions_processed']}")
        logger.info("=" * 60)
        
        # Performance comparison
        estimated_throughput = config['estimated_performance']['records_per_second']
        actual_throughput = stats['records_per_second']
        performance_ratio = actual_throughput / estimated_throughput
        
        logger.info(f"Performance vs estimate: {performance_ratio:.2f}x")
        if performance_ratio > 0.8:
            logger.info("✅ Performance is within expected range")
        else:
            logger.warning("⚠️  Performance is below expectations - consider tuning")
        
    except Exception as e:
        logger.error(f"Error during execution: {e}")
        raise
    finally:
        spark.stop()

def benchmark_different_configurations():
    """
    Benchmark different cluster configurations
    """
    configurations = [
        ("Single Node", "single_node", 1.0),
        ("Multi Node", "multi_node", 5.0),
        ("High Performance", "high_performance", 10.0)
    ]
    
    results = []
    
    for config_name, cluster_type, data_size in configurations:
        logger.info(f"\n{'='*50}")
        logger.info(f"BENCHMARKING: {config_name}")
        logger.info(f"{'='*50}")
        
        config = get_optimal_config(data_size, "redis_cluster")
        
        # Simulate performance (replace with actual runs)
        estimated_perf = config['estimated_performance']
        
        results.append({
            'configuration': config_name,
            'data_size_millions': data_size,
            'estimated_throughput': estimated_perf['records_per_second'],
            'estimated_duration_minutes': estimated_perf['estimated_duration_minutes'],
            'batch_size': config['cluster_config']['redis_writer_config']['batch_size'],
            'max_workers': config['cluster_config']['redis_writer_config']['max_workers']
        })
    
    # Print comparison table
    logger.info(f"\n{'='*80}")
    logger.info("CONFIGURATION COMPARISON")
    logger.info(f"{'='*80}")
    logger.info(f"{'Config':<15} {'Data(M)':<8} {'Throughput':<12} {'Duration(min)':<12} {'Batch Size':<10} {'Workers':<8}")
    logger.info("-" * 80)
    
    for result in results:
        logger.info(f"{result['configuration']:<15} "
                   f"{result['data_size_millions']:<8.1f} "
                   f"{result['estimated_throughput']:<12,} "
                   f"{result['estimated_duration_minutes']:<12.1f} "
                   f"{result['batch_size']:<10,} "
                   f"{result['max_workers']:<8}")

if __name__ == "__main__":
    # Run main example
    main()
    
    # Uncomment to run benchmark comparison
    # benchmark_different_configurations()