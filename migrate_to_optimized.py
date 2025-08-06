"""
Migration script to transition from original Redis code to optimized TS.MADD version
Run this to see the performance difference and migrate safely
"""

from pyspark.sql import SparkSession
import time
import logging
from databricks_config import (
    AZURE_REDIS_CONFIG, 
    apply_databricks_config, 
    apply_runtime_optimizations,
    get_optimal_config_for_data_size,
    estimate_processing_time
)
from redis_optimized import (
    get_optimized_dataframe,
    write_timeseries_data_optimized,
    create_keys_optimized
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def compare_performance_estimates():
    """
    Compare performance between original and optimized approaches
    """
    print("🔍 PERFORMANCE COMPARISON: Original vs Optimized")
    print("=" * 60)
    
    test_sizes = [1_000_000, 5_000_000, 10_000_000, 50_000_000]
    
    for size in test_sizes:
        print(f"\n📊 Dataset Size: {size:,} records")
        print("-" * 40)
        
        # Original approach estimates (based on your current batch size of 1000)
        original_throughput = 5000  # Conservative estimate for individual TS.ADD
        original_time = size / original_throughput
        
        # Optimized approach estimates
        config_name = get_optimal_config_for_data_size(size)
        optimized = estimate_processing_time(size, config_name)
        
        speedup = original_time / optimized['estimated_time_seconds']
        
        print(f"Original (TS.ADD):     {original_time/60:6.1f} min @ {original_throughput:,} records/sec")
        print(f"Optimized (TS.MADD):   {optimized['estimated_time_minutes']:6.1f} min @ {optimized['estimated_throughput']:,.0f} records/sec")
        print(f"Speedup:               {speedup:6.1f}x faster")
        print(f"Recommended config:    {config_name.replace('_', ' ').title()}")

def run_small_test():
    """
    Run a small test to validate the optimized approach works
    """
    logger.info("🧪 Running small validation test...")
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("RedisOptimizationTest") \
        .getOrCreate()
    
    try:
        # Apply optimizations for current single node
        spark = apply_databricks_config(spark, "current_single_node")
        spark = apply_runtime_optimizations(spark)
        
        # Get a small sample of data for testing
        df_ts = get_optimized_dataframe(spark)
        
        # Take a small sample for testing (1000 records)
        df_sample = df_ts.limit(1000)
        df_sample.cache()
        
        record_count = df_sample.count()
        logger.info(f"📋 Testing with {record_count:,} records")
        
        # Test the optimized write function
        start_time = time.time()
        
        write_stats = write_timeseries_data_optimized(
            df_sample, 
            AZURE_REDIS_CONFIG, 
            batch_size=500  # Small batch for testing
        )
        
        duration = time.time() - start_time
        
        # Results
        logger.info("🎯 TEST RESULTS:")
        logger.info(f"   Records processed: {write_stats['total_records']:,}")
        logger.info(f"   Successfully written: {write_stats['successful']:,}")
        logger.info(f"   Success rate: {write_stats['success_rate']:.1f}%")
        logger.info(f"   Throughput: {write_stats['records_per_second']:,.0f} records/second")
        logger.info(f"   Duration: {duration:.2f} seconds")
        
        if write_stats['success_rate'] > 90:
            logger.info("✅ Test PASSED - Ready for full migration!")
            return True
        else:
            logger.warning("⚠️ Test had issues - check Redis connection and configuration")
            return False
            
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        return False
    finally:
        spark.stop()

def create_migration_plan(current_record_count: int):
    """
    Create a step-by-step migration plan
    """
    config_name = get_optimal_config_for_data_size(current_record_count)
    estimate = estimate_processing_time(current_record_count, config_name)
    
    print("\n📋 MIGRATION PLAN")
    print("=" * 50)
    print(f"Current dataset size: {current_record_count:,} records")
    print(f"Recommended config: {config_name.replace('_', ' ').title()}")
    print(f"Expected processing time: {estimate['estimated_time_minutes']:.1f} minutes")
    print(f"Expected throughput: {estimate['estimated_throughput']:,.0f} records/second")
    
    print("\n🗺️ STEP-BY-STEP MIGRATION:")
    print("1. 🧪 Run small test (completed above)")
    print("2. 📋 Update Redis configuration with your password")
    print("3. 🔧 Choose appropriate Databricks cluster configuration:")
    
    from databricks_config import DATABRICKS_CONFIGS
    recommended_config = DATABRICKS_CONFIGS[config_name]
    print(f"   - Cluster Type: {recommended_config['cluster_type']}")
    if 'node_type' in recommended_config:
        print(f"   - Node Type: {recommended_config['node_type']}")
    if 'workers' in recommended_config and recommended_config['workers'] > 0:
        print(f"   - Workers: {recommended_config['workers']}")
    
    print("4. 🚀 Deploy optimized code:")
    print("   - Replace redis.py with redis_optimized.py")
    print("   - Update batch_size in configuration")
    print("   - Test with subset of data first")
    
    print("5. 📊 Monitor performance:")
    print("   - Check Redis CPU/memory usage")
    print("   - Monitor Spark job metrics")
    print("   - Validate data integrity")
    
    print("\n⚠️ ROLLBACK PLAN:")
    print("- Keep original redis.py as backup")
    print("- Monitor first few runs closely")
    print("- Have Redis monitoring in place")

def main():
    """
    Main migration analysis and planning
    """
    print("🚀 REDIS TIME SERIES OPTIMIZATION MIGRATION")
    print("=" * 60)
    
    # Performance comparison
    compare_performance_estimates()
    
    # Run validation test
    print("\n" + "="*60)
    test_passed = run_small_test()
    
    if test_passed:
        # Create migration plan
        # Estimate current dataset size (you can modify this)
        estimated_current_size = 5_000_000  # Adjust based on your typical data volume
        create_migration_plan(estimated_current_size)
        
        print("\n🎯 NEXT STEPS:")
        print("1. Update AZURE_REDIS_CONFIG with your Redis password")
        print("2. Choose appropriate cluster configuration from databricks_config.py")
        print("3. Run redis_optimized.py with a small subset first")
        print("4. Scale up to full dataset once validated")
        
    else:
        print("\n❌ MIGRATION NOT READY:")
        print("- Fix Redis connection issues first")
        print("- Verify Azure Redis configuration")
        print("- Check network connectivity from Databricks")

if __name__ == "__main__":
    main()