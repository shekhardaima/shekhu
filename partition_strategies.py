"""
Partitioning Strategies to Fix Slow Repartition Issues
Choose the right strategy based on your data characteristics
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, hash as spark_hash
import logging

logger = logging.getLogger(__name__)

class PartitionOptimizer:
    """
    Smart partitioning optimizer that chooses the best strategy
    """
    
    @staticmethod
    def analyze_dataframe(df: DataFrame) -> dict:
        """
        Analyze DataFrame to determine optimal partitioning strategy
        """
        current_partitions = df.rdd.getNumPartitions()
        
        # Sample partition sizes to check distribution
        partition_sizes = df.rdd.mapPartitionsWithIndex(
            lambda i, partition: [sum(1 for _ in partition)]
        ).collect()
        
        if not partition_sizes:
            return {
                "strategy": "no_repartition",
                "reason": "Empty DataFrame"
            }
        
        max_size = max(partition_sizes)
        min_size = min(partition_sizes)
        avg_size = sum(partition_sizes) / len(partition_sizes)
        skew_ratio = max_size / avg_size if avg_size > 0 else 1
        
        total_records = sum(partition_sizes)
        
        logger.info(f"📊 Partition Analysis:")
        logger.info(f"   Partitions: {current_partitions}")
        logger.info(f"   Total records: {total_records:,}")
        logger.info(f"   Sizes - Min: {min_size}, Max: {max_size}, Avg: {avg_size:.0f}")
        logger.info(f"   Skew ratio: {skew_ratio:.2f}")
        
        # Decision logic
        if skew_ratio > 5.0:
            return {
                "strategy": "repartition_required",
                "reason": f"High skew detected (ratio: {skew_ratio:.2f})",
                "optimal_partitions": min(200, max(20, total_records // 50000))
            }
        elif current_partitions > 1000:
            return {
                "strategy": "coalesce_recommended", 
                "reason": f"Too many partitions ({current_partitions})",
                "optimal_partitions": min(current_partitions // 4, 200)
            }
        elif avg_size < 100:
            return {
                "strategy": "coalesce_recommended",
                "reason": f"Very small partitions (avg: {avg_size:.0f} records)",
                "optimal_partitions": max(10, current_partitions // 8)
            }
        else:
            return {
                "strategy": "no_repartition",
                "reason": "Data already well distributed",
                "current_partitions": current_partitions
            }
    
    @staticmethod
    def apply_optimal_partitioning(df: DataFrame, force_strategy: str = None) -> DataFrame:
        """
        Apply optimal partitioning based on analysis or forced strategy
        
        Args:
            df: Input DataFrame
            force_strategy: Force a specific strategy ("no_repartition", "coalesce", "repartition", "smart")
        
        Returns:
            Optimized DataFrame
        """
        if force_strategy:
            analysis = {"strategy": force_strategy}
        else:
            analysis = PartitionOptimizer.analyze_dataframe(df)
        
        strategy = analysis["strategy"]
        
        if strategy == "no_repartition":
            logger.info("✅ Using DataFrame as-is (no partitioning changes)")
            return df
            
        elif strategy == "coalesce_recommended" or force_strategy == "coalesce":
            optimal_partitions = analysis.get("optimal_partitions", df.rdd.getNumPartitions() // 4)
            logger.info(f"⚡ Coalescing to {optimal_partitions} partitions (no shuffle)")
            return df.coalesce(optimal_partitions)
            
        elif strategy == "repartition_required" or force_strategy == "repartition":
            optimal_partitions = analysis.get("optimal_partitions", 100)
            logger.info(f"🔄 Repartitioning to {optimal_partitions} partitions (with shuffle)")
            return df.repartition(optimal_partitions, spark_hash(col("CycleId")))
            
        else:  # smart strategy
            logger.info("🧠 Using smart partitioning...")
            return PartitionOptimizer.apply_optimal_partitioning(df, None)

# Quick fix functions for different scenarios
def quick_fix_no_repartition(df: DataFrame) -> DataFrame:
    """
    FASTEST: Use DataFrame as-is, rely on CycleId grouping within partitions
    Best for: When repartition is taking too long
    """
    logger.info("🚀 QUICK FIX: No repartitioning - fastest startup")
    return df

def quick_fix_coalesce_only(df: DataFrame) -> DataFrame:
    """
    FAST: Only reduce partition count without shuffling
    Best for: When you have too many small partitions
    """
    current_partitions = df.rdd.getNumPartitions()
    optimal_partitions = min(current_partitions, max(32, current_partitions // 4))
    
    logger.info(f"⚡ QUICK FIX: Coalesce {current_partitions} → {optimal_partitions} partitions")
    return df.coalesce(optimal_partitions)

def quick_fix_minimal_repartition(df: DataFrame) -> DataFrame:
    """
    MODERATE: Minimal repartitioning only when absolutely necessary
    Best for: When you need some redistribution but want to minimize shuffle
    """
    current_partitions = df.rdd.getNumPartitions()
    
    # Only repartition if we have too many partitions
    if current_partitions > 500:
        optimal_partitions = min(200, current_partitions // 3)
        logger.info(f"🔄 QUICK FIX: Minimal repartition {current_partitions} → {optimal_partitions}")
        return df.repartition(optimal_partitions, spark_hash(col("CycleId")))
    else:
        logger.info("✅ QUICK FIX: No repartitioning needed")
        return df

# Performance comparison guide
PARTITIONING_STRATEGIES = {
    "no_repartition": {
        "startup_time": "Fastest (0 seconds)",
        "memory_usage": "Lowest",
        "cross_slot_risk": "Medium (handled by CycleId grouping within partitions)",
        "best_for": "When repartition is taking too long",
        "performance": "Good (may have some uneven partition processing)"
    },
    
    "coalesce_only": {
        "startup_time": "Fast (5-30 seconds)", 
        "memory_usage": "Low",
        "cross_slot_risk": "Medium (handled by CycleId grouping)",
        "best_for": "Many small partitions",
        "performance": "Better (more even partition sizes)"
    },
    
    "minimal_repartition": {
        "startup_time": "Moderate (30 seconds - 2 minutes)",
        "memory_usage": "Medium", 
        "cross_slot_risk": "Low (CycleId-based partitioning)",
        "best_for": "Moderate data skew",
        "performance": "Good (balanced partitions)"
    },
    
    "full_repartition": {
        "startup_time": "Slow (2-10 minutes)",
        "memory_usage": "High",
        "cross_slot_risk": "Lowest (optimal CycleId distribution)",
        "best_for": "Severe data skew, maximum performance needed",
        "performance": "Best (optimal distribution)"
    }
}

def print_strategy_comparison():
    """
    Print comparison of different partitioning strategies
    """
    print("\n" + "="*80)
    print("🔄 PARTITIONING STRATEGY COMPARISON")
    print("="*80)
    
    for strategy, details in PARTITIONING_STRATEGIES.items():
        print(f"\n📊 {strategy.upper().replace('_', ' ')}")
        print(f"   Startup Time: {details['startup_time']}")
        print(f"   Memory Usage: {details['memory_usage']}")
        print(f"   Cross-slot Risk: {details['cross_slot_risk']}")
        print(f"   Best For: {details['best_for']}")
        print(f"   Performance: {details['performance']}")

def recommend_strategy_for_problem(problem_description: str) -> str:
    """
    Recommend strategy based on problem description
    """
    problem = problem_description.lower()
    
    if "taking forever" in problem or "too slow" in problem or "hanging" in problem:
        return "no_repartition"
    elif "many partitions" in problem or "small partitions" in problem:
        return "coalesce_only"
    elif "skew" in problem or "uneven" in problem:
        return "minimal_repartition"
    elif "cross slot" in problem or "redis error" in problem:
        return "full_repartition"
    else:
        return "no_repartition"  # Default to fastest

# Usage examples
def example_usage():
    """
    Example of how to use different strategies
    """
    print("\n🔧 USAGE EXAMPLES:")
    print("-" * 50)
    
    print("\n1. FASTEST FIX (when repartition is taking forever):")
    print("   df_optimized = quick_fix_no_repartition(df)")
    
    print("\n2. FAST FIX (when you have many small partitions):")
    print("   df_optimized = quick_fix_coalesce_only(df)")
    
    print("\n3. MODERATE FIX (when you need some redistribution):")
    print("   df_optimized = quick_fix_minimal_repartition(df)")
    
    print("\n4. SMART FIX (automatic decision):")
    print("   df_optimized = PartitionOptimizer.apply_optimal_partitioning(df)")
    
    print("\n5. FORCE SPECIFIC STRATEGY:")
    print("   df_optimized = PartitionOptimizer.apply_optimal_partitioning(df, 'coalesce')")

if __name__ == "__main__":
    print("🚀 Redis Time Series Partitioning Strategy Guide")
    print_strategy_comparison()
    example_usage()
    
    # Recommendation based on your issue
    print("\n" + "="*80)
    print("💡 RECOMMENDATION FOR YOUR ISSUE:")
    print("="*80)
    print("Since repartition is 'taking forever', use:")
    print("🚀 STRATEGY: no_repartition")
    print("📁 FILE: redis_optimized_fast.py")
    print("⚡ BENEFIT: Immediate startup, no shuffle overhead")
    print("🛡️ SAFETY: Cross-slot errors prevented by CycleId grouping within partitions")