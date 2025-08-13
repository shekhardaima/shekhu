"""
Redis configuration settings.
"""
from typing import Dict, Any
import os


def get_redis_config() -> Dict[str, Any]:
    """
    Get Redis configuration from environment variables or defaults.
    
    Returns:
        Dictionary containing Redis configuration
    """
    return {
        "host": os.getenv("REDIS_HOST", 'rc-cvdev-03.westeurope.redis.azure.net'),
        "port": int(os.getenv("REDIS_PORT", 10000)),
        "password": os.getenv("REDIS_PASSWORD", 'UoCtYTtWECibVStp6oIFlwveUScgKC10nAzCaB5GXbA='),
        "ssl": os.getenv("REDIS_SSL", "true").lower() == "true",
        "max_connections": int(os.getenv("REDIS_MAX_CONNECTIONS", 50)),
        "socket_timeout": int(os.getenv("REDIS_SOCKET_TIMEOUT", 30)),
        "socket_connect_timeout": int(os.getenv("REDIS_SOCKET_CONNECT_TIMEOUT", 10)),
        "retry_on_timeout": os.getenv("REDIS_RETRY_ON_TIMEOUT", "true").lower() == "true",
        "health_check_interval": int(os.getenv("REDIS_HEALTH_CHECK_INTERVAL", 30)),
    }


def get_timeseries_config() -> Dict[str, Any]:
    """
    Get TimeSeries-specific configuration.
    
    Returns:
        Dictionary containing TimeSeries configuration
    """
    return {
        "retention": int(os.getenv("REDIS_TS_RETENTION", 2592000000)),  # 30 days in milliseconds
        "chunk_size": int(os.getenv("REDIS_TS_CHUNK_SIZE", 4096)),
        "duplicate_policy": os.getenv("REDIS_TS_DUPLICATE_POLICY", "LAST"),
    }


# Default configuration for backward compatibility
redis_config = get_redis_config()