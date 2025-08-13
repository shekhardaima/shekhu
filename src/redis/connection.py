"""
Redis connection management utilities.
"""
import redis
from typing import Dict, Any
from ..utils.logging_config import get_default_logger

logger = get_default_logger(__name__)


class RedisConnectionManager:
    """Manages Redis connections with SSL support and connection pooling."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Redis connection manager.
        
        Args:
            config: Redis configuration dictionary
        """
        self.config = config
        self._client = None
        self._pool = None
    
    def _create_connection_pool(self) -> redis.ConnectionPool:
        """Create a Redis connection pool with SSL support."""
        pool_config = {
            "host": self.config["host"],
            "port": self.config["port"],
            "password": self.config["password"],
            "connection_class": redis.SSLConnection,
            "max_connections": self.config.get("max_connections", 50),
            "socket_timeout": self.config.get("socket_timeout", 30),
            "socket_connect_timeout": self.config.get("socket_connect_timeout", 10),
            "retry_on_timeout": self.config.get("retry_on_timeout", True),
            "health_check_interval": self.config.get("health_check_interval", 30),
        }
        
        return redis.ConnectionPool(**pool_config)
    
    def get_client(self) -> redis.Redis:
        """
        Get a Redis client instance.
        
        Returns:
            Redis client with connection pooling
        """
        if self._client is None:
            if self._pool is None:
                self._pool = self._create_connection_pool()
            self._client = redis.StrictRedis(connection_pool=self._pool, decode_responses=True)
            logger.info("Redis client created successfully")
        
        return self._client
    
    def test_connection(self) -> bool:
        """
        Test the Redis connection.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            client = self.get_client()
            client.ping()
            logger.info("Redis connection test successful")
            return True
        except Exception as e:
            logger.error(f"Redis connection test failed: {e}")
            return False
    
    def close(self):
        """Close the Redis connection and cleanup resources."""
        if self._client:
            self._client.close()
            self._client = None
        if self._pool:
            self._pool.disconnect()
            self._pool = None
        logger.info("Redis connection closed")


def get_redis_client(config: Dict[str, Any]) -> redis.Redis:
    """
    Create and return a Redis client (legacy function for backward compatibility).
    
    Args:
        config: Redis configuration dictionary
    
    Returns:
        Redis client instance
    """
    manager = RedisConnectionManager(config)
    return manager.get_client()