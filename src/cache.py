import redis
import json
import logging
from typing import Any, Optional, Union
from datetime import timedelta
from dotenv import load_dotenv
import os

logger = logging.getLogger(__name__)

class RedisCache:
    def __init__(self):
        load_dotenv()
        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=int(os.getenv('REDIS_DB', 0)),
            decode_responses=True
        )

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        try:
            value = self.redis_client.get(key)
            return json.loads(value) if value else None
        except Exception as e:
            logger.error(f"Error getting value from cache: {str(e)}")
            return None

    def set(self, key: str, value: Any, expire: Optional[Union[int, timedelta]] = None) -> bool:
        """Set value in cache with optional expiration."""
        try:
            serialized_value = json.dumps(value)
            return self.redis_client.set(key, serialized_value, ex=expire)
        except Exception as e:
            logger.error(f"Error setting value in cache: {str(e)}")
            return False

    def delete(self, key: str) -> bool:
        """Delete value from cache."""
        try:
            return bool(self.redis_client.delete(key))
        except Exception as e:
            logger.error(f"Error deleting value from cache: {str(e)}")
            return False

    def exists(self, key: str) -> bool:
        """Check if key exists in cache."""
        try:
            return bool(self.redis_client.exists(key))
        except Exception as e:
            logger.error(f"Error checking key existence in cache: {str(e)}")
            return False

    def clear_pattern(self, pattern: str) -> int:
        """Clear all keys matching pattern."""
        try:
            keys = self.redis_client.keys(pattern)
            if keys:
                return self.redis_client.delete(*keys)
            return 0
        except Exception as e:
            logger.error(f"Error clearing pattern from cache: {str(e)}")
            return 0

    def get_or_set(self, key: str, func: callable, expire: Optional[Union[int, timedelta]] = None) -> Any:
        """Get value from cache or set it using provided function."""
        value = self.get(key)
        if value is None:
            value = func()
            self.set(key, value, expire)
        return value

    def invalidate_by_prefix(self, prefix: str) -> int:
        """Invalidate all cache entries with given prefix."""
        return self.clear_pattern(f"{prefix}:*")

# Create global cache instance
cache = RedisCache() 