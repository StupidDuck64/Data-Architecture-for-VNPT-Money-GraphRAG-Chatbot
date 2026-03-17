"""
Redis Manager - Quản lý kết nối và operations với Redis
========================================================

Cung cấp:
- Connection pooling
- Session management
- Caching layer
- Rate limiting support
- A/B testing support
- Monitoring metrics storage
"""

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, asdict
from enum import Enum

logger = logging.getLogger(__name__)


@dataclass
class RedisConfig:
    """Cấu hình Redis"""
    url: str = "redis://localhost:6379"
    max_connections: int = 50
    socket_timeout: float = 5.0
    socket_connect_timeout: float = 5.0
    retry_on_timeout: bool = True
    health_check_interval: int = 30
    
    # Key prefixes
    prefix_session: str = "session:"
    prefix_cache: str = "cache:"
    prefix_rate_limit: str = "ratelimit:"
    prefix_ab_test: str = "abtest:"
    prefix_metrics: str = "metrics:"
    prefix_chat_history: str = "chat_history:"
    
    # TTLs (seconds)
    ttl_session: int = 1800      # 30 minutes
    ttl_cache: int = 3600        # 1 hour
    ttl_rate_limit: int = 60     # 1 minute
    ttl_metrics: int = 86400     # 24 hours
    ttl_chat_history: int = 1800 # 30 minutes (same as session)


class RedisManager:
    """
    Singleton Redis Manager với connection pooling và automatic reconnect.
    """
    
    _instance = None
    _redis = None
    _config: RedisConfig = None
    _connected: bool = False
    _last_health_check: float = 0
    
    def __new__(cls, config: RedisConfig = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._config = config or RedisConfig()
        return cls._instance
    
    def __init__(self, config: RedisConfig = None):
        if config:
            self._config = config
        if self._redis is None:
            self._connect()
    
    def _connect(self) -> bool:
        """Tạo kết nối Redis với connection pool."""
        try:
            import redis
            from redis import ConnectionPool
            
            # Create connection pool
            pool = ConnectionPool.from_url(
                self._config.url,
                max_connections=self._config.max_connections,
                socket_timeout=self._config.socket_timeout,
                socket_connect_timeout=self._config.socket_connect_timeout,
                retry_on_timeout=self._config.retry_on_timeout,
                health_check_interval=self._config.health_check_interval
            )
            
            self._redis = redis.Redis(connection_pool=pool, decode_responses=True)
            
            # Test connection
            self._redis.ping()
            self._connected = True
            self._last_health_check = time.time()
            
            logger.info("Redis connected successfully")
            return True
            
        except ImportError:
            logger.warning("Redis package not installed. Running without Redis.")
            self._connected = False
            return False
        except Exception as e:
            logger.warning(f"Redis connection failed: {e}. Running without Redis.")
            self._connected = False
            return False
    
    def _ensure_connection(self) -> bool:
        """Kiểm tra và reconnect nếu cần."""
        if not self._connected:
            return self._connect()
        
        # Health check mỗi 30 giây
        if time.time() - self._last_health_check > self._config.health_check_interval:
            try:
                self._redis.ping()
                self._last_health_check = time.time()
            except Exception:
                self._connected = False
                return self._connect()
        
        return True
    
    @property
    def is_connected(self) -> bool:
        """Kiểm tra trạng thái kết nối."""
        return self._connected and self._ensure_connection()
    
    @property
    def client(self):
        """Lấy Redis client."""
        if self._ensure_connection():
            return self._redis
        return None
    
    # ==================== Session Operations ====================
    
    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Lấy session data."""
        if not self.is_connected:
            return None
        
        try:
            key = f"{self._config.prefix_session}{session_id}"
            data = self._redis.get(key)
            return json.loads(data) if data else None
        except Exception as e:
            logger.error(f"Redis get_session error: {e}")
            return None
    
    def set_session(self, session_id: str, data: Dict[str, Any]) -> bool:
        """Lưu session data."""
        if not self.is_connected:
            return False
        
        try:
            key = f"{self._config.prefix_session}{session_id}"
            self._redis.setex(key, self._config.ttl_session, json.dumps(data))
            return True
        except Exception as e:
            logger.error(f"Redis set_session error: {e}")
            return False
    
    def update_session(self, session_id: str, updates: Dict[str, Any]) -> bool:
        """Cập nhật một phần session data."""
        current = self.get_session(session_id) or {}
        current.update(updates)
        return self.set_session(session_id, current)
    
    def delete_session(self, session_id: str) -> bool:
        """Xóa session."""
        if not self.is_connected:
            return False
        
        try:
            key = f"{self._config.prefix_session}{session_id}"
            self._redis.delete(key)
            return True
        except Exception as e:
            logger.error(f"Redis delete_session error: {e}")
            return False
    
    def extend_session_ttl(self, session_id: str) -> bool:
        """Gia hạn TTL cho session."""
        if not self.is_connected:
            return False
        
        try:
            key = f"{self._config.prefix_session}{session_id}"
            return self._redis.expire(key, self._config.ttl_session)
        except Exception as e:
            logger.error(f"Redis extend_session_ttl error: {e}")
            return False
    
    # ==================== Cache Operations ====================
    
    def cache_get(self, cache_key: str) -> Optional[Any]:
        """Lấy giá trị từ cache."""
        if not self.is_connected:
            return None
        
        try:
            key = f"{self._config.prefix_cache}{cache_key}"
            data = self._redis.get(key)
            return json.loads(data) if data else None
        except Exception as e:
            logger.error(f"Redis cache_get error: {e}")
            return None
    
    def cache_set(self, cache_key: str, value: Any, ttl: int = None) -> bool:
        """Lưu giá trị vào cache."""
        if not self.is_connected:
            return False
        
        try:
            key = f"{self._config.prefix_cache}{cache_key}"
            ttl = ttl or self._config.ttl_cache
            self._redis.setex(key, ttl, json.dumps(value))
            return True
        except Exception as e:
            logger.error(f"Redis cache_set error: {e}")
            return False
    
    def cache_delete(self, cache_key: str) -> bool:
        """Xóa cache entry."""
        if not self.is_connected:
            return False
        
        try:
            key = f"{self._config.prefix_cache}{cache_key}"
            self._redis.delete(key)
            return True
        except Exception as e:
            logger.error(f"Redis cache_delete error: {e}")
            return False
    
    def cache_invalidate_pattern(self, pattern: str) -> int:
        """Xóa tất cả cache entries khớp pattern."""
        if not self.is_connected:
            return 0
        
        try:
            full_pattern = f"{self._config.prefix_cache}{pattern}"
            keys = self._redis.keys(full_pattern)
            if keys:
                return self._redis.delete(*keys)
            return 0
        except Exception as e:
            logger.error(f"Redis cache_invalidate_pattern error: {e}")
            return 0
    
    # ==================== Chat History Operations ====================
    
    def get_chat_history(self, session_id: str, max_messages: int = 10) -> List[Dict[str, str]]:
        """
        Lấy chat history cho session.
        
        Args:
            session_id: Session ID
            max_messages: Số lượng messages tối đa trả về
            
        Returns:
            List of {"role": "user"|"assistant", "content": str}
        """
        if not self.is_connected:
            return []
        
        try:
            key = f"{self._config.prefix_chat_history}{session_id}"
            # Lấy tất cả messages từ list (newest first)
            data = self._redis.lrange(key, 0, max_messages * 2 - 1)
            # Reverse để có oldest first
            messages = []
            for item in reversed(data):
                try:
                    messages.append(json.loads(item))
                except json.JSONDecodeError:
                    continue
            return messages[-max_messages * 2:]  # Limit to max_messages pairs
        except Exception as e:
            logger.error(f"Redis get_chat_history error: {e}")
            return []
    
    def add_chat_message(self, session_id: str, role: str, content: str) -> bool:
        """
        Thêm một message vào chat history.
        
        Args:
            session_id: Session ID
            role: "user" hoặc "assistant"
            content: Nội dung message
            
        Returns:
            True nếu thành công
        """
        if not self.is_connected:
            return False
        
        try:
            key = f"{self._config.prefix_chat_history}{session_id}"
            message = json.dumps({"role": role, "content": content})
            # Push vào đầu list (newest first)
            self._redis.lpush(key, message)
            # Set TTL
            self._redis.expire(key, self._config.ttl_chat_history)
            # Trim để giữ max 20 messages (10 pairs)
            self._redis.ltrim(key, 0, 19)
            return True
        except Exception as e:
            logger.error(f"Redis add_chat_message error: {e}")
            return False
    
    def update_chat_history(
        self, 
        session_id: str, 
        user_message: str, 
        assistant_message: str
    ) -> bool:
        """
        Thêm cả user và assistant message vào history.
        
        Args:
            session_id: Session ID
            user_message: Tin nhắn của user
            assistant_message: Tin nhắn của assistant
            
        Returns:
            True nếu thành công
        """
        if not self.is_connected:
            return False
        
        try:
            key = f"{self._config.prefix_chat_history}{session_id}"
            # Push assistant first, then user (so when reversed: user, assistant)
            pipe = self._redis.pipeline()
            pipe.lpush(key, json.dumps({"role": "assistant", "content": assistant_message}))
            pipe.lpush(key, json.dumps({"role": "user", "content": user_message}))
            pipe.expire(key, self._config.ttl_chat_history)
            pipe.ltrim(key, 0, 19)  # Keep max 20 messages
            pipe.execute()
            return True
        except Exception as e:
            logger.error(f"Redis update_chat_history error: {e}")
            return False
    
    def clear_chat_history(self, session_id: str) -> bool:
        """Xóa chat history cho session."""
        if not self.is_connected:
            return False
        
        try:
            key = f"{self._config.prefix_chat_history}{session_id}"
            self._redis.delete(key)
            return True
        except Exception as e:
            logger.error(f"Redis clear_chat_history error: {e}")
            return False
    
    # ==================== Counter Operations ====================
    
    def incr(self, key: str, amount: int = 1) -> int:
        """Increment counter."""
        if not self.is_connected:
            return -1
        
        try:
            return self._redis.incr(key, amount)
        except Exception as e:
            logger.error(f"Redis incr error: {e}")
            return -1
    
    def get_counter(self, key: str) -> int:
        """Get counter value."""
        if not self.is_connected:
            return 0
        
        try:
            val = self._redis.get(key)
            return int(val) if val else 0
        except Exception as e:
            logger.error(f"Redis get_counter error: {e}")
            return 0
    
    # ==================== List Operations ====================
    
    def list_push(self, key: str, *values, ttl: int = None) -> int:
        """Push values to list."""
        if not self.is_connected:
            return 0
        
        try:
            count = self._redis.rpush(key, *[json.dumps(v) if not isinstance(v, str) else v for v in values])
            if ttl:
                self._redis.expire(key, ttl)
            return count
        except Exception as e:
            logger.error(f"Redis list_push error: {e}")
            return 0
    
    def list_range(self, key: str, start: int = 0, end: int = -1) -> List[Any]:
        """Get list range."""
        if not self.is_connected:
            return []
        
        try:
            values = self._redis.lrange(key, start, end)
            result = []
            for v in values:
                try:
                    result.append(json.loads(v))
                except:
                    result.append(v)
            return result
        except Exception as e:
            logger.error(f"Redis list_range error: {e}")
            return []
    
    def list_trim(self, key: str, start: int, end: int) -> bool:
        """Trim list to specified range."""
        if not self.is_connected:
            return False
        
        try:
            self._redis.ltrim(key, start, end)
            return True
        except Exception as e:
            logger.error(f"Redis list_trim error: {e}")
            return False
    
    # ==================== Hash Operations ====================
    
    def hash_set(self, key: str, field: str, value: Any) -> bool:
        """Set hash field."""
        if not self.is_connected:
            return False
        
        try:
            self._redis.hset(key, field, json.dumps(value) if not isinstance(value, (str, int, float)) else value)
            return True
        except Exception as e:
            logger.error(f"Redis hash_set error: {e}")
            return False
    
    def hash_get(self, key: str, field: str) -> Optional[Any]:
        """Get hash field."""
        if not self.is_connected:
            return None
        
        try:
            value = self._redis.hget(key, field)
            if value is None:
                return None
            try:
                return json.loads(value)
            except:
                return value
        except Exception as e:
            logger.error(f"Redis hash_get error: {e}")
            return None
    
    def hash_get_all(self, key: str) -> Dict[str, Any]:
        """Get all hash fields."""
        if not self.is_connected:
            return {}
        
        try:
            data = self._redis.hgetall(key)
            result = {}
            for k, v in data.items():
                try:
                    result[k] = json.loads(v)
                except:
                    result[k] = v
            return result
        except Exception as e:
            logger.error(f"Redis hash_get_all error: {e}")
            return {}
    
    def hash_incr(self, key: str, field: str, amount: int = 1) -> int:
        """Increment hash field."""
        if not self.is_connected:
            return -1
        
        try:
            return self._redis.hincrby(key, field, amount)
        except Exception as e:
            logger.error(f"Redis hash_incr error: {e}")
            return -1
    
    # ==================== Pub/Sub Operations ====================
    
    def publish(self, channel: str, message: Any) -> int:
        """Publish message to channel."""
        if not self.is_connected:
            return 0
        
        try:
            msg = json.dumps(message) if not isinstance(message, str) else message
            return self._redis.publish(channel, msg)
        except Exception as e:
            logger.error(f"Redis publish error: {e}")
            return 0
    
    # ==================== Utility Operations ====================
    
    def exists(self, key: str) -> bool:
        """Check if key exists."""
        if not self.is_connected:
            return False
        
        try:
            return bool(self._redis.exists(key))
        except Exception as e:
            logger.error(f"Redis exists error: {e}")
            return False
    
    def expire(self, key: str, ttl: int) -> bool:
        """Set TTL for key."""
        if not self.is_connected:
            return False
        
        try:
            return self._redis.expire(key, ttl)
        except Exception as e:
            logger.error(f"Redis expire error: {e}")
            return False
    
    def ttl(self, key: str) -> int:
        """Get TTL for key."""
        if not self.is_connected:
            return -1
        
        try:
            return self._redis.ttl(key)
        except Exception as e:
            logger.error(f"Redis ttl error: {e}")
            return -1
    
    def delete(self, *keys: str) -> int:
        """Delete keys."""
        if not self.is_connected:
            return 0
        
        try:
            return self._redis.delete(*keys)
        except Exception as e:
            logger.error(f"Redis delete error: {e}")
            return 0
    
    def close(self):
        """Đóng kết nối Redis."""
        if self._redis:
            try:
                self._redis.close()
            except Exception:
                pass
            self._redis = None
            self._connected = False
            logger.info("Redis connection closed")


# ==================== Global Instance ====================

_redis_manager: Optional[RedisManager] = None


def get_redis_manager(config: RedisConfig = None) -> RedisManager:
    """Get global Redis manager instance."""
    global _redis_manager
    if _redis_manager is None:
        _redis_manager = RedisManager(config)
    return _redis_manager


def init_redis(url: str = None, **kwargs) -> RedisManager:
    """Initialize Redis with URL."""
    config = RedisConfig(url=url, **kwargs) if url else RedisConfig(**kwargs)
    return get_redis_manager(config)
