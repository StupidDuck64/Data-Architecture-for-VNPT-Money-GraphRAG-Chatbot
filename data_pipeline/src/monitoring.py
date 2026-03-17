"""
Cung cấp:
- Metrics collection (counters, gauges, histograms)
- Real-time monitoring
- Health checks
- Performance tracking
- Dashboard data API
"""

import time
import logging
import json
import statistics
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import defaultdict
import threading

from redis_manager import get_redis_manager, RedisManager

logger = logging.getLogger(__name__)


class MetricType(str, Enum):
    """Loại metric."""
    COUNTER = "counter"       # Đếm (luôn tăng)
    GAUGE = "gauge"           # Giá trị hiện tại (có thể tăng/giảm)
    HISTOGRAM = "histogram"   # Phân phối giá trị
    SUMMARY = "summary"       # Tóm tắt (mean, percentiles)


@dataclass
class MetricPoint:
    """Một điểm dữ liệu metric."""
    name: str
    value: float
    timestamp: float = field(default_factory=time.time)
    labels: Dict[str, str] = field(default_factory=dict)
    metric_type: MetricType = MetricType.GAUGE


@dataclass
class HealthStatus:
    """Trạng thái health của một component."""
    name: str
    healthy: bool
    message: str = ""
    latency_ms: float = 0
    last_check: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DashboardStats:
    """Thống kê cho dashboard."""
    # Request metrics
    total_requests: int = 0
    requests_per_minute: float = 0
    requests_last_hour: int = 0
    
    # Performance metrics
    avg_latency_ms: float = 0
    p50_latency_ms: float = 0
    p95_latency_ms: float = 0
    p99_latency_ms: float = 0
    
    # Decision metrics
    direct_answer_rate: float = 0
    clarification_rate: float = 0
    escalation_rate: float = 0
    
    # Confidence metrics
    avg_confidence: float = 0
    high_confidence_rate: float = 0
    low_confidence_rate: float = 0
    
    # Error metrics
    error_rate: float = 0
    error_count: int = 0
    
    # User metrics
    active_sessions: int = 0
    unique_users_today: int = 0
    
    # System health
    neo4j_healthy: bool = True
    redis_healthy: bool = True
    openai_healthy: bool = True
    
    # Uptime
    uptime_seconds: float = 0
    
    # Time range
    period_start: str = ""
    period_end: str = ""


class MetricsCollector:
    """
    Thu thập và lưu trữ metrics.
    
    Supports:
    - In-memory storage (fallback)
    - Redis storage (persistent)
    - Aggregations (sum, avg, percentiles)
    """
    
    def __init__(self, redis_manager: RedisManager = None, retention_hours: int = 24):
        self.redis = redis_manager or get_redis_manager()
        self.retention_hours = retention_hours
        
        # In-memory storage (fallback và cho real-time)
        self._counters: Dict[str, int] = defaultdict(int)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, List[float]] = defaultdict(list)
        self._timestamps: Dict[str, List[float]] = defaultdict(list)
        
        # Lock for thread safety
        self._lock = threading.Lock()
        
        # Histogram buckets
        self.latency_buckets = [10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]
    
    # ==================== Counter Operations ====================
    
    def increment(self, name: str, value: int = 1, labels: Dict[str, str] = None) -> int:
        """Tăng counter."""
        key = self._make_key(name, labels)
        
        with self._lock:
            self._counters[key] += value
            current = self._counters[key]
        
        # Save to Redis
        if self.redis.is_connected:
            redis_key = f"metrics:counter:{key}"
            self.redis.client.incrby(redis_key, value)
            self.redis.client.expire(redis_key, self.retention_hours * 3600)
        
        return current
    
    def get_counter(self, name: str, labels: Dict[str, str] = None) -> int:
        """Lấy giá trị counter."""
        key = self._make_key(name, labels)
        
        if self.redis.is_connected:
            redis_key = f"metrics:counter:{key}"
            val = self.redis.client.get(redis_key)
            return int(val) if val else 0
        
        return self._counters.get(key, 0)
    
    # ==================== Gauge Operations ====================
    
    def set_gauge(self, name: str, value: float, labels: Dict[str, str] = None) -> None:
        """Set giá trị gauge."""
        key = self._make_key(name, labels)
        
        with self._lock:
            self._gauges[key] = value
            self._timestamps[key].append(time.time())
            # Keep only recent values
            if len(self._timestamps[key]) > 1000:
                self._timestamps[key] = self._timestamps[key][-1000:]
        
        # Save to Redis
        if self.redis.is_connected:
            redis_key = f"metrics:gauge:{key}"
            self.redis.client.set(redis_key, value)
            self.redis.client.expire(redis_key, self.retention_hours * 3600)
            
            # Also save to time series
            ts_key = f"metrics:ts:{key}"
            self.redis.list_push(ts_key, {"value": value, "timestamp": time.time()})
            self.redis.list_trim(ts_key, -1000, -1)
    
    def get_gauge(self, name: str, labels: Dict[str, str] = None) -> float:
        """Lấy giá trị gauge."""
        key = self._make_key(name, labels)
        
        if self.redis.is_connected:
            redis_key = f"metrics:gauge:{key}"
            val = self.redis.client.get(redis_key)
            return float(val) if val else 0.0
        
        return self._gauges.get(key, 0.0)
    
    # ==================== Histogram Operations ====================
    
    def observe(self, name: str, value: float, labels: Dict[str, str] = None) -> None:
        """Record một observation cho histogram."""
        key = self._make_key(name, labels)
        
        with self._lock:
            self._histograms[key].append(value)
            # Keep only recent values
            if len(self._histograms[key]) > 10000:
                self._histograms[key] = self._histograms[key][-10000:]
        
        # Save to Redis
        if self.redis.is_connected:
            redis_key = f"metrics:histogram:{key}"
            self.redis.list_push(redis_key, value)
            self.redis.list_trim(redis_key, -10000, -1)
            self.redis.expire(redis_key, self.retention_hours * 3600)
    
    def get_histogram_stats(self, name: str, labels: Dict[str, str] = None) -> Dict[str, float]:
        """Lấy statistics từ histogram."""
        key = self._make_key(name, labels)
        
        # Get values
        values = []
        if self.redis.is_connected:
            redis_key = f"metrics:histogram:{key}"
            raw_values = self.redis.list_range(redis_key, 0, -1)
            values = [float(v) for v in raw_values if v]
        else:
            values = self._histograms.get(key, [])
        
        if not values:
            return {
                "count": 0,
                "min": 0,
                "max": 0,
                "mean": 0,
                "p50": 0,
                "p95": 0,
                "p99": 0
            }
        
        sorted_values = sorted(values)
        n = len(sorted_values)
        
        return {
            "count": n,
            "min": sorted_values[0],
            "max": sorted_values[-1],
            "mean": statistics.mean(values),
            "p50": sorted_values[int(n * 0.5)],
            "p95": sorted_values[int(n * 0.95)] if n >= 20 else sorted_values[-1],
            "p99": sorted_values[int(n * 0.99)] if n >= 100 else sorted_values[-1]
        }
    
    # ==================== Time Series Operations ====================
    
    def record_time_series(
        self,
        name: str,
        value: float,
        timestamp: float = None,
        labels: Dict[str, str] = None
    ) -> None:
        """Record một điểm time series."""
        key = self._make_key(name, labels)
        ts = timestamp or time.time()
        
        point = {"value": value, "timestamp": ts}
        
        if self.redis.is_connected:
            redis_key = f"metrics:ts:{key}"
            self.redis.list_push(redis_key, point)
            self.redis.list_trim(redis_key, -10000, -1)
            self.redis.expire(redis_key, self.retention_hours * 3600)
    
    def get_time_series(
        self,
        name: str,
        labels: Dict[str, str] = None,
        start_time: float = None,
        end_time: float = None,
        limit: int = 1000
    ) -> List[Dict[str, float]]:
        """Lấy time series data."""
        key = self._make_key(name, labels)
        
        if self.redis.is_connected:
            redis_key = f"metrics:ts:{key}"
            points = self.redis.list_range(redis_key, -limit, -1)
        else:
            points = []
        
        # Filter by time range
        if start_time or end_time:
            filtered = []
            for p in points:
                ts = p.get("timestamp", 0)
                if start_time and ts < start_time:
                    continue
                if end_time and ts > end_time:
                    continue
                filtered.append(p)
            points = filtered
        
        return points
    
    # ==================== Utility Methods ====================
    
    def _make_key(self, name: str, labels: Dict[str, str] = None) -> str:
        """Tạo key từ name và labels."""
        if not labels:
            return name
        
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"
    
    def clear(self, name: str = None) -> None:
        """Clear metrics."""
        with self._lock:
            if name:
                keys_to_remove = [k for k in self._counters if k.startswith(name)]
                for k in keys_to_remove:
                    self._counters.pop(k, None)
                    self._gauges.pop(k, None)
                    self._histograms.pop(k, None)
            else:
                self._counters.clear()
                self._gauges.clear()
                self._histograms.clear()


class HealthChecker:
    """
    Kiểm tra health của các components.
    """
    
    def __init__(self, redis_manager: RedisManager = None):
        self.redis = redis_manager or get_redis_manager()
        self._checks: Dict[str, callable] = {}
        self._last_results: Dict[str, HealthStatus] = {}
    
    def register_check(self, name: str, check_func: callable) -> None:
        """Đăng ký health check function."""
        self._checks[name] = check_func
    
    def check(self, name: str) -> HealthStatus:
        """Chạy health check cho một component."""
        if name not in self._checks:
            return HealthStatus(name=name, healthy=False, message="Check not registered")
        
        start = time.time()
        try:
            result = self._checks[name]()
            latency = (time.time() - start) * 1000
            
            if isinstance(result, bool):
                status = HealthStatus(
                    name=name,
                    healthy=result,
                    latency_ms=latency
                )
            elif isinstance(result, HealthStatus):
                status = result
                status.latency_ms = latency
            else:
                status = HealthStatus(
                    name=name,
                    healthy=bool(result),
                    latency_ms=latency
                )
            
        except Exception as e:
            latency = (time.time() - start) * 1000
            status = HealthStatus(
                name=name,
                healthy=False,
                message=str(e),
                latency_ms=latency
            )
        
        self._last_results[name] = status
        return status
    
    def check_all(self) -> Dict[str, HealthStatus]:
        """Chạy tất cả health checks."""
        results = {}
        for name in self._checks:
            results[name] = self.check(name)
        return results
    
    def get_overall_health(self) -> Tuple[bool, Dict[str, HealthStatus]]:
        """Kiểm tra overall health."""
        results = self.check_all()
        all_healthy = all(s.healthy for s in results.values())
        return all_healthy, results


class MonitoringDashboard:
    """
    Dashboard tổng hợp cho monitoring.
    """
    
    def __init__(
        self,
        redis_manager: RedisManager = None,
        neo4j_driver=None,
        openai_client=None
    ):
        self.redis = redis_manager or get_redis_manager()
        self.metrics = MetricsCollector(self.redis)
        self.health = HealthChecker(self.redis)
        self.start_time = time.time()  # Track service start time
        
        # Register health checks
        self._setup_health_checks(neo4j_driver, openai_client)
    
    def _setup_health_checks(self, neo4j_driver, openai_client):
        """Setup default health checks."""
        
        # Redis health check
        def check_redis():
            if self.redis.is_connected:
                self.redis.client.ping()
                return True
            return False
        
        self.health.register_check("redis", check_redis)
        
        # Neo4j health check
        if neo4j_driver:
            def check_neo4j():
                try:
                    neo4j_driver.verify_connectivity()
                    return True
                except:
                    return False
            self.health.register_check("neo4j", check_neo4j)
        
        # OpenAI health check
        if openai_client:
            def check_openai():
                try:
                    # Simple models list call
                    openai_client.models.list()
                    return True
                except:
                    return False
            self.health.register_check("openai", check_openai)
    
    # ==================== Metric Recording ====================
    
    def record_request(
        self,
        session_id: str,
        latency_ms: float,
        decision_type: str,
        confidence: float,
        success: bool = True
    ) -> None:
        """Record một request."""
        # Increment counters
        self.metrics.increment("requests_total")
        self.metrics.increment(f"requests_by_decision", labels={"decision": decision_type})
        
        if not success:
            self.metrics.increment("errors_total")
        
        # Record latency
        self.metrics.observe("latency_ms", latency_ms)
        self.metrics.observe(f"latency_ms_by_decision", latency_ms, labels={"decision": decision_type})
        
        # Record confidence
        self.metrics.observe("confidence_score", confidence)
        
        # Track unique sessions
        self._track_session(session_id)
        
        # Time series
        self.metrics.record_time_series("requests", 1)
        self.metrics.record_time_series("latency", latency_ms)
    
    def _track_session(self, session_id: str) -> None:
        """Track active session."""
        if self.redis.is_connected:
            key = f"active_sessions:{datetime.now().strftime('%Y%m%d')}"
            self.redis.client.sadd(key, session_id)
            self.redis.client.expire(key, 86400 * 2)  # 2 days
    
    def record_error(self, error_type: str, message: str = "") -> None:
        """Record một error."""
        self.metrics.increment("errors_total")
        self.metrics.increment("errors_by_type", labels={"type": error_type})
        
        # Log error
        logger.error(f"Recorded error: {error_type} - {message}")
    
    # ==================== Dashboard Data ====================
    
    def get_dashboard_stats(self, period_hours: int = 24) -> DashboardStats:
        """Lấy thống kê cho dashboard."""
        stats = DashboardStats()
        
        now = time.time()
        period_start = now - (period_hours * 3600)
        
        stats.period_start = datetime.fromtimestamp(period_start).isoformat()
        stats.period_end = datetime.fromtimestamp(now).isoformat()
        
        # Request metrics
        stats.total_requests = self.metrics.get_counter("requests_total")
        
        # Calculate requests per minute
        time_series = self.metrics.get_time_series("requests", start_time=now - 3600)
        stats.requests_last_hour = len(time_series)
        stats.requests_per_minute = len(time_series) / 60 if time_series else 0
        
        # Latency metrics
        latency_stats = self.metrics.get_histogram_stats("latency_ms")
        stats.avg_latency_ms = latency_stats.get("mean", 0)
        stats.p50_latency_ms = latency_stats.get("p50", 0)
        stats.p95_latency_ms = latency_stats.get("p95", 0)
        stats.p99_latency_ms = latency_stats.get("p99", 0)
        
        # Decision metrics
        total = stats.total_requests or 1
        direct_count = self.metrics.get_counter("requests_by_decision", labels={"decision": "direct_answer"})
        clarify_count = self.metrics.get_counter("requests_by_decision", labels={"decision": "clarify_required"})
        escalate_count = sum([
            self.metrics.get_counter("requests_by_decision", labels={"decision": "escalate_personal"}),
            self.metrics.get_counter("requests_by_decision", labels={"decision": "escalate_out_of_scope"}),
            self.metrics.get_counter("requests_by_decision", labels={"decision": "escalate_max_retry"}),
            self.metrics.get_counter("requests_by_decision", labels={"decision": "escalate_low_confidence"}),
        ])
        
        stats.direct_answer_rate = direct_count / total
        stats.clarification_rate = clarify_count / total
        stats.escalation_rate = escalate_count / total
        
        # Confidence metrics
        confidence_stats = self.metrics.get_histogram_stats("confidence_score")
        stats.avg_confidence = confidence_stats.get("mean", 0)
        
        # Error metrics
        stats.error_count = self.metrics.get_counter("errors_total")
        stats.error_rate = stats.error_count / total
        
        # User metrics
        if self.redis.is_connected:
            today_key = f"active_sessions:{datetime.now().strftime('%Y%m%d')}"
            stats.unique_users_today = self.redis.client.scard(today_key) or 0
            
            # Count active sessions (sessions with activity in last 30 minutes)
            session_keys = self.redis.client.keys("session:*")
            stats.active_sessions = len(session_keys)
        
        # Health status
        health_results = self.health.check_all()
        stats.redis_healthy = health_results.get("redis", HealthStatus("redis", False)).healthy
        stats.neo4j_healthy = health_results.get("neo4j", HealthStatus("neo4j", True)).healthy
        stats.openai_healthy = health_results.get("openai", HealthStatus("openai", True)).healthy
        
        # Uptime
        stats.uptime_seconds = now - self.start_time
        
        return stats
    
    def get_time_series_data(
        self,
        metric_name: str,
        period_hours: int = 24,
        bucket_minutes: int = 5
    ) -> List[Dict[str, Any]]:
        """Lấy time series data cho charting."""
        now = time.time()
        start_time = now - (period_hours * 3600)
        
        # Get raw data
        points = self.metrics.get_time_series(metric_name, start_time=start_time)
        
        if not points:
            return []
        
        # Bucket data
        bucket_size = bucket_minutes * 60
        buckets = defaultdict(list)
        
        for point in points:
            ts = point.get("timestamp", 0)
            bucket_ts = int(ts // bucket_size) * bucket_size
            buckets[bucket_ts].append(point.get("value", 0))
        
        # Aggregate buckets
        result = []
        for bucket_ts in sorted(buckets.keys()):
            values = buckets[bucket_ts]
            result.append({
                "timestamp": bucket_ts,
                "datetime": datetime.fromtimestamp(bucket_ts).isoformat(),
                "count": len(values),
                "sum": sum(values),
                "avg": statistics.mean(values) if values else 0,
                "min": min(values) if values else 0,
                "max": max(values) if values else 0,
            })
        
        return result
    
    def get_decision_distribution(self) -> Dict[str, int]:
        """Lấy phân phối decision types."""
        decisions = [
            "direct_answer",
            "answer_with_clarify",
            "clarify_required",
            "escalate_personal",
            "escalate_out_of_scope",
            "escalate_max_retry",
            "escalate_low_confidence"
        ]
        
        distribution = {}
        for decision in decisions:
            count = self.metrics.get_counter("requests_by_decision", labels={"decision": decision})
            distribution[decision] = count
        
        return distribution
    
    def get_error_distribution(self) -> Dict[str, int]:
        """Lấy phân phối error types."""
        if not self.redis.is_connected:
            return {}
        
        # Get all error type keys
        pattern = "metrics:counter:errors_by_type*"
        keys = self.redis.client.keys(pattern)
        
        distribution = {}
        for key in keys:
            # Extract error type from key
            try:
                error_type = key.split("type=")[1].rstrip("}")
                count = int(self.redis.client.get(key) or 0)
                distribution[error_type] = count
            except:
                continue
        
        return distribution
    
    def export_metrics(self, format: str = "json") -> str:
        """Export metrics ở các formats khác nhau."""
        stats = self.get_dashboard_stats()
        
        if format == "json":
            return json.dumps(asdict(stats), indent=2)
        elif format == "prometheus":
            # Prometheus format
            lines = []
            lines.append(f"# HELP requests_total Total number of requests")
            lines.append(f"# TYPE requests_total counter")
            lines.append(f"requests_total {stats.total_requests}")
            
            lines.append(f"# HELP latency_ms Request latency in milliseconds")
            lines.append(f"# TYPE latency_ms histogram")
            lines.append(f'latency_ms{{quantile="0.5"}} {stats.p50_latency_ms}')
            lines.append(f'latency_ms{{quantile="0.95"}} {stats.p95_latency_ms}')
            lines.append(f'latency_ms{{quantile="0.99"}} {stats.p99_latency_ms}')
            
            lines.append(f"# HELP errors_total Total number of errors")
            lines.append(f"# TYPE errors_total counter")
            lines.append(f"errors_total {stats.error_count}")
            
            return "\n".join(lines)
        
        return str(asdict(stats))


# ==================== Global Instance ====================

_dashboard: Optional[MonitoringDashboard] = None


def get_monitoring_dashboard() -> MonitoringDashboard:
    """Get global monitoring dashboard."""
    global _dashboard
    if _dashboard is None:
        _dashboard = MonitoringDashboard()
    return _dashboard


def init_monitoring(neo4j_driver=None, openai_client=None) -> MonitoringDashboard:
    """Initialize monitoring with dependencies."""
    global _dashboard
    _dashboard = MonitoringDashboard(
        neo4j_driver=neo4j_driver,
        openai_client=openai_client
    )
    return _dashboard


def record_request(**kwargs) -> None:
    """Quick helper to record request."""
    get_monitoring_dashboard().record_request(**kwargs)


def record_error(error_type: str, message: str = "") -> None:
    """Quick helper to record error."""
    get_monitoring_dashboard().record_error(error_type, message)
