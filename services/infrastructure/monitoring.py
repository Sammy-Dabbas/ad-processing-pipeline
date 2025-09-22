"""
CloudWatch-Style Monitoring and Alerting System
Production-grade monitoring for ad event processing pipeline
"""

import time
import json
import asyncio
import logging
from typing import Dict, List, Optional, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from collections import deque, defaultdict
from enum import Enum
import threading
import psutil
import os


class AlertLevel(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class MetricType(str, Enum):
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    RATE = "rate"


@dataclass
class Metric:
    """Individual metric data point"""
    name: str
    value: float
    metric_type: MetricType
    timestamp: float
    dimensions: Dict[str, str]
    unit: str = "count"


@dataclass
class Alert:
    """Alert definition and state"""
    name: str
    metric_name: str
    threshold: float
    comparison: str  # "gt", "lt", "eq"
    level: AlertLevel
    description: str
    enabled: bool = True
    last_triggered: Optional[float] = None
    trigger_count: int = 0


@dataclass
class AlarmState:
    """Current state of an alarm"""
    alert_name: str
    state: str  # "OK", "ALARM", "INSUFFICIENT_DATA"
    reason: str
    timestamp: float
    metric_value: Optional[float] = None


class ProductionMonitoring:
    """CloudWatch-style monitoring for ad event processing"""
    
    def __init__(self, namespace: str = "AdEventProcessing"):
        self.namespace = namespace
        
        # Metric storage (in production, this would be CloudWatch/Prometheus)
        self.metrics: deque = deque(maxlen=10000)  # Keep last 10K metrics
        self.metric_buffers: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        
        # Alert definitions and states
        self.alerts: Dict[str, Alert] = {}
        self.alarm_states: Dict[str, AlarmState] = {}
        
        # Callbacks for alert notifications
        self.alert_callbacks: List[Callable] = []
        
        # Background monitoring thread
        self.monitoring_thread = None
        self.stop_monitoring = False
        
        # Performance tracking
        self.start_time = time.time()
        self.last_metrics_flush = time.time()
        
        # Initialize default alerts
        self._setup_default_alerts()
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def _setup_default_alerts(self):
        """Setup default alerts for ad event processing"""
        
        # Performance alerts
        self.add_alert(Alert(
            name="HighLatency",
            metric_name="ProcessingLatency",
            threshold=50.0,  # 50ms
            comparison="gt",
            level=AlertLevel.WARNING,
            description="Processing latency above 50ms"
        ))
        
        self.add_alert(Alert(
            name="LowThroughput", 
            metric_name="EventsPerSecond",
            threshold=100000.0,  # 100K/sec
            comparison="lt",
            level=AlertLevel.CRITICAL,
            description="Event processing below 100K/sec"
        ))
        
        self.add_alert(Alert(
            name="HighErrorRate",
            metric_name="ErrorRate",
            threshold=1.0,  # 1%
            comparison="gt", 
            level=AlertLevel.CRITICAL,
            description="Error rate above 1%"
        ))
        
        # System resource alerts
        self.add_alert(Alert(
            name="HighCPUUsage",
            metric_name="CPUUtilization",
            threshold=80.0,  # 80%
            comparison="gt",
            level=AlertLevel.WARNING,
            description="CPU usage above 80%"
        ))
        
        self.add_alert(Alert(
            name="HighMemoryUsage", 
            metric_name="MemoryUtilization",
            threshold=85.0,  # 85%
            comparison="gt",
            level=AlertLevel.WARNING,
            description="Memory usage above 85%"
        ))
        
        # Business metric alerts
        self.add_alert(Alert(
            name="LowRevenue",
            metric_name="RevenuePerHour",
            threshold=1000.0,  # $1000/hour
            comparison="lt",
            level=AlertLevel.WARNING,
            description="Revenue below $1000/hour"
        ))
    
    def add_alert(self, alert: Alert):
        """Add new alert definition"""
        self.alerts[alert.name] = alert
        self.alarm_states[alert.name] = AlarmState(
            alert_name=alert.name,
            state="INSUFFICIENT_DATA",
            reason="Alert just created",
            timestamp=time.time()
        )
    
    def add_alert_callback(self, callback: Callable[[Alert, AlarmState], None]):
        """Add callback for alert notifications"""
        self.alert_callbacks.append(callback)
    
    # =============================================
    # METRIC COLLECTION
    # =============================================
    
    def put_metric(self, name: str, value: float, 
                   metric_type: MetricType = MetricType.GAUGE,
                   dimensions: Dict[str, str] = None,
                   unit: str = "count"):
        """Put a metric data point"""
        
        metric = Metric(
            name=name,
            value=value,
            metric_type=metric_type,
            timestamp=time.time(),
            dimensions=dimensions or {},
            unit=unit
        )
        
        self.metrics.append(metric)
        self.metric_buffers[name].append(metric)
        
        # Check alerts for this metric
        self._check_alerts_for_metric(name, value)
    
    def put_custom_metric(self, name: str, value: float, **dimensions):
        """Convenience method for custom metrics"""
        self.put_metric(name, value, MetricType.GAUGE, dimensions)
    
    def increment_counter(self, name: str, value: float = 1.0, **dimensions):
        """Increment a counter metric"""
        self.put_metric(name, value, MetricType.COUNTER, dimensions, "count")
    
    def record_processing_metrics(self, events_processed: int, duration: float, 
                                errors: int, revenue: float):
        """Record key processing metrics"""
        
        # Calculate derived metrics
        events_per_second = events_processed / max(duration, 0.001)
        avg_latency = (duration / max(events_processed, 1)) * 1000  # ms
        error_rate = (errors / max(events_processed, 1)) * 100  # %
        
        # Core performance metrics
        self.put_metric("EventsProcessed", events_processed, MetricType.COUNTER)
        self.put_metric("EventsPerSecond", events_per_second, MetricType.GAUGE, unit="count/sec")
        self.put_metric("ProcessingLatency", avg_latency, MetricType.GAUGE, unit="ms")
        self.put_metric("ErrorRate", error_rate, MetricType.GAUGE, unit="percent")
        
        # Business metrics
        self.put_metric("TotalRevenue", revenue, MetricType.COUNTER, unit="usd")
        self.put_metric("RevenuePerHour", revenue * (3600 / max(duration, 1)), 
                       MetricType.GAUGE, unit="usd/hour")
        
        # System metrics
        self._record_system_metrics()
    
    def _record_system_metrics(self):
        """Record system resource metrics"""
        
        # CPU usage
        cpu_percent = psutil.cpu_percent(interval=None)
        self.put_metric("CPUUtilization", cpu_percent, MetricType.GAUGE, unit="percent")
        
        # Memory usage
        memory = psutil.virtual_memory()
        self.put_metric("MemoryUtilization", memory.percent, MetricType.GAUGE, unit="percent")
        self.put_metric("MemoryUsed", memory.used / (1024**3), MetricType.GAUGE, unit="gb")
        
        # Disk I/O
        disk_io = psutil.disk_io_counters()
        if disk_io:
            self.put_metric("DiskReadBytes", disk_io.read_bytes, MetricType.COUNTER, unit="bytes")
            self.put_metric("DiskWriteBytes", disk_io.write_bytes, MetricType.COUNTER, unit="bytes")
        
        # Network I/O
        net_io = psutil.net_io_counters()
        if net_io:
            self.put_metric("NetworkBytesIn", net_io.bytes_recv, MetricType.COUNTER, unit="bytes")
            self.put_metric("NetworkBytesOut", net_io.bytes_sent, MetricType.COUNTER, unit="bytes")
    
    # =============================================
    # ALERT EVALUATION
    # =============================================
    
    def _check_alerts_for_metric(self, metric_name: str, value: float):
        """Check all alerts for a specific metric"""
        
        for alert in self.alerts.values():
            if alert.metric_name == metric_name and alert.enabled:
                self._evaluate_alert(alert, value)
    
    def _evaluate_alert(self, alert: Alert, metric_value: float):
        """Evaluate a single alert against metric value"""
        
        # Determine if alert condition is met
        triggered = False
        
        if alert.comparison == "gt" and metric_value > alert.threshold:
            triggered = True
        elif alert.comparison == "lt" and metric_value < alert.threshold:
            triggered = True
        elif alert.comparison == "eq" and abs(metric_value - alert.threshold) < 0.001:
            triggered = True
        
        current_state = self.alarm_states.get(alert.name)
        if not current_state:
            return
        
        # Update alarm state
        new_state = "ALARM" if triggered else "OK"
        reason = f"Metric {alert.metric_name} = {metric_value:.2f}"
        
        if triggered:
            reason += f" {alert.comparison} {alert.threshold}"
            alert.trigger_count += 1
            alert.last_triggered = time.time()
        
        # Only notify if state changed
        state_changed = current_state.state != new_state
        
        current_state.state = new_state
        current_state.reason = reason
        current_state.timestamp = time.time()
        current_state.metric_value = metric_value
        
        if state_changed and triggered:
            self._notify_alert(alert, current_state)
    
    def _notify_alert(self, alert: Alert, alarm_state: AlarmState):
        """Notify all callbacks about alert"""
        
        self.logger.warning(f"ALERT: {alert.name} - {alert.description}")
        self.logger.warning(f"State: {alarm_state.state} - {alarm_state.reason}")
        
        for callback in self.alert_callbacks:
            try:
                callback(alert, alarm_state)
            except Exception as e:
                self.logger.error(f"Alert callback error: {e}")
    
    # =============================================
    # METRIC QUERIES & DASHBOARDS
    # =============================================
    
    def get_metric_statistics(self, metric_name: str, 
                            start_time: float, end_time: float,
                            statistic: str = "Average") -> Optional[float]:
        """Get metric statistics for time range"""
        
        if metric_name not in self.metric_buffers:
            return None
        
        # Filter metrics by time range
        metrics_in_range = [
            m for m in self.metric_buffers[metric_name]
            if start_time <= m.timestamp <= end_time
        ]
        
        if not metrics_in_range:
            return None
        
        values = [m.value for m in metrics_in_range]
        
        if statistic.lower() == "average":
            return sum(values) / len(values)
        elif statistic.lower() == "sum":
            return sum(values)
        elif statistic.lower() == "maximum":
            return max(values)
        elif statistic.lower() == "minimum":
            return min(values)
        elif statistic.lower() == "count":
            return len(values)
        
        return None
    
    def get_dashboard_data(self) -> Dict:
        """Get comprehensive dashboard data"""
        
        now = time.time()
        last_hour = now - 3600
        last_5_min = now - 300
        
        dashboard = {
            "timestamp": now,
            "uptime_seconds": now - self.start_time,
            "metrics": {},
            "alerts": {},
            "system_health": {}
        }
        
        # Key metrics for last 5 minutes
        key_metrics = [
            "EventsPerSecond", "ProcessingLatency", "ErrorRate",
            "CPUUtilization", "MemoryUtilization", "TotalRevenue"
        ]
        
        for metric in key_metrics:
            avg_value = self.get_metric_statistics(metric, last_5_min, now, "Average")
            max_value = self.get_metric_statistics(metric, last_5_min, now, "Maximum")
            
            dashboard["metrics"][metric] = {
                "current": avg_value,
                "peak_5min": max_value,
                "unit": self._get_metric_unit(metric)
            }
        
        # Alert status
        for alert_name, alarm_state in self.alarm_states.items():
            dashboard["alerts"][alert_name] = {
                "state": alarm_state.state,
                "reason": alarm_state.reason,
                "level": self.alerts[alert_name].level,
                "last_triggered": self.alerts[alert_name].last_triggered
            }
        
        # System health summary
        dashboard["system_health"] = {
            "overall_status": self._calculate_overall_health(),
            "active_alerts": len([a for a in self.alarm_states.values() if a.state == "ALARM"]),
            "total_events_processed": self.get_metric_statistics("EventsProcessed", self.start_time, now, "Sum") or 0
        }
        
        return dashboard
    
    def _get_metric_unit(self, metric_name: str) -> str:
        """Get unit for metric"""
        unit_map = {
            "EventsPerSecond": "events/sec",
            "ProcessingLatency": "ms", 
            "ErrorRate": "%",
            "CPUUtilization": "%",
            "MemoryUtilization": "%",
            "TotalRevenue": "$"
        }
        return unit_map.get(metric_name, "count")
    
    def _calculate_overall_health(self) -> str:
        """Calculate overall system health"""
        
        critical_alerts = [
            a for a in self.alarm_states.values() 
            if a.state == "ALARM" and self.alerts[a.alert_name].level in [AlertLevel.CRITICAL, AlertLevel.EMERGENCY]
        ]
        
        warning_alerts = [
            a for a in self.alarm_states.values()
            if a.state == "ALARM" and self.alerts[a.alert_name].level == AlertLevel.WARNING
        ]
        
        if critical_alerts:
            return "CRITICAL"
        elif warning_alerts:
            return "WARNING"
        else:
            return "HEALTHY"
    
    # =============================================
    # BACKGROUND MONITORING
    # =============================================
    
    def start_monitoring(self, interval: float = 30.0):
        """Start background monitoring thread"""
        
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            return
        
        def monitor_loop():
            while not self.stop_monitoring:
                try:
                    # Record system metrics
                    self._record_system_metrics()
                    
                    # Flush old metrics
                    self._flush_old_metrics()
                    
                    # Health check
                    self._perform_health_checks()
                    
                    time.sleep(interval)
                
                except Exception as e:
                    self.logger.error(f"Monitoring error: {e}")
                    time.sleep(interval)
        
        self.monitoring_thread = threading.Thread(target=monitor_loop, daemon=True)
        self.monitoring_thread.start()
        
        self.logger.info("Background monitoring started")
    
    def stop_monitoring_thread(self):
        """Stop background monitoring"""
        self.stop_monitoring = True
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=5)
    
    def _flush_old_metrics(self):
        """Remove old metrics to prevent memory bloat"""
        cutoff_time = time.time() - 3600  # Keep 1 hour of metrics
        
        for metric_name in list(self.metric_buffers.keys()):
            buffer = self.metric_buffers[metric_name]
            # Remove old metrics
            while buffer and buffer[0].timestamp < cutoff_time:
                buffer.popleft()
    
    def _perform_health_checks(self):
        """Perform periodic health checks"""
        
        # Check if we're receiving metrics
        last_metric_time = max((m.timestamp for m in self.metrics), default=0)
        if time.time() - last_metric_time > 300:  # 5 minutes
            self.put_metric("HealthCheck", 0, MetricType.GAUGE)  # Unhealthy
        else:
            self.put_metric("HealthCheck", 1, MetricType.GAUGE)  # Healthy


# Example alert callback
def slack_alert_callback(alert: Alert, alarm_state: AlarmState):
    """Example callback to send alerts to Slack"""
    message = f" ALERT: {alert.name}\n"
    message += f"Description: {alert.description}\n"
    message += f"State: {alarm_state.state}\n"
    message += f"Reason: {alarm_state.reason}\n"
    message += f"Level: {alert.level.value.upper()}"
    
    print(f"SLACK ALERT: {message}")  # In production, send to actual Slack


def email_alert_callback(alert: Alert, alarm_state: AlarmState):
    """Example callback to send email alerts"""
    subject = f"Production Alert: {alert.name} - {alert.level.value.upper()}"
    body = f"""
    Alert: {alert.name}
    Description: {alert.description}
    
    Current State: {alarm_state.state}
    Reason: {alarm_state.reason}
    Metric Value: {alarm_state.metric_value}
    
    Time: {datetime.fromtimestamp(alarm_state.timestamp)}
    """
    
    print(f"EMAIL ALERT: {subject}\n{body}")  # In production, send actual email


# Usage example
if __name__ == "__main__":
    # Initialize monitoring
    monitor = ProductionMonitoring("AdEventProcessing")
    
    # Add alert callbacks
    monitor.add_alert_callback(slack_alert_callback)
    monitor.add_alert_callback(email_alert_callback)
    
    # Start background monitoring
    monitor.start_monitoring(interval=10.0)
    
    # Simulate some metrics
    for i in range(100):
        monitor.record_processing_metrics(
            events_processed=1000,
            duration=1.0,
            errors=i // 10,  # Gradually increase errors
            revenue=100.0
        )
        
        time.sleep(1)
    
    # Get dashboard data
    dashboard = monitor.get_dashboard_data()
    print(json.dumps(dashboard, indent=2))
    
    # Stop monitoring
    monitor.stop_monitoring_thread()
