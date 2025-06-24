"""
Monitoring backends for mini-worker framework
"""

import os
import json
import time
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional


class MonitoringBackend(ABC):
    """Abstract base class for monitoring backends"""
    
    @abstractmethod
    def report_status(self, worker_id: str, status: Dict[str, Any]):
        """Report worker status"""
        pass
        
    @abstractmethod
    def get_status(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """Get worker status"""
        pass


class FileMonitoring(MonitoringBackend):
    """File-based monitoring backend"""
    
    def __init__(self, stats_dir: str):
        self.stats_dir = stats_dir
        os.makedirs(stats_dir, exist_ok=True)
        
    def report_status(self, worker_id: str, status: Dict[str, Any]):
        """Write status to files"""
        try:
            # Write human-readable stats
            stats_file = os.path.join(self.stats_dir, f"{worker_id}.stats")
            with open(stats_file, 'w') as f:
                f.write(self._format_status(status))
                
            # Write detailed JSON stats
            json_file = os.path.join(self.stats_dir, f"{worker_id}.json")
            with open(json_file, 'w') as f:
                json.dump(status, f, indent=2, default=str)
                
        except Exception as e:
            # Don't raise exceptions from monitoring to avoid breaking worker
            print(f"Error writing status files for {worker_id}: {e}")
            
    def get_status(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """Read status from JSON file"""
        json_file = os.path.join(self.stats_dir, f"{worker_id}.json")
        try:
            if os.path.exists(json_file):
                with open(json_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            print(f"Error reading status file for {worker_id}: {e}")
        return None
        
    def _format_status(self, status: Dict[str, Any]) -> str:
        """Format status dictionary as human-readable string"""
        lines = []
        
        # Basic info
        lines.append(f"Worker ID: {status.get('worker_id', 'unknown')}")
        lines.append(f"Status: {status.get('status', 'unknown')}")
        lines.append(f"Total Cycles: {status.get('total_work_cycles', 0)}")
        
        # Timing info
        if status.get('last_work_cycle_time'):
            lines.append(f"Last Cycle Duration: {status['last_work_cycle_time']:.2f}s")
            
        if status.get('total_processing_time'):
            avg_time = status['total_processing_time'] / max(1, status.get('total_work_cycles', 1))
            lines.append(f"Average Cycle Time: {avg_time:.2f}s")
            
        # Operation stats
        operations = status.get('operations', {})
        if operations:
            lines.append("\nOperations:")
            for op_name, op_stats in sorted(operations.items()):
                rate = op_stats.get('rate_per_hour', 0)
                count = op_stats.get('count', 0)
                lines.append(f"  {op_name}: {rate:.1f}/hour ({count} total)")
                
        # Timestamp
        timestamp = status.get('timestamp')
        if timestamp:
            time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
            lines.append(f"\nLast Updated: {time_str}")
            
        return "\n".join(lines)


class DatabaseMonitoring(MonitoringBackend):
    """Database-based monitoring backend (example implementation)"""
    
    def __init__(self, db_url: str, table_name: str = "worker_status"):
        self.db_url = db_url
        self.table_name = table_name
        # Note: This is a placeholder - actual implementation would
        # require a database library like SQLAlchemy
        
    def report_status(self, worker_id: str, status: Dict[str, Any]):
        """Write status to database"""
        # Placeholder implementation
        # In real implementation, would INSERT/UPDATE database record
        pass
        
    def get_status(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """Read status from database"""
        # Placeholder implementation
        # In real implementation, would SELECT from database
        return None


class HTTPMonitoring(MonitoringBackend):
    """HTTP-based monitoring backend (example implementation)"""
    
    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        
    def report_status(self, worker_id: str, status: Dict[str, Any]):
        """POST status to HTTP endpoint"""
        # Placeholder implementation
        # In real implementation, would use requests library
        # requests.post(f"{self.base_url}/workers/{worker_id}/status", 
        #               json=status, timeout=self.timeout)
        pass
        
    def get_status(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """GET status from HTTP endpoint"""
        # Placeholder implementation
        # In real implementation, would use requests library
        # response = requests.get(f"{self.base_url}/workers/{worker_id}/status")
        # return response.json() if response.ok else None
        return None
