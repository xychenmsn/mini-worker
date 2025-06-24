#!/usr/bin/env python3
"""
Performance and stress tests for mini-worker
"""

import os
import time
import tempfile
import shutil
import threading
import psutil
from unittest.mock import patch
import pytest

from mini_worker import BaseMiniWorker


class HighFrequencyWorker(BaseMiniWorker):
    """Worker that performs many small operations"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.operations_per_cycle = kwargs.get('operations_per_cycle', 1000)
        self.total_operations = 0
        
    def get_worker_id(self):
        return "high_frequency_worker"
        
    def do_work(self):
        """Perform many small operations"""
        with self.track_operation('bulk_processing'):
            for i in range(self.operations_per_cycle):
                with self.track_operation('micro_operation'):
                    # Simulate very small operation
                    self.total_operations += 1
                    
        self.logger.info(f"Completed {self.operations_per_cycle} operations")


class MemoryIntensiveWorker(BaseMiniWorker):
    """Worker that uses significant memory"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.memory_size_mb = kwargs.get('memory_size_mb', 50)
        self.data_store = []
        
    def get_worker_id(self):
        return "memory_intensive_worker"
        
    def do_work(self):
        """Allocate and process memory"""
        with self.track_operation('memory_allocation'):
            # Allocate memory (1MB = ~1 million characters)
            data_chunk = 'x' * (self.memory_size_mb * 1024 * 1024)
            self.data_store.append(data_chunk)
            
        with self.track_operation('memory_processing'):
            # Process the data
            for chunk in self.data_store[-5:]:  # Process last 5 chunks
                processed = chunk.upper()
                time.sleep(0.001)  # Simulate processing
                
        # Clean up old data to prevent unlimited growth
        if len(self.data_store) > 10:
            self.data_store = self.data_store[-5:]
            
        self.logger.info(f"Memory usage: {len(self.data_store)} chunks")


class LongRunningWorker(BaseMiniWorker):
    """Worker that simulates long-running operations"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.operation_duration = kwargs.get('operation_duration', 5.0)
        self.completed_operations = 0
        
    def get_worker_id(self):
        return "long_running_worker"
        
    def do_work(self):
        """Perform a long-running operation"""
        with self.track_operation('long_operation'):
            start_time = time.time()
            
            # Simulate long operation with periodic progress updates
            while time.time() - start_time < self.operation_duration:
                with self.track_operation('progress_check'):
                    time.sleep(0.1)
                    
            self.completed_operations += 1
            
        self.logger.info(f"Completed long operation #{self.completed_operations}")


class ConcurrentWorker(BaseMiniWorker):
    """Worker that simulates concurrent operations"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.thread_count = kwargs.get('thread_count', 5)
        self.operations_per_thread = kwargs.get('operations_per_thread', 10)
        self.completed_threads = 0
        
    def get_worker_id(self):
        return "concurrent_worker"
        
    def worker_thread(self, thread_id):
        """Worker thread function"""
        for i in range(self.operations_per_thread):
            with self.track_operation(f'thread_{thread_id}_operation'):
                time.sleep(0.01)  # Simulate work
                
        self.completed_threads += 1
        
    def do_work(self):
        """Run multiple concurrent operations"""
        with self.track_operation('concurrent_processing'):
            threads = []
            
            # Start threads
            for i in range(self.thread_count):
                thread = threading.Thread(target=self.worker_thread, args=(i,))
                threads.append(thread)
                thread.start()
                
            # Wait for all threads to complete
            for thread in threads:
                thread.join()
                
        self.logger.info(f"Completed concurrent processing with {self.thread_count} threads")


class ResourceMonitoringWorker(BaseMiniWorker):
    """Worker that monitors its own resource usage"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.process = psutil.Process()
        self.max_memory_mb = 0
        self.max_cpu_percent = 0
        
    def get_worker_id(self):
        return "resource_monitoring_worker"
        
    def do_work(self):
        """Monitor resource usage while working"""
        with self.track_operation('resource_monitoring'):
            # Get current resource usage
            memory_info = self.process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024
            cpu_percent = self.process.cpu_percent()
            
            # Update maximums
            self.max_memory_mb = max(self.max_memory_mb, memory_mb)
            self.max_cpu_percent = max(self.max_cpu_percent, cpu_percent)
            
            # Simulate some work
            with self.track_operation('simulated_work'):
                data = []
                for i in range(10000):
                    data.append(f"item_{i}" * 10)
                    
                # Process the data
                processed = [item.upper() for item in data]
                time.sleep(0.1)
                
        self.logger.info(f"Memory: {memory_mb:.1f}MB (max: {self.max_memory_mb:.1f}MB), "
                        f"CPU: {cpu_percent:.1f}% (max: {self.max_cpu_percent:.1f}%)")


class TestPerformanceStress:
    """Performance and stress test cases"""
    
    def setup_method(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Clean up test environment"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        
    def test_high_frequency_operations(self):
        """Test worker handling many small operations"""
        worker = HighFrequencyWorker(
            log_dir=self.temp_dir,
            operations_per_cycle=500,  # Reduced for test speed
            max_cycles=3,
            wait_seconds=0.1
        )
        
        start_time = time.time()
        
        with patch.object(worker, 'setup_signal_handlers'):
            worker.run()
            
        end_time = time.time()
        duration = end_time - start_time
        
        # Verify operations completed
        assert worker.total_operations == 1500  # 3 cycles * 500 operations
        
        # Check performance stats
        assert 'bulk_processing' in worker.stats_dict
        assert 'micro_operation' in worker.stats_dict
        assert worker.stats_dict['micro_operation']['count'] == 1500
        
        # Verify reasonable performance (should complete in reasonable time)
        assert duration < 10.0  # Should complete within 10 seconds
        
        # Check operation rate
        ops_per_second = worker.total_operations / duration
        assert ops_per_second > 100  # Should handle at least 100 ops/second
        
    def test_memory_intensive_operations(self):
        """Test worker with significant memory usage"""
        worker = MemoryIntensiveWorker(
            log_dir=self.temp_dir,
            memory_size_mb=10,  # 10MB per cycle
            max_cycles=3,
            wait_seconds=0.1
        )
        
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        with patch.object(worker, 'setup_signal_handlers'):
            worker.run()
            
        # Verify memory operations completed
        assert 'memory_allocation' in worker.stats_dict
        assert 'memory_processing' in worker.stats_dict
        assert worker.stats_dict['memory_allocation']['count'] == 3
        
        # Verify memory was managed (not unlimited growth)
        assert len(worker.data_store) <= 10
        
    def test_long_running_operations(self):
        """Test worker with long-running operations"""
        worker = LongRunningWorker(
            log_dir=self.temp_dir,
            operation_duration=1.0,  # 1 second operations
            max_cycles=2,
            wait_seconds=0.1
        )
        
        start_time = time.time()
        
        with patch.object(worker, 'setup_signal_handlers'):
            worker.run()
            
        end_time = time.time()
        duration = end_time - start_time
        
        # Verify operations completed
        assert worker.completed_operations == 2
        
        # Verify timing (should take at least 2 seconds for 2 operations)
        assert duration >= 2.0
        
        # Check stats
        assert 'long_operation' in worker.stats_dict
        assert 'progress_check' in worker.stats_dict
        assert worker.stats_dict['long_operation']['count'] == 2
        
    def test_concurrent_operations(self):
        """Test worker with concurrent operations"""
        worker = ConcurrentWorker(
            log_dir=self.temp_dir,
            thread_count=3,
            operations_per_thread=5,
            max_cycles=2,
            wait_seconds=0.1
        )
        
        with patch.object(worker, 'setup_signal_handlers'):
            worker.run()
            
        # Verify concurrent operations completed
        assert 'concurrent_processing' in worker.stats_dict
        assert worker.stats_dict['concurrent_processing']['count'] == 2
        
        # Check that thread operations were tracked
        thread_ops = [key for key in worker.stats_dict.keys() 
                     if key.startswith('thread_') and key.endswith('_operation')]
        assert len(thread_ops) == 3  # 3 threads
        
        # Each thread should have completed operations in both cycles
        for op_key in thread_ops:
            assert worker.stats_dict[op_key]['count'] == 10  # 5 ops * 2 cycles
            
    def test_resource_monitoring(self):
        """Test worker resource monitoring"""
        worker = ResourceMonitoringWorker(
            log_dir=self.temp_dir,
            max_cycles=3,
            wait_seconds=0.1
        )
        
        with patch.object(worker, 'setup_signal_handlers'):
            worker.run()
            
        # Verify monitoring completed
        assert 'resource_monitoring' in worker.stats_dict
        assert 'simulated_work' in worker.stats_dict
        assert worker.stats_dict['resource_monitoring']['count'] == 3
        
        # Verify resource tracking
        assert worker.max_memory_mb > 0
        assert worker.max_cpu_percent >= 0
        
    def test_stats_performance_with_many_operations(self):
        """Test that stats tracking doesn't degrade performance significantly"""
        class StatsTestWorker(BaseMiniWorker):
            def get_worker_id(self):
                return "stats_test_worker"
                
            def do_work(self):
                # Create many different operation types
                for i in range(100):
                    with self.track_operation(f'operation_type_{i % 10}'):
                        pass  # Minimal work
                        
        worker = StatsTestWorker(
            log_dir=self.temp_dir,
            max_cycles=5,
            wait_seconds=0.01
        )
        
        start_time = time.time()
        
        with patch.object(worker, 'setup_signal_handlers'):
            worker.run()
            
        end_time = time.time()
        duration = end_time - start_time
        
        # Verify many operations were tracked
        total_ops = sum(stats['count'] for stats in worker.stats_dict.values())
        assert total_ops == 500  # 5 cycles * 100 operations
        
        # Verify reasonable performance despite many stats
        assert duration < 5.0  # Should complete quickly
        
        # Verify all operation types were tracked
        operation_types = [key for key in worker.stats_dict.keys() 
                          if key.startswith('operation_type_')]
        assert len(operation_types) == 10  # 10 different operation types
        
    def test_error_handling_under_stress(self):
        """Test error handling when operations fail frequently"""
        class ErrorProneWorker(BaseMiniWorker):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.error_count = 0
                self.success_count = 0
                
            def get_worker_id(self):
                return "error_prone_worker"
                
            def do_work(self):
                for i in range(50):
                    try:
                        with self.track_operation('risky_operation'):
                            if i % 3 == 0:  # Fail every 3rd operation
                                raise ValueError(f"Simulated error {i}")
                            self.success_count += 1
                    except Exception as e:
                        self.error_count += 1
                        self.logger.warning(f"Operation failed: {e}")
                        
        worker = ErrorProneWorker(
            log_dir=self.temp_dir,
            max_cycles=2,
            wait_seconds=0.1
        )
        
        with patch.object(worker, 'setup_signal_handlers'):
            worker.run()
            
        # Verify worker continued despite errors
        assert worker.error_count > 0
        assert worker.success_count > 0
        assert worker.error_count + worker.success_count == 100  # 2 cycles * 50 operations
        
        # Verify stats were still tracked correctly
        assert 'risky_operation' in worker.stats_dict
        # Only successful operations should be in stats
        assert worker.stats_dict['risky_operation']['count'] == worker.success_count
