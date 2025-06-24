#!/usr/bin/env python3
"""
Integration tests for real-world usage scenarios
"""

import os
import time
import tempfile
import shutil
import json
import sqlite3
import threading
from unittest.mock import patch, MagicMock
import pytest

from mini_worker import BaseMiniWorker


class DatabaseWorker(BaseMiniWorker):
    """Simulates a database processing worker"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.db_path = kwargs.get('db_path', ':memory:')
        self.batch_size = kwargs.get('batch_size', 10)
        self.processed_records = 0
        self.connection = None
        
    def get_worker_id(self):
        return "database_worker"
        
    def setup(self):
        super().setup()
        self.connection = sqlite3.connect(self.db_path)
        self.connection.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY,
                status TEXT DEFAULT 'pending',
                data TEXT,
                processed_at TIMESTAMP
            )
        ''')
        self.connection.commit()
        
    def do_work(self):
        """Process pending database records"""
        with self.track_operation('process_records'):
            cursor = self.connection.cursor()
            cursor.execute(
                'SELECT id, data FROM tasks WHERE status = ? LIMIT ?',
                ('pending', self.batch_size)
            )
            records = cursor.fetchall()
            
            if not records:
                self.logger.info("No pending records found")
                return
                
            for record_id, data in records:
                with self.track_operation('process_single_record'):
                    # Simulate processing
                    time.sleep(0.01)
                    
                    cursor.execute(
                        'UPDATE tasks SET status = ?, processed_at = ? WHERE id = ?',
                        ('completed', time.time(), record_id)
                    )
                    self.processed_records += 1
                    
            self.connection.commit()
            self.logger.info(f"Processed {len(records)} records")
            
    def cleanup(self):
        if self.connection:
            self.connection.close()
        super().cleanup()


class APIWorker(BaseMiniWorker):
    """Simulates an API polling worker"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.api_url = kwargs.get('api_url', 'https://api.example.com/data')
        self.api_key = kwargs.get('api_key', 'test-key')
        self.last_sync_time = kwargs.get('last_sync_time', 0)
        self.sync_count = 0
        
    def get_worker_id(self):
        return "api_worker"
        
    def do_work(self):
        """Poll API for new data"""
        with self.track_operation('api_poll'):
            # Simulate API call
            with self.track_operation('http_request'):
                time.sleep(0.1)  # Simulate network delay
                
            # Simulate data processing
            with self.track_operation('process_api_data'):
                time.sleep(0.05)
                self.sync_count += 1
                self.last_sync_time = time.time()
                
            self.logger.info(f"API sync completed. Total syncs: {self.sync_count}")


class FileProcessingWorker(BaseMiniWorker):
    """Simulates a file processing worker"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.input_dir = kwargs.get('input_dir', '/tmp/input')
        self.output_dir = kwargs.get('output_dir', '/tmp/output')
        self.processed_files = 0
        
    def get_worker_id(self):
        return "file_processor"
        
    def setup(self):
        super().setup()
        os.makedirs(self.input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        
    def do_work(self):
        """Process files from input directory"""
        with self.track_operation('scan_directory'):
            files = [f for f in os.listdir(self.input_dir) 
                    if f.endswith('.txt') and os.path.isfile(os.path.join(self.input_dir, f))]
            
        if not files:
            self.logger.info("No files to process")
            return
            
        for filename in files:
            with self.track_operation('process_file'):
                input_path = os.path.join(self.input_dir, filename)
                output_path = os.path.join(self.output_dir, f"processed_{filename}")
                
                # Simulate file processing
                with open(input_path, 'r') as infile:
                    content = infile.read()
                    
                with open(output_path, 'w') as outfile:
                    outfile.write(f"PROCESSED: {content.upper()}")
                    
                os.remove(input_path)
                self.processed_files += 1
                
        self.logger.info(f"Processed {len(files)} files")


class BatchWorker(BaseMiniWorker):
    """Simulates a batch processing worker with complex operations"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.batch_size = kwargs.get('batch_size', 100)
        self.error_rate = kwargs.get('error_rate', 0.1)  # 10% error rate
        self.total_processed = 0
        self.total_errors = 0
        
    def get_worker_id(self):
        return "batch_worker"
        
    def do_work(self):
        """Process a batch of items with error handling"""
        with self.track_operation('batch_processing'):
            for i in range(self.batch_size):
                try:
                    with self.track_operation('process_item'):
                        # Simulate processing time
                        time.sleep(0.001)
                        
                        # Simulate occasional errors
                        if i % int(1 / self.error_rate) == 0:
                            raise ValueError(f"Simulated error for item {i}")
                            
                        self.total_processed += 1
                        
                except Exception as e:
                    with self.track_operation('handle_error'):
                        self.total_errors += 1
                        self.logger.warning(f"Error processing item {i}: {e}")
                        
        self.logger.info(f"Batch complete. Processed: {self.total_processed}, Errors: {self.total_errors}")


class TestIntegrationScenarios:
    """Test real-world usage scenarios"""
    
    def setup_method(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Clean up test environment"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        
    def test_database_worker_scenario(self):
        """Test database processing worker scenario"""
        db_path = os.path.join(self.temp_dir, 'test.db')
        
        # Create worker
        worker = DatabaseWorker(
            log_dir=self.temp_dir,
            db_path=db_path,
            batch_size=5,
            max_cycles=3,
            wait_seconds=0.1
        )
        
        # Setup database with test data
        worker.setup_logging()
        worker.setup()
        cursor = worker.connection.cursor()
        for i in range(15):
            cursor.execute(
                'INSERT INTO tasks (data) VALUES (?)',
                (f'test_data_{i}',)
            )
        worker.connection.commit()
        
        # Run worker
        with patch.object(worker, 'setup_signal_handlers'):
            worker.run()
            
        # Verify results
        assert worker.processed_records == 15  # 3 cycles * 5 records per cycle
        
        # Check stats
        assert 'process_records' in worker.stats_dict
        assert 'process_single_record' in worker.stats_dict
        assert worker.stats_dict['process_records']['count'] == 3
        assert worker.stats_dict['process_single_record']['count'] == 15
        
        worker.cleanup()
        
    def test_api_worker_scenario(self):
        """Test API polling worker scenario"""
        worker = APIWorker(
            log_dir=self.temp_dir,
            api_url='https://test-api.com/data',
            api_key='test-key-123',
            max_cycles=5,
            wait_seconds=0.1
        )
        
        with patch.object(worker, 'setup_signal_handlers'):
            worker.run()
            
        # Verify API calls were made
        assert worker.sync_count == 5
        assert worker.last_sync_time > 0
        
        # Check stats
        assert 'api_poll' in worker.stats_dict
        assert 'http_request' in worker.stats_dict
        assert 'process_api_data' in worker.stats_dict
        assert worker.stats_dict['api_poll']['count'] == 5
        
    def test_file_processing_worker_scenario(self):
        """Test file processing worker scenario"""
        input_dir = os.path.join(self.temp_dir, 'input')
        output_dir = os.path.join(self.temp_dir, 'output')
        
        worker = FileProcessingWorker(
            log_dir=self.temp_dir,
            input_dir=input_dir,
            output_dir=output_dir,
            max_cycles=2,
            wait_seconds=0.1
        )
        
        # Create test files
        worker.setup_logging()
        worker.setup()
        for i in range(3):
            with open(os.path.join(input_dir, f'test_{i}.txt'), 'w') as f:
                f.write(f'test content {i}')
                
        with patch.object(worker, 'setup_signal_handlers'):
            worker.run()
            
        # Verify files were processed
        assert worker.processed_files == 3
        assert len(os.listdir(input_dir)) == 0  # Input files should be removed
        assert len(os.listdir(output_dir)) == 3  # Output files should exist
        
        # Check output content
        with open(os.path.join(output_dir, 'processed_test_0.txt'), 'r') as f:
            content = f.read()
            assert content == 'PROCESSED: TEST CONTENT 0'
            
    def test_batch_worker_scenario(self):
        """Test batch processing worker with error handling"""
        worker = BatchWorker(
            log_dir=self.temp_dir,
            batch_size=50,
            error_rate=0.1,  # 10% error rate
            max_cycles=2,
            wait_seconds=0.1
        )
        
        with patch.object(worker, 'setup_signal_handlers'):
            worker.run()
            
        # Verify processing
        assert worker.total_processed > 0
        assert worker.total_errors > 0
        assert worker.total_processed + worker.total_errors == 100  # 2 cycles * 50 items
        
        # Check stats
        assert 'batch_processing' in worker.stats_dict
        assert 'process_item' in worker.stats_dict
        assert 'handle_error' in worker.stats_dict
        assert worker.stats_dict['batch_processing']['count'] == 2
        
    def test_worker_with_custom_parameters(self):
        """Test worker with complex custom parameters"""
        custom_params = {
            'database_url': 'postgresql://user:pass@localhost/db',
            'api_endpoints': ['endpoint1', 'endpoint2', 'endpoint3'],
            'processing_config': {
                'timeout': 30,
                'retries': 3,
                'batch_size': 100
            },
            'feature_flags': {
                'enable_caching': True,
                'enable_metrics': True,
                'debug_mode': False
            }
        }
        
        class CustomWorker(BaseMiniWorker):
            def get_worker_id(self):
                return "custom_worker"
                
            def do_work(self):
                # Access custom parameters
                db_url = self.worker_params.get('database_url')
                endpoints = self.worker_params.get('api_endpoints', [])
                config = self.worker_params.get('processing_config', {})
                flags = self.worker_params.get('feature_flags', {})
                
                with self.track_operation('custom_processing'):
                    time.sleep(0.01)
                    
                self.logger.info(f"Processing with {len(endpoints)} endpoints")
                
        worker = CustomWorker(
            log_dir=self.temp_dir,
            max_cycles=1,
            **custom_params
        )
        
        with patch.object(worker, 'setup_signal_handlers'):
            worker.run()
            
        # Verify parameters were accessible
        assert worker.worker_params['database_url'] == custom_params['database_url']
        assert worker.worker_params['api_endpoints'] == custom_params['api_endpoints']
        assert 'custom_processing' in worker.stats_dict
