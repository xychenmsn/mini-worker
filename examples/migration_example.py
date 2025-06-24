#!/usr/bin/env python3
"""
Example showing how to migrate from existing worker system to mini-worker
"""

import time
import random
from mini_worker import BaseMiniWorker, MiniWorkerManager


# Simulated existing dependencies (like your current system)
class MockDatabase:
    """Mock database client"""
    def get_unprocessed_items(self, limit=10):
        return [f"item_{i}" for i in range(limit)]
    
    def mark_processed(self, item_id):
        print(f"Marked {item_id} as processed in database")


class MockAPIClient:
    """Mock API client"""
    def __init__(self, url):
        self.url = url
    
    def fetch_data(self, item_id):
        return f"data_for_{item_id}"


class MockProcessor:
    """Mock processing component"""
    def process(self, data):
        time.sleep(0.1)  # Simulate processing time
        return f"processed_{data}"


# Example of migrated worker (similar to your SpiderArticleWorker)
class MigratedWorker(BaseMiniWorker):
    """
    Example worker migrated from existing system.
    
    This shows how your existing workers can be easily migrated:
    1. Change inheritance from BaseWorker to BaseMiniWorker
    2. Add **kwargs to __init__ and call super().__init__(**kwargs)
    3. Add get_worker_id() method
    4. Keep all existing logic and dependencies
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # Initialize dependencies exactly like before
        # No dependency injection needed - import and instantiate normally!
        self.db = MockDatabase()
        self.api_client = MockAPIClient("http://localhost:8003")
        self.processor = MockProcessor()
        
        # Get worker-specific parameters (optional)
        self.batch_size = self.worker_params.get('batch_size', 5)
        self.enable_cleanup = self.worker_params.get('enable_cleanup', True)
        
    def get_worker_id(self):
        """Required method - return worker identifier"""
        return "migrated_worker"
        
    def setup(self):
        """Optional setup method"""
        super().setup()
        self.logger.info(f"MigratedWorker setup complete")
        self.logger.info(f"Batch size: {self.batch_size}")
        self.logger.info(f"Cleanup enabled: {self.enable_cleanup}")
        
    def do_work(self):
        """Main work method - keep existing logic"""
        self.logger.info("Starting work cycle...")
        
        # Use calc_one for compatibility with existing code
        with self.calc_one('fetch_items'):
            items = self.db.get_unprocessed_items(limit=self.batch_size)
            
        if not items:
            self.logger.info("No items to process")
            return
            
        self.logger.info(f"Processing {len(items)} items")
        
        # Process each item
        for item in items:
            with self.calc_one('process_item'):
                # Fetch data
                data = self.api_client.fetch_data(item)
                
                # Process data
                result = self.processor.process(data)
                
                # Save result
                self.db.mark_processed(item)
                
                self.logger.debug(f"Processed {item}: {result}")
        
        # Optional cleanup
        if self.enable_cleanup:
            with self.calc_one('cleanup'):
                self._perform_cleanup()
                
        self.logger.info(f"Work cycle completed - processed {len(items)} items")
        
    def _perform_cleanup(self):
        """Cleanup method"""
        self.logger.debug("Performing cleanup...")
        time.sleep(0.1)  # Simulate cleanup work
        
    def cleanup(self):
        """Optional cleanup when worker stops"""
        super().cleanup()
        self.logger.info("MigratedWorker cleanup completed")


# Example of using the worker manager (like your current WorkerManager)
class WorkerManagerExample:
    """
    Example showing how to replace your current WorkerManager
    with MiniWorkerManager while keeping the same interface.
    """
    
    def __init__(self):
        # Create manager with same interface as your current system
        self.manager = MiniWorkerManager(
            log_dir="/var/log/workers",
            stats_dir="/var/log/workers"
        )
        
        # Register workers (replaces your workers config)
        self.manager.register_worker("migrated_worker", "examples.migration_example.MigratedWorker")
        
    def start_worker_with_params(self, worker_name: str, parameters: dict):
        """Same interface as your current WorkerManager"""
        try:
            self.manager.start_worker_with_params(worker_name, parameters)
            return {"success": True, "message": f"Worker {worker_name} started"}
        except Exception as e:
            return {"success": False, "error": str(e)}
            
    def stop_worker(self, worker_name: str):
        """Same interface as your current WorkerManager"""
        try:
            self.manager.stop_worker(worker_name)
            return {"success": True, "message": f"Worker {worker_name} stopped"}
        except Exception as e:
            return {"success": False, "error": str(e)}
            
    def get_worker_status(self, worker_name: str):
        """Same interface as your current WorkerManager"""
        try:
            return self.manager.get_worker_status(worker_name)
        except Exception as e:
            return {"error": str(e)}
            
    def get_all_worker_statuses(self):
        """Same interface as your current WorkerManager"""
        return self.manager.get_worker_statuses()


def demonstrate_direct_execution():
    """Show direct worker execution (like your current system)"""
    print("=== Direct Worker Execution ===")
    
    worker = MigratedWorker(
        worker_id="migrated_worker_001",
        log_dir="./logs",
        wait_seconds=5,  # Short interval for demo
        max_cycles=3,    # Stop after 3 cycles for demo
        batch_size=3,
        enable_cleanup=True
    )
    
    print("Starting worker directly...")
    worker.run()
    print("Worker completed")


def demonstrate_manager_usage():
    """Show manager usage (like your current REST API)"""
    print("\n=== Manager Usage ===")
    
    manager_example = WorkerManagerExample()
    
    # Start worker with parameters (like your frontend does)
    print("Starting worker via manager...")
    result = manager_example.start_worker_with_params("migrated_worker", {
        "batch_size": 2,
        "enable_cleanup": False
    })
    print(f"Start result: {result}")
    
    # Check status
    time.sleep(2)
    status = manager_example.get_worker_status("migrated_worker")
    print(f"Worker status: {status}")
    
    # Stop worker
    time.sleep(3)
    result = manager_example.stop_worker("migrated_worker")
    print(f"Stop result: {result}")


if __name__ == '__main__':
    print("Mini-Worker Migration Example")
    print("=" * 40)
    
    # Demonstrate both execution methods
    demonstrate_direct_execution()
    demonstrate_manager_usage()
    
    print("\nMigration complete! Your workers can now:")
    print("1. Run directly via command line")
    print("2. Be managed via MiniWorkerManager")
    print("3. Keep all existing dependencies and logic")
    print("4. Work with your existing REST API and frontend")
