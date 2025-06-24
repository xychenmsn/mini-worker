#!/usr/bin/env python3
"""
Basic example of a mini-worker
"""

import time
import random
from mini_worker import BaseMiniWorker


class BasicWorker(BaseMiniWorker):
    """
    A simple example worker that demonstrates basic functionality
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Get worker-specific parameters
        self.message = self.worker_params.get('message', 'Hello from BasicWorker!')
        self.min_delay = self.worker_params.get('min_delay', 1)
        self.max_delay = self.worker_params.get('max_delay', 5)
        
    def get_worker_id(self):
        return "basic_worker"
        
    def setup(self):
        """Perform any setup needed before starting work"""
        super().setup()
        self.logger.info(f"BasicWorker setup complete")
        self.logger.info(f"Message: {self.message}")
        self.logger.info(f"Delay range: {self.min_delay}-{self.max_delay} seconds")
        
    def do_work(self):
        """Main work logic"""
        self.logger.info("Starting work cycle...")
        
        # Simulate some work with tracking
        with self.track_operation('process_items'):
            # Simulate processing multiple items
            num_items = random.randint(1, 5)
            for i in range(num_items):
                self.logger.info(f"Processing item {i+1}/{num_items}")
                
                # Simulate work time
                delay = random.uniform(self.min_delay, self.max_delay)
                time.sleep(delay)
                
                self.logger.info(f"Completed item {i+1} in {delay:.2f}s")
        
        # Simulate another type of operation
        with self.track_operation('cleanup_tasks'):
            self.logger.info("Performing cleanup...")
            time.sleep(random.uniform(0.5, 2.0))
            self.logger.info("Cleanup completed")
            
        self.logger.info(f"Work cycle completed. Message: {self.message}")
        
    def cleanup(self):
        """Perform cleanup when worker stops"""
        super().cleanup()
        self.logger.info("BasicWorker cleanup completed")


if __name__ == '__main__':
    # Example of running the worker directly
    worker = BasicWorker(
        worker_id="basic_worker_001",
        log_dir="./logs",
        wait_seconds=10,  # 10 second intervals for testing
        max_cycles=5,     # Stop after 5 cycles for testing
        message="Hello from direct execution!",
        min_delay=0.5,
        max_delay=2.0
    )
    
    print("Starting BasicWorker...")
    print("Press Ctrl+C to stop")
    
    try:
        worker.run()
    except KeyboardInterrupt:
        print("\nStopped by user")
