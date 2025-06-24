#!/usr/bin/env python3
"""
Example of a batch processing worker
"""

import time
import random
from typing import List, Dict, Any
from mini_worker import BaseMiniWorker


class BatchWorker(BaseMiniWorker):
    """
    Example worker that processes items in batches
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Get worker-specific parameters
        self.batch_size = self.worker_params.get('batch_size', 10)
        self.data_source = self.worker_params.get('data_source', 'memory')
        self.processing_delay = self.worker_params.get('processing_delay', 0.1)
        
        # Simulate a data source
        self.pending_items = list(range(1, 101))  # Items 1-100
        self.processed_items = []
        
    def get_worker_id(self):
        return "batch_worker"
        
    def setup(self):
        """Setup the batch worker"""
        super().setup()
        self.logger.info(f"BatchWorker setup complete")
        self.logger.info(f"Batch size: {self.batch_size}")
        self.logger.info(f"Data source: {self.data_source}")
        self.logger.info(f"Processing delay: {self.processing_delay}s per item")
        self.logger.info(f"Total items to process: {len(self.pending_items)}")
        
    def do_work(self):
        """Process a batch of items"""
        if not self.pending_items:
            self.logger.info("No more items to process")
            return
            
        # Get next batch
        batch = self.get_next_batch()
        if not batch:
            self.logger.info("No items in current batch")
            return
            
        self.logger.info(f"Processing batch of {len(batch)} items")
        
        # Process the batch with tracking
        with self.track_operation('process_batch'):
            self.process_batch(batch)
            
        # Update statistics
        with self.track_operation('update_stats'):
            self.update_processing_stats(batch)
            
        self.logger.info(f"Batch completed. Remaining items: {len(self.pending_items)}")
        
    def get_next_batch(self) -> List[int]:
        """Get the next batch of items to process"""
        if not self.pending_items:
            return []
            
        batch_size = min(self.batch_size, len(self.pending_items))
        batch = self.pending_items[:batch_size]
        self.pending_items = self.pending_items[batch_size:]
        
        return batch
        
    def process_batch(self, batch: List[int]):
        """Process a batch of items"""
        for item in batch:
            self.logger.debug(f"Processing item {item}")
            
            # Simulate processing time
            time.sleep(self.processing_delay)
            
            # Simulate occasional processing errors
            if random.random() < 0.05:  # 5% error rate
                self.logger.warning(f"Failed to process item {item}")
                continue
                
            # Mark as processed
            self.processed_items.append(item)
            
        self.logger.info(f"Successfully processed {len([i for i in batch if i in self.processed_items])} items")
        
    def update_processing_stats(self, batch: List[int]):
        """Update processing statistics"""
        # This could update a database, send metrics, etc.
        total_processed = len(self.processed_items)
        total_items = total_processed + len(self.pending_items)
        completion_rate = (total_processed / total_items) * 100
        
        self.logger.info(f"Progress: {total_processed}/{total_items} ({completion_rate:.1f}%)")
        
    def cleanup(self):
        """Cleanup when worker stops"""
        super().cleanup()
        self.logger.info(f"BatchWorker cleanup - Processed {len(self.processed_items)} items total")


class DatabaseBatchWorker(BatchWorker):
    """
    Example of a batch worker that could work with a database
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Database connection parameters
        self.db_url = self.worker_params.get('db_url')
        self.table_name = self.worker_params.get('table_name', 'items')
        
    def get_worker_id(self):
        return "db_batch_worker"
        
    def setup(self):
        """Setup database connection"""
        super().setup()
        if self.db_url:
            self.logger.info(f"Would connect to database: {self.db_url}")
            self.logger.info(f"Would process table: {self.table_name}")
        else:
            self.logger.warning("No database URL provided, using mock data")
            
    def get_next_batch(self) -> List[Dict[str, Any]]:
        """Get next batch from database (simulated)"""
        if not self.pending_items:
            return []
            
        # In real implementation, this would be a database query like:
        # SELECT * FROM items WHERE status = 'pending' LIMIT batch_size
        
        batch_size = min(self.batch_size, len(self.pending_items))
        batch_ids = self.pending_items[:batch_size]
        self.pending_items = self.pending_items[batch_size:]
        
        # Convert to dict format (simulating database records)
        batch = [{'id': item_id, 'data': f'data_{item_id}'} for item_id in batch_ids]
        return batch
        
    def process_batch(self, batch: List[Dict[str, Any]]):
        """Process database records"""
        for record in batch:
            item_id = record['id']
            self.logger.debug(f"Processing database record {item_id}")
            
            # Simulate processing
            time.sleep(self.processing_delay)
            
            # In real implementation, would update database:
            # UPDATE items SET status = 'processed' WHERE id = item_id
            
            self.processed_items.append(item_id)
            
        self.logger.info(f"Updated {len(batch)} database records")


if __name__ == '__main__':
    # Example of running the batch worker directly
    worker = BatchWorker(
        worker_id="batch_worker_001",
        log_dir="./logs",
        wait_seconds=5,   # 5 second intervals
        max_cycles=10,    # Stop after 10 cycles
        batch_size=5,
        processing_delay=0.2
    )
    
    print("Starting BatchWorker...")
    print("Press Ctrl+C to stop")
    
    try:
        worker.run()
    except KeyboardInterrupt:
        print("\nStopped by user")
