#!/usr/bin/env python3
"""
Integration example showing how mini-worker replaces your current worker system
"""

from mini_worker import BaseMiniWorker, MiniWorkerManager
import json


# Example: Your SpiderWorker migrated to mini-worker
class NewsSpiderWorker(BaseMiniWorker):
    """
    Migrated version of your SpiderWorker.
    
    Changes from original:
    1. Inherit from BaseMiniWorker instead of BaseWorker
    2. Add **kwargs to __init__
    3. Add get_worker_id() method
    4. Everything else stays the same!
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # Same imports and initialization as your current worker
        # from src.spider.spider_client import SpiderAPIClient
        # from src.db import db
        # self.spider_client = SpiderAPIClient()
        # self.db = db
        
        # Get parameters (replaces config system)
        self.spider_api_url = self.worker_params.get('spider_api_url', 'http://localhost:8003')
        self.max_articles = self.worker_params.get('max_articles_per_iteration', 10)
        
    def get_worker_id(self):
        return "spider_worker"
        
    def do_work(self):
        """Same logic as your current SpiderWorker.do_work()"""
        with self.calc_one('crawl_sites'):  # calc_one still works!
            # Your existing crawling logic
            self.logger.info(f"Crawling sites (max {self.max_articles} articles)")
            # ... existing implementation


# Example: Your SpiderArticleWorker migrated to mini-worker  
class NewsSpiderArticleWorker(BaseMiniWorker):
    """Migrated version of your SpiderArticleWorker"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # Same imports and initialization
        # from src.ai.spider_article_processor import SpiderArticleProcessor
        # from src.ai.spider_article_clusterer import SpiderArticleClusterer
        # self.processor = SpiderArticleProcessor()
        # self.clusterer = SpiderArticleClusterer()
        
        # Get parameters
        self.max_articles = self.worker_params.get('max_articles_per_iteration', 20)
        self.steps = self.worker_params.get('steps', ['cleanup', 'summarize', 'categorize'])
        
    def get_worker_id(self):
        return "spider_article_worker"
        
    def do_work(self):
        """Same logic as your current SpiderArticleWorker.do_work()"""
        with self.calc_one('process_articles'):
            # Your existing article processing logic
            self.logger.info(f"Processing articles (max {self.max_articles})")
            # ... existing implementation


# Example: Replacement for your WorkerManager
class NewsWorkerManager:
    """
    Drop-in replacement for your current WorkerManager.
    
    Provides the same interface for your REST API routes.
    """
    
    def __init__(self, log_dir="/var/log/workers", stats_dir=None):
        self.manager = MiniWorkerManager(
            log_dir=log_dir,
            stats_dir=stats_dir or log_dir
        )
        
        # Register your workers (replaces workers.toml)
        self.manager.register_worker("spider_worker", "your_project.workers.NewsSpiderWorker")
        self.manager.register_worker("spider_article_worker", "your_project.workers.NewsSpiderArticleWorker")
        
    def start_worker_with_params(self, worker_name: str, parameters: dict):
        """Same interface as your current WorkerManager.start_worker_with_params()"""
        try:
            self.manager.start_worker_with_params(worker_name, parameters)
            return True
        except Exception as e:
            self.logger.error(f"Failed to start worker {worker_name}: {e}")
            raise
            
    def stop_worker(self, worker_name: str):
        """Same interface as your current WorkerManager.stop_worker()"""
        try:
            self.manager.stop_worker(worker_name)
            return True
        except Exception as e:
            self.logger.error(f"Failed to stop worker {worker_name}: {e}")
            raise
            
    def get_worker_status(self, worker_name: str):
        """Same interface as your current WorkerManager.get_worker_status()"""
        return self.manager.get_worker_status(worker_name)
        
    def get_worker_statuses(self):
        """Same interface as your current WorkerManager.get_worker_statuses()"""
        return self.manager.get_worker_statuses()
        
    def is_worker_running(self, worker_name: str):
        """Same interface as your current WorkerManager.is_worker_running()"""
        return self.manager.is_worker_running(worker_name)


# Example: Your REST API routes (no changes needed!)
"""
Your existing routes in backend/src/routes/admin/worker_routes.py work unchanged:

@router.post("/start-with-params/{worker_name}")
async def start_worker_with_params(worker_name: str, request: StartWorkerWithParamsRequest):
    # This code stays exactly the same!
    worker_manager.start_worker_with_params(worker_name, request.parameters)
    return {"message": f"Worker {worker_name} started successfully"}

@router.post("/stop/{worker_name}")  
async def stop_worker(worker_name: str):
    # This code stays exactly the same!
    worker_manager.stop_worker(worker_name)
    return {"message": f"Worker {worker_name} stopped successfully"}

# Just replace the worker_manager instance:
# OLD: worker_manager = WorkerManager()
# NEW: worker_manager = NewsWorkerManager()
"""


# Example: Configuration bridge (optional)
def create_worker_manager_from_config(config):
    """
    Helper to create worker manager from your existing config system.
    This bridges the gap during migration.
    """
    manager = NewsWorkerManager(
        log_dir=config.logs_path('workers'),
        stats_dir=config.logs_path('workers')
    )
    return manager


# Example: Direct execution (same as current system)
def run_spider_worker_directly():
    """
    Example of running worker directly (like your current run_spider_worker.sh)
    """
    worker = NewsSpiderWorker(
        worker_id="spider_worker_direct",
        log_dir="/var/log/workers",
        wait_seconds=600,  # 10 minutes
        spider_api_url="http://localhost:8003",
        max_articles_per_iteration=10
    )
    
    # This replaces: python -m src.workers.spider_worker
    worker.run()


# Example: Command line usage (replaces your shell scripts)
"""
# OLD: ./run_spider_worker.sh
# NEW: 
mini-worker run --worker-class=your_project.workers.NewsSpiderWorker \
    --log-dir=/var/log/workers \
    --wait-seconds=600 \
    --worker-params='{"spider_api_url": "http://localhost:8003", "max_articles_per_iteration": 10}'

# OLD: python -m src.workers.spider_article_worker --max-articles-per-iteration=20
# NEW:
mini-worker run --worker-class=your_project.workers.NewsSpiderArticleWorker \
    --worker-params='{"max_articles_per_iteration": 20}'
"""


if __name__ == '__main__':
    print("Mini-Worker Integration Example")
    print("=" * 40)
    
    print("\n1. Your workers migrate with minimal changes:")
    print("   - Change inheritance: BaseWorker -> BaseMiniWorker")
    print("   - Add **kwargs to __init__")
    print("   - Add get_worker_id() method")
    print("   - Keep all existing logic and dependencies!")
    
    print("\n2. Your WorkerManager becomes MiniWorkerManager:")
    print("   - Same interface for REST API")
    print("   - Same methods: start_worker_with_params(), stop_worker(), etc.")
    print("   - No changes needed in your routes!")
    
    print("\n3. Your frontend works unchanged:")
    print("   - Same REST API endpoints")
    print("   - Same parameter passing")
    print("   - Same status monitoring")
    
    print("\n4. Command line execution:")
    print("   - Replace shell scripts with mini-worker CLI")
    print("   - Or keep direct Python execution")
    
    print("\n5. Benefits:")
    print("   - Standalone package (no project dependencies)")
    print("   - Better process management")
    print("   - Enhanced monitoring")
    print("   - Easier testing and deployment")
    
    print("\nMigration is a drop-in replacement!")
