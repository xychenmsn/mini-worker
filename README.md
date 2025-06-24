# Mini-Worker

A simple, parameter-driven worker framework with internal loops and file-based monitoring.

## Features

- **Parameter-driven**: No external configuration files required - all settings passed as parameters
- **Internal loops**: Workers run continuously with configurable wait times between iterations
- **File-based monitoring**: Status and logs written to specified directories
- **Process tracking**: Creates worker_id files with process IDs for monitoring
- **Worker management**: Built-in manager for starting, stopping, and monitoring workers
- **Simple CLI**: Easy command-line interface for running workers
- **Flexible execution**: Support for batch processing and continuous operation
- **Statistics tracking**: Built-in performance monitoring and rate calculation
- **Compatibility**: Drop-in replacement for existing worker systems

## Installation

```bash
pip install mini-worker
```

## Quick Start

### 1. Create a Worker

```python
from mini_worker import BaseMiniWorker

class MyWorker(BaseMiniWorker):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Initialize your worker-specific components
        # Dependencies are imported normally - no injection needed!

    def do_work(self):
        """Implement your work logic here"""
        with self.track_operation('my_operation'):
            print("Doing some work...")
            # Your actual work logic

    def get_worker_id(self):
        return "my_worker"
```

### 2. Run via CLI

```bash
# Run worker with 5-minute intervals
mini-worker run --worker-class=MyWorker --log-dir=/var/log/workers --wait-seconds=300

# Run with custom parameters
mini-worker run --worker-class=MyWorker --log-dir=/tmp/logs --wait-seconds=60 \
    --worker-params='{"param1": "value1", "param2": 123}'
```

### 3. Run Programmatically

```python
worker = MyWorker(
    worker_id="my_worker_001",
    log_dir="/var/log/workers",
    stats_dir="/var/log/workers",
    wait_seconds=300
)
worker.run()
```

### 4. Use Worker Manager

```python
from mini_worker import MiniWorkerManager

# Create manager
manager = MiniWorkerManager(log_dir="/var/log/workers")

# Register workers
manager.register_worker("my_worker", "mymodule.MyWorker")

# Start worker
manager.start_worker_with_params("my_worker", {"param1": "value1"})

# Check status
status = manager.get_worker_status("my_worker")

# Stop worker
manager.stop_worker("my_worker")
```

## CLI Commands

### `mini-worker run`

Run a worker directly.

Options:

- `--worker-class`: Python class name of the worker to run (required)
- `--log-dir`: Directory for log files (default: current directory)
- `--stats-dir`: Directory for stats files (default: same as log-dir)
- `--wait-seconds`: Seconds to wait between work cycles (default: 600)
- `--worker-params`: JSON string of worker-specific parameters
- `--max-cycles`: Maximum number of work cycles before stopping (default: unlimited)
- `--worker-id`: Override worker ID (default: use worker class default)

### `mini-worker status`

Check worker status.

Options:

- `--stats-dir`: Directory containing stats files (default: current directory)
- `--worker-id`: Show status for specific worker ID
- `--format`: Output format - 'text' or 'json' (default: text)

## Monitoring

Mini-worker creates several files for monitoring:

- `{worker_id}.log`: Worker log file with rotating logs
- `{worker_id}.stats`: Human-readable statistics
- `{worker_id}.json`: Detailed statistics in JSON format
- `{worker_id}.pid`: Process ID file for monitoring

## Migration from Existing Workers

Mini-worker is designed as a drop-in replacement for existing worker systems:

```python
# Before (existing worker)
from src.workers.base_worker import BaseWorker

class MyWorker(BaseWorker):
    def __init__(self):
        super().__init__()
        # Same initialization code

    def do_work(self):
        with self.calc_one('my_operation'):
            # Same work logic
            pass

# After (mini-worker)
from mini_worker import BaseMiniWorker

class MyWorker(BaseMiniWorker):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Same initialization code

    def get_worker_id(self):
        return "my_worker"

    def do_work(self):
        with self.calc_one('my_operation'):  # Still works!
            # Same work logic
            pass
```

Key changes:

1. Import `BaseMiniWorker` instead of `BaseWorker`
2. Add `**kwargs` to `__init__` and call `super().__init__(**kwargs)`
3. Implement `get_worker_id()` method
4. `calc_one()` still works for compatibility, or use `track_operation()`

## Examples

See the `examples/` directory for complete working examples.

## License

MIT License
