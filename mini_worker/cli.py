"""
Command-line interface for mini-worker
"""

import sys
import os
import json
import click
from typing import Optional

from .utils import import_worker_class, parse_worker_params, validate_worker_class


@click.command()
@click.option('--worker-class', required=True, 
              help='Python class name of the worker to run (e.g., mymodule.MyWorker)')
@click.option('--log-dir', default=None,
              help='Directory for log files (default: current directory)')
@click.option('--stats-dir', default=None,
              help='Directory for stats files (default: same as log-dir)')
@click.option('--wait-seconds', type=int, default=600,
              help='Seconds to wait between work cycles (default: 600)')
@click.option('--max-cycles', type=int, default=None,
              help='Maximum number of work cycles before stopping (default: unlimited)')
@click.option('--worker-params', default='{}',
              help='JSON string of worker-specific parameters (default: {})')
@click.option('--worker-id', default=None,
              help='Override worker ID (default: use worker class default)')
@click.option('--verbose', '-v', is_flag=True,
              help='Enable verbose logging')
def main(worker_class: str, 
         log_dir: Optional[str],
         stats_dir: Optional[str], 
         wait_seconds: int,
         max_cycles: Optional[int],
         worker_params: str,
         worker_id: Optional[str],
         verbose: bool):
    """
    Mini-Worker: A simple, parameter-driven worker framework
    
    Examples:
    
    \b
    # Run a worker with 5-minute intervals
    mini-worker --worker-class=MyWorker --wait-seconds=300
    
    \b
    # Run with custom parameters
    mini-worker --worker-class=mymodule.MyWorker \\
        --worker-params='{"param1": "value1", "param2": 123}' \\
        --log-dir=/var/log/workers
    
    \b
    # Run for limited cycles
    mini-worker --worker-class=MyWorker --max-cycles=10
    """
    
    try:
        # Parse worker parameters
        try:
            params = parse_worker_params(worker_params)
        except ValueError as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)
            
        # Import worker class
        try:
            WorkerClass = import_worker_class(worker_class)
        except (ImportError, AttributeError) as e:
            click.echo(f"Error importing worker class '{worker_class}': {e}", err=True)
            sys.exit(1)
            
        # Validate worker class
        if not validate_worker_class(WorkerClass):
            click.echo(f"Error: '{worker_class}' is not a valid mini-worker class", err=True)
            click.echo("Worker classes must inherit from BaseMiniWorker and implement "
                      "get_worker_id() and do_work() methods", err=True)
            sys.exit(1)
            
        # Set up directories
        if log_dir is None:
            log_dir = os.getcwd()
        if stats_dir is None:
            stats_dir = log_dir
            
        # Create worker instance
        worker_kwargs = {
            'log_dir': log_dir,
            'stats_dir': stats_dir,
            'wait_seconds': wait_seconds,
            'max_cycles': max_cycles,
            **params  # Add worker-specific parameters
        }
        
        if worker_id:
            worker_kwargs['worker_id'] = worker_id
            
        if verbose:
            click.echo(f"Creating worker: {worker_class}")
            click.echo(f"Log directory: {log_dir}")
            click.echo(f"Stats directory: {stats_dir}")
            click.echo(f"Wait seconds: {wait_seconds}")
            click.echo(f"Max cycles: {max_cycles or 'unlimited'}")
            click.echo(f"Worker parameters: {json.dumps(params, indent=2)}")
            
        worker = WorkerClass(**worker_kwargs)
        
        if verbose:
            click.echo(f"Starting worker with ID: {worker.worker_id}")
            
        # Run the worker
        worker.run()
        
    except KeyboardInterrupt:
        click.echo("\nShutdown requested by user")
        sys.exit(0)
    except Exception as e:
        click.echo(f"Unexpected error: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@click.command()
@click.option('--stats-dir', default='.',
              help='Directory containing stats files (default: current directory)')
@click.option('--worker-id', default=None,
              help='Show status for specific worker ID')
@click.option('--format', 'output_format', type=click.Choice(['text', 'json']), 
              default='text', help='Output format (default: text)')
def status(stats_dir: str, worker_id: Optional[str], output_format: str):
    """Show worker status information"""
    
    from .utils import get_worker_status, is_worker_running
    import glob
    
    if worker_id:
        # Show specific worker
        status_data = get_worker_status(worker_id, stats_dir)
        if not status_data:
            click.echo(f"No status found for worker '{worker_id}'", err=True)
            sys.exit(1)
            
        running = is_worker_running(worker_id, stats_dir)
        status_data['is_running'] = running
        
        if output_format == 'json':
            click.echo(json.dumps(status_data, indent=2, default=str))
        else:
            _print_worker_status(worker_id, status_data, running)
    else:
        # Show all workers
        json_files = glob.glob(os.path.join(stats_dir, "*.json"))
        if not json_files:
            click.echo("No worker status files found")
            return
            
        workers = []
        for json_file in json_files:
            wid = os.path.basename(json_file)[:-5]  # Remove .json extension
            status_data = get_worker_status(wid, stats_dir)
            if status_data:
                running = is_worker_running(wid, stats_dir)
                status_data['is_running'] = running
                workers.append((wid, status_data, running))
                
        if output_format == 'json':
            result = {wid: data for wid, data, _ in workers}
            click.echo(json.dumps(result, indent=2, default=str))
        else:
            for wid, data, running in workers:
                _print_worker_status(wid, data, running)
                click.echo()  # Empty line between workers


def _print_worker_status(worker_id: str, status_data: dict, is_running: bool):
    """Print worker status in human-readable format"""
    click.echo(f"Worker: {worker_id}")
    click.echo(f"Status: {status_data.get('status', 'unknown')} ({'running' if is_running else 'stopped'})")
    click.echo(f"Total Cycles: {status_data.get('total_work_cycles', 0)}")
    
    if status_data.get('last_work_cycle_time'):
        click.echo(f"Last Cycle: {status_data['last_work_cycle_time']:.2f}s")
        
    operations = status_data.get('operations', {})
    if operations:
        click.echo("Operations:")
        for op_name, op_stats in sorted(operations.items()):
            rate = op_stats.get('rate_per_hour', 0)
            count = op_stats.get('count', 0)
            click.echo(f"  {op_name}: {rate:.1f}/hour ({count} total)")


# Create a click group for multiple commands
@click.group()
def cli():
    """Mini-Worker: A simple, parameter-driven worker framework"""
    pass


cli.add_command(main, name='run')
cli.add_command(status)


# For compatibility with setup.py entry point
def main_entry():
    """Entry point for mini-worker command"""
    cli()


if __name__ == '__main__':
    cli()
