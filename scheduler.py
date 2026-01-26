"""
ETL Scheduler - Runs the ETL pipeline on a configurable schedule.

This replaces the bash infinite loop in the Dockerfile with proper Python scheduling,
graceful shutdown handling, and better error recovery.

Usage:
    python scheduler.py                    # Run with default schedule (2 hours)
    python scheduler.py --interval 3600    # Run every 1 hour
    python scheduler.py --run-once         # Run once and exit
"""

import os
import sys
import time
import signal
import argparse
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import the main ETL function
try:
    from src.etl.etl import run_etl
except ImportError as e:
    print(f"ERROR: Could not import ETL function: {e}")
    print("Make sure you're running from the mk2 directory with the src package available.")
    sys.exit(1)


class ETLScheduler:
    """
    Scheduler for running ETL pipeline at regular intervals.

    Handles:
    - Configurable run intervals
    - Graceful shutdown on SIGTERM/SIGINT
    - Error recovery with backoff
    - Run statistics and logging
    """

    def __init__(self, interval_seconds=7200, run_once=False):
        """
        Initialize scheduler.

        Args:
            interval_seconds: Seconds between ETL runs (default: 7200 = 2 hours)
            run_once: If True, run once and exit
        """
        self.interval_seconds = interval_seconds
        self.run_once = run_once
        self.running = True
        self.run_count = 0
        self.success_count = 0
        self.error_count = 0
        self.last_run_time = None
        self.last_run_success = None

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

        print(f"{'='*60}")
        print(f"ETL Scheduler Initialized")
        print(f"{'='*60}")
        print(f"Interval: {self.interval_seconds} seconds ({self.interval_seconds/3600:.1f} hours)")
        print(f"Run mode: {'Single run' if run_once else 'Continuous'}")
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}\n")

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals gracefully."""
        print(f"\n{'='*60}")
        print(f"Received shutdown signal ({signal.Signals(signum).name})")
        print(f"Finishing current operation and shutting down...")
        print(f"{'='*60}\n")
        self.running = False

    def _run_etl_with_retry(self, max_retries=3, retry_delay=60):
        """
        Run ETL with retry logic.

        Args:
            max_retries: Maximum number of retry attempts
            retry_delay: Seconds to wait between retries

        Returns:
            bool: True if ETL succeeded, False otherwise
        """
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    print(f"Retry attempt {attempt}/{max_retries} after {retry_delay}s delay...")
                    time.sleep(retry_delay)

                print(f"\n{'='*60}")
                print(f"ETL Run #{self.run_count + 1}")
                print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"{'='*60}\n")

                start_time = time.time()

                # Run the ETL
                run_etl()

                elapsed_time = time.time() - start_time

                print(f"\n{'='*60}")
                print(f"✓ ETL Run #{self.run_count + 1} COMPLETED")
                print(f"Duration: {elapsed_time:.2f} seconds")
                print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"{'='*60}\n")

                return True

            except KeyboardInterrupt:
                # Don't retry on keyboard interrupt
                raise

            except Exception as e:
                print(f"\n{'='*60}")
                print(f"✗ ETL Run #{self.run_count + 1} FAILED")
                print(f"Error: {e}")
                print(f"{'='*60}\n")

                if attempt < max_retries - 1:
                    print(f"Will retry in {retry_delay} seconds...")
                else:
                    print(f"Max retries ({max_retries}) reached. Giving up on this run.")
                    import traceback
                    traceback.print_exc()

        return False

    def _print_statistics(self):
        """Print scheduler statistics."""
        print(f"\n{'='*60}")
        print(f"Scheduler Statistics")
        print(f"{'='*60}")
        print(f"Total runs: {self.run_count}")
        print(f"Successful: {self.success_count}")
        print(f"Failed: {self.error_count}")
        if self.run_count > 0:
            success_rate = (self.success_count / self.run_count) * 100
            print(f"Success rate: {success_rate:.1f}%")
        if self.last_run_time:
            print(f"Last run: {self.last_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Last run status: {'✓ Success' if self.last_run_success else '✗ Failed'}")
        print(f"{'='*60}\n")

    def _wait_for_next_run(self):
        """
        Wait for the next run with interruptible sleep.
        Checks for shutdown signal every second.
        """
        print(f"Next run in {self.interval_seconds} seconds ({self.interval_seconds/3600:.1f} hours)")
        print(f"Next run at: {datetime.fromtimestamp(time.time() + self.interval_seconds).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Press Ctrl+C to stop\n")

        # Sleep in 1-second increments to allow for graceful shutdown
        for _ in range(self.interval_seconds):
            if not self.running:
                break
            time.sleep(1)

    def run(self):
        """
        Main scheduler loop.

        Runs ETL at configured intervals until stopped.
        """
        try:
            while self.running:
                self.run_count += 1
                self.last_run_time = datetime.now()

                # Run ETL with retry logic
                success = self._run_etl_with_retry()

                # Update statistics
                self.last_run_success = success
                if success:
                    self.success_count += 1
                else:
                    self.error_count += 1

                # Print statistics after each run
                self._print_statistics()

                # Exit if run_once mode
                if self.run_once:
                    print("Run-once mode: Exiting after single execution")
                    break

                # Check if we should continue
                if not self.running:
                    break

                # Wait for next run
                self._wait_for_next_run()

        except KeyboardInterrupt:
            print("\n\nKeyboard interrupt received")

        finally:
            # Print final statistics
            print(f"\n{'='*60}")
            print(f"ETL Scheduler Stopped")
            print(f"{'='*60}")
            self._print_statistics()
            print(f"Shutdown at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*60}\n")


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='ETL Scheduler - Run ETL pipeline on a schedule',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scheduler.py                    # Run every 2 hours (default)
  python scheduler.py --interval 3600    # Run every 1 hour
  python scheduler.py --interval 1800    # Run every 30 minutes
  python scheduler.py --run-once         # Run once and exit

Environment variables:
  ETL_INTERVAL: Default interval in seconds (overridden by --interval)
        """
    )

    # Get default interval from environment or use 2 hours
    default_interval = int(os.getenv('ETL_INTERVAL', '7200'))

    parser.add_argument(
        '--interval',
        type=int,
        default=default_interval,
        help=f'Interval between runs in seconds (default: {default_interval})'
    )

    parser.add_argument(
        '--run-once',
        action='store_true',
        help='Run once and exit (useful for testing)'
    )

    return parser.parse_args()


if __name__ == '__main__':
    # Parse command line arguments
    args = parse_arguments()

    # Create and run scheduler
    scheduler = ETLScheduler(
        interval_seconds=args.interval,
        run_once=args.run_once
    )

    # Run the scheduler
    try:
        scheduler.run()
        exit_code = 0 if scheduler.error_count == 0 else 1
        sys.exit(exit_code)
    except Exception as e:
        print(f"\n{'='*60}")
        print(f"CRITICAL ERROR in scheduler")
        print(f"{'='*60}")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        print(f"{'='*60}\n")
        sys.exit(1)
