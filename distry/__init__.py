"""
Distry - Distributed Task Execution Framework
"""

__version__ = '0.1.0'
__author__ = 'Your Name'

from .client import Client
from .worker import WorkerServer
from .exceptions import DistryError, WorkerUnavailableError, JobFailedError

__all__ = [
    'Client',
    'WorkerServer',
    'DistryError',
    'WorkerUnavailableError',
    'JobFailedError',
    'register_workers',
    'distry'
]

# Set default logging
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global registry for workers
_worker_urls = []

def register_workers(worker_urls: list[str]):
    """Register worker URLs for the @distry decorator."""
    global _worker_urls
    _worker_urls = worker_urls

def distry(func):
    """Decorator to distribute a function call to a worker."""
    def wrapper(*args, **kwargs):
        if not _worker_urls:
            # Fallback to local execution if no workers are registered
            return func(*args, **kwargs)

        client = Client(_worker_urls)
        return client.run(func, args, kwargs)

    return wrapper
