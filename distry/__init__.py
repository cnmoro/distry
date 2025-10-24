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
    'JobFailedError'
]

# Set default logging
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
