
import pytest
from distry import distry, register_workers
from .test_client import run_worker

def test_distry_decorator(run_worker):
    """Test the @distry decorator."""

    register_workers([run_worker])

    @distry
    def process_data(a, b=None):
        if b is not None:
            return a * b
        return a + 1

    # Test with a single argument
    result1 = process_data(10)
    assert result1 == 11

    # Test with multiple arguments
    result2 = process_data(10, b=2)
    assert result2 == 20
