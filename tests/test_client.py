import threading
import time
import pytest
import uvicorn
from distry.client import Client
from distry.exceptions import WorkerUnavailableError
from distry.worker import app

def add_one(x):
    return x + 1

def faulty_function(x):
    if x == 2:
        raise ValueError("I don't like 2")
    return x

@pytest.fixture(scope="module")
def run_worker():
    """Start worker in a background thread."""

    config = uvicorn.Config(app, host="127.0.0.1", port=8001, log_level="info")
    server = uvicorn.Server(config)

    worker_thread = threading.Thread(target=server.run)
    worker_thread.daemon = True
    worker_thread.start()

    time.sleep(2)  # Allow server to start

    yield "http://127.0.0.1:8001"

def test_client_initialization_no_workers():
    """Test client initialization with no healthy workers."""
    with pytest.raises(WorkerUnavailableError):
        Client(["http://localhost:9999"], max_concurrent_jobs=1)

def test_client_map_simple(run_worker):
    """Test client map with a simple function."""

    client = Client([run_worker])

    inputs = list(range(10))
    results = client.map(add_one, inputs)

    assert results == [i + 1 for i in inputs]

    client.close()

def test_client_map_with_failures(run_worker):
    """Test client map with some failing inputs."""

    client = Client([run_worker])

    inputs = list(range(5))
    results = client.map(faulty_function, inputs)

    expected = [0, 1, None, 3, 4]

    assert results == expected

    client.close()

def test_client_batching_for_large_jobs(run_worker):
    """Test that large jobs are split into batches."""
    import requests
    from unittest.mock import patch
    import math

    # Set a low RAM limit on the worker
    requests.post(f"{run_worker}/testing/update_settings", json={"max_ram_gb": 0.0001})

    client = Client([run_worker])

    # This job will be larger than the RAM limit
    inputs = [list(range(10000)) for _ in range(5)]

    with patch.object(client, '_run_worker_job', wraps=client._run_worker_job) as spy:
        results = client.map(len, inputs)

        job_size_gb = client._estimate_job_size(len, inputs)
        # We need to access the worker's settings for the correct assertion
        worker_max_ram = client.worker_clients[0].max_ram_gb
        expected_batches = math.ceil(job_size_gb / worker_max_ram)

        assert spy.call_count == expected_batches

    assert results == [10000] * 5

    # Reset RAM limit
    requests.post(f"{run_worker}/testing/update_settings", json={"max_ram_gb": None})

    client.close()

def test_import_extraction():
    """Test extraction of inline and module-level imports."""
    from distry.client import extract_imports_from_function
    import numpy
    import pandas as pd

    def my_func(x):
        import scipy
        return numpy.mean(x) + pd.Series(x).mean() + scipy.pi

    packages = extract_imports_from_function(my_func)
    assert set(packages) == {'numpy', 'pandas', 'scipy'}
