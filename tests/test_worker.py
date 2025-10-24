import threading
import time
import base64
import pytest
import uvicorn
import requests
import cloudpickle
from fastapi.testclient import TestClient

from distry.worker import app

client = TestClient(app)

def add_one(x):
    return x + 1

@pytest.fixture(scope="module")
def run_worker():
    """Start worker in a background thread."""

    config = uvicorn.Config(app, host="127.0.0.1", port=8000, log_level="info")
    server = uvicorn.Server(config)

    worker_thread = threading.Thread(target=server.run)
    worker_thread.daemon = True
    worker_thread.start()

    time.sleep(2)

    yield "http://127.0.0.1:8000"

    # No clean shutdown for uvicorn server in thread
    # It will exit when the main thread exits

def test_health_check():
    """Test worker health endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_status_endpoint():
    """Test worker status endpoint."""
    response = client.get("/status")
    assert response.status_code == 200
    assert "is_busy" in response.json()

def test_submit_job_and_get_results(run_worker):
    """Test submitting a job and retrieving results."""

    worker_url = run_worker

    # Prepare the function and inputs
    pickled_func = cloudpickle.dumps(add_one)
    encoded_func = base64.b64encode(pickled_func).decode('utf-8')
    inputs = [(i, i) for i in range(5)]

    # Start the job
    start_payload = {
        "job_id": "test_job_123",
        "function": encoded_func,
        "inputs": inputs,
        "required_packages": []
    }

    start_response = requests.post(f"{worker_url}/start_job", json=start_payload)
    assert start_response.status_code == 200
    assert start_response.json()["status"] == "started"

    # Poll for results
    results_url = f"{worker_url}/results/test_job_123"

    for _ in range(20):
        results_response = requests.get(results_url)
        assert results_response.status_code == 200

        data = results_response.json()

        if data["status"] == "completed":
            break

        time.sleep(0.1)

    # Verify results
    assert data["status"] == "completed", "Job did not complete in time"

    # Note: The results may not be ordered
    results = sorted(data["results"], key=lambda x: x[0])

    assert len(results) == 5

    for i in range(5):
        assert results[i][0] == i
        assert results[i][1] == i + 1

def task_that_returns_time(duration):
    """A simple task that sleeps and returns timestamps."""
    start = time.time()
    time.sleep(duration)
    end = time.time()
    return start, end

def test_job_queueing(run_worker):
    """Test that jobs are queued and processed sequentially."""
    worker_url = run_worker

    # Job 1
    pickled_func_1 = cloudpickle.dumps(task_that_returns_time)
    encoded_func_1 = base64.b64encode(pickled_func_1).decode('utf-8')
    job_1_payload = {
        "job_id": "job_1",
        "function": encoded_func_1,
        "inputs": [(0, 1)],
        "required_packages": []
    }

    # Job 2
    pickled_func_2 = cloudpickle.dumps(task_that_returns_time)
    encoded_func_2 = base64.b64encode(pickled_func_2).decode('utf-8')
    job_2_payload = {
        "job_id": "job_2",
        "function": encoded_func_2,
        "inputs": [(0, 1)],
        "required_packages": []
    }

    # Start both jobs
    start_response_1 = requests.post(f"{worker_url}/start_job", json=job_1_payload)
    assert start_response_1.status_code == 200
    start_response_2 = requests.post(f"{worker_url}/start_job", json=job_2_payload)
    assert start_response_2.status_code == 200

    # Wait for both jobs to complete
    job_1_results = None
    job_2_results = None

    for _ in range(40):
        if not job_1_results:
            results_response_1 = requests.get(f"{worker_url}/results/job_1")
            if results_response_1.json()["status"] == "completed":
                job_1_results = results_response_1.json()

        if not job_2_results:
            results_response_2 = requests.get(f"{worker_url}/results/job_2")
            if results_response_2.json()["status"] == "completed":
                job_2_results = results_response_2.json()

        if job_1_results and job_2_results:
            break

        time.sleep(0.1)

    assert job_1_results is not None, "Job 1 did not complete in time"
    assert job_2_results is not None, "Job 2 did not complete in time"

    # Verify that job 2 started after job 1 finished
    job_1_start_time = job_1_results["results"][0][1][0]
    job_1_end_time = job_1_results["results"][0][1][1]
    job_2_start_time = job_2_results["results"][0][1][0]
    job_2_end_time = job_2_results["results"][0][1][1]

    assert job_2_start_time >= job_1_end_time

def test_parse_ram_settings():
    """Test parsing of --max-ram parameter."""
    from distry.worker import main as worker_main
    from click.testing import CliRunner
    from distry.worker import SETTINGS
    from unittest.mock import patch

    runner = CliRunner()

    with patch("uvicorn.run") as mock_run:
        # Test with 'g'
        result = runner.invoke(worker_main, ["--max-ram", "5g"], catch_exceptions=False)
        assert SETTINGS["max_ram_gb"] == 5.0

        # Test with 'm'
        result = runner.invoke(worker_main, ["--max-ram", "512m"], catch_exceptions=False)
        assert SETTINGS["max_ram_gb"] == 0.5

        # Test with 'gb'
        result = runner.invoke(worker_main, ["--max-ram", "2gb"], catch_exceptions=False)
        assert SETTINGS["max_ram_gb"] == 2.0

        # Test float
        result = runner.invoke(worker_main, ["--max-ram", "1.5"], catch_exceptions=False)
        assert SETTINGS["max_ram_gb"] == 1.5
