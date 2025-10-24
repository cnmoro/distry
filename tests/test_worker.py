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
