from fastapi.testclient import TestClient
from distry.worker import app

client = TestClient(app)

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
