import pytest
from unittest.mock import Mock, patch
from distry.client import Client, WorkerClient
from distry.exceptions import WorkerUnavailableError

def test_worker_client_health():
    """Test worker client health check."""
    with patch('requests.Session.get') as mock_get:
        mock_response = Mock()
        mock_response.json.return_value = {"status": "healthy"}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        client = WorkerClient("http://test")
        assert client.health_check() is True

def test_client_initialization_no_workers():
    """Test client initialization with no healthy workers."""
    with pytest.raises(WorkerUnavailableError):
        Client(["http://unhealthy"], max_concurrent_jobs=1)
