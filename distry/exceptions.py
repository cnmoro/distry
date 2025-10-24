"""Distry exceptions."""

class DistryError(Exception):
    """Base exception for distry errors."""
    pass

class WorkerUnavailableError(DistryError):
    """Raised when no workers are available."""
    pass

class JobFailedError(DistryError):
    """Raised when a job fails to execute."""
    def __init__(self, job_id, error_msg, traceback=None):
        self.job_id = job_id
        self.error_msg = error_msg
        self.traceback = traceback
        super().__init__(f"Job {job_id} failed: {error_msg}")

class WorkerCommunicationError(DistryError):
    """Raised when communication with worker fails."""
    pass
