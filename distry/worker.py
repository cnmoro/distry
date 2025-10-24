"""
Distry Worker - Execute tasks on worker nodes
"""

import pickle
import traceback
import base64
import subprocess
import sys
import importlib
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from typing import Callable, List, Any, Dict, Tuple, Set, Union
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import cloudpickle
import asyncio
from pydantic import BaseModel
from enum import Enum
import time
import uvicorn
from distry.exceptions import DistryError

app = FastAPI(title="Distry Worker", version="0.1.0")

# Worker state
SETTINGS = {
    "max_ram_gb": None
}
executor = ThreadPoolExecutor(max_workers=4)
active_jobs: Dict[str, Tuple[Callable, List[Tuple[int, Any]], Queue]] = {}
result_queues: Dict[str, Queue] = {}
completed_jobs: Dict[str, Dict] = {}
installed_packages: Set[str] = set()
job_semaphore = asyncio.Semaphore(1)

class JobStatus(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class StartJobRequest(BaseModel):
    job_id: str
    function: str
    inputs: List[Tuple[int, Any]]
    required_packages: List[str] = []

class JobInfo(BaseModel):
    job_id: str
    status: JobStatus
    completed: int
    total: int
    results: List[Union[Tuple[int, Any], Tuple[int, None, str, str]]]

def install_package(package_name: str) -> bool:
    """Install Python package using pip."""
    try:
        print(f"Installing {package_name}...")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", package_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        installed_packages.add(package_name)
        print(f"✓ Installed {package_name}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to install {package_name}: {e}")
        return False

def check_and_install_packages(packages: List[str]) -> Tuple[bool, List[str]]:
    """Check and install required packages."""
    missing_packages = []
    failed_packages = []
    
    for package in packages:
        if package in installed_packages:
            continue
            
        try:
            importlib.import_module(package)
            installed_packages.add(package)
        except ImportError:
            missing_packages.append(package)
            if not install_package(package):
                failed_packages.append(package)
    
    return not failed_packages, failed_packages

def extract_imports_from_function(func: Callable) -> List[str]:
    """Extract imports from pickled function."""
    imports = set()
    try:
        if hasattr(func, '__globals__'):
            for key, value in func.__globals__.items():
                if (hasattr(value, '__name__') and 
                    hasattr(value, '__package__') and 
                    value.__name__ not in sys.builtin_module_names):
                    module_name = value.__name__.split('.')[0]
                    imports.add(module_name)
    except Exception as e:
        print(f"Warning: Could not extract imports: {e}")
    
    return list(imports)

@app.post("/start_job")
async def start_job(request: StartJobRequest):
    """Start a new job."""
    job_id = request.job_id
    
    if job_id in active_jobs or job_id in completed_jobs:
        raise HTTPException(status_code=400, detail=f"Job {job_id} already exists")
    
    # Load function
    try:
        decoded_func = base64.b64decode(request.function.encode('utf-8'))
        func = cloudpickle.loads(decoded_func)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to load function: {e}")
    
    # Handle package requirements
    packages_to_check = request.required_packages
    if not packages_to_check:
        packages_to_check = extract_imports_from_function(func)
    
    if packages_to_check:
        success, failed = check_and_install_packages(packages_to_check)
        if not success:
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to install packages: {failed}"
            )
    
    # Initialize job
    result_queue = Queue()
    result_queues[job_id] = result_queue
    active_jobs[job_id] = (func, request.inputs, result_queue)
    
    # Start processing
    asyncio.create_task(process_job_queued(job_id))
    
    return {
        "status": "started",
        "job_id": job_id,
        "total_inputs": len(request.inputs),
        "packages": packages_to_check
    }

async def process_job_queued(job_id: str):
    """Acquire semaphore and then process job."""
    async with job_semaphore:
        await process_job(job_id)

async def process_job(job_id: str):
    """Process job inputs in background."""
    if job_id not in active_jobs:
        return
    
    func, inputs, result_queue = active_jobs[job_id]
    completed_count = 0
    total_inputs = len(inputs)
    results = []
    
    try:
        for global_idx, input_data in inputs:
            if job_id not in active_jobs:
                break
                
            try:
                future = executor.submit(func, input_data)
                result = future.result(timeout=30)
                result_queue.put((global_idx, result))
                results.append((global_idx, result))
                completed_count += 1
                
            except Exception as e:
                error_msg = str(e)
                tb = traceback.format_exc()
                result_queue.put((global_idx, None, error_msg, tb))
                results.append((global_idx, None, error_msg, tb))
                completed_count += 1
        
    finally:
        # Cleanup
        if job_id in active_jobs:
            del active_jobs[job_id]
        
        if job_id in result_queues:
            result_queue.put(None)
            del result_queues[job_id]
        
        completed_jobs[job_id] = {
            "job_id": job_id,
            "status": JobStatus.COMPLETED,
            "completed": completed_count,
            "total": total_inputs,
            "results": results,
            "completion_time": time.time()
        }
        
        print(f"Job {job_id} completed: {completed_count}/{total_inputs}")

@app.get("/results/{job_id}")
async def get_results(job_id: str):
    """Get job results."""
    if job_id in completed_jobs:
        job_data = completed_jobs[job_id]
        return JobInfo(**job_data)
    
    if job_id in active_jobs:
        _, inputs, _ = active_jobs[job_id]
        return {
            "job_id": job_id,
            "status": JobStatus.RUNNING,
            "completed": 0,
            "total": len(inputs),
            "results": []
        }
    
    if job_id in result_queues:
        result_queue = result_queues[job_id]
        results = []
        
        while True:
            try:
                item = result_queue.get_nowait()
                if item is None:
                    break
                results.append(item)
            except:
                break
        
        status = JobStatus.RUNNING if job_id in active_jobs else JobStatus.COMPLETED
        return {
            "job_id": job_id,
            "status": status,
            "completed": len(results),
            "total": len(results) if status == JobStatus.COMPLETED else len(active_jobs[job_id][1]),
            "results": results
        }
    
    raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

@app.delete("/cancel_job/{job_id}")
async def cancel_job(job_id: str):
    """Cancel running job."""
    if job_id in active_jobs:
        del active_jobs[job_id]
        completed_jobs[job_id] = {
            "status": JobStatus.CANCELLED,
            "results": [],
            "total": 0,
            "completion_time": time.time()
        }
    
    if job_id in result_queues:
        result_queues[job_id].put(None)
    
    return {"status": "cancelled"}

@app.get("/status")
async def get_status():
    """Get worker status."""
    return {
        "is_busy": len(active_jobs) > 0,
        "running_jobs": list(active_jobs.keys()),
        "completed_jobs": len(completed_jobs),
        "max_workers": executor._max_workers,
        "installed_packages": list(installed_packages),
        "settings": SETTINGS
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "worker_id": id(executor)}

@app.get("/installed_packages")
async def get_installed_packages():
    """List installed packages."""
    return {"packages": list(installed_packages)}


@app.post("/testing/update_settings")
async def testing_update_settings(settings: Dict):
    """(For testing only) Update worker settings."""
    SETTINGS.update(settings)
    return {"status": "updated", "settings": SETTINGS}


class WorkerServer:
    """Worker server wrapper."""
    
    def __init__(self, host: str = "127.0.0.1", port: int = 8000):
        self.host = host
        self.port = port
        self.server = None
    
    def run(self):
        """Run the worker server."""
        uvicorn.run(
            "distry.worker:app",
            host=self.host,
            port=self.port,
            log_level="info",
            reload=False
        )
    
    def start(self, block: bool = True):
        """Start server in background."""
        import threading
        
        def _run():
            uvicorn.run(
                "distry.worker:app",
                host=self.host,
                port=self.port,
                log_level="info",
                reload=False
            )
        
        self.server = threading.Thread(target=_run, daemon=True)
        self.server.start()
        
        if block:
            self.server.join()
    
    def stop(self):
        """Stop the server."""
        if self.server and self.server.is_alive():
            self.server.join(timeout=5)

import click

def parse_ram(ram_str: str) -> float:
    """Parse RAM string like '5g', '512m' to GB."""
    ram_str = ram_str.lower().strip()
    if ram_str.endswith('g'):
        return float(ram_str[:-1])
    elif ram_str.endswith('m'):
        return float(ram_str[:-1]) / 1024
    elif ram_str.endswith('gb'):
        return float(ram_str[:-2])
    elif ram_str.endswith('mb'):
        return float(ram_str[:-2]) / 1024
    return float(ram_str)

@click.command()
@click.option("--host", default="127.0.0.1", help="Host to bind to.")
@click.option("--port", default=8000, help="Port to bind to.")
@click.option("--max-ram", default=None, help="Max RAM usage (e.g. '4g', '512m').")
def main(host, port, max_ram):
    """Run a Distry worker."""
    if max_ram:
        SETTINGS["max_ram_gb"] = parse_ram(max_ram)
        print(f"RAM limit set to {SETTINGS['max_ram_gb']:.2f} GB")

    print(f"Starting Distry worker on {host}:{port}...")
    ws = WorkerServer(host=host, port=port)
    ws.run()

if __name__ == "__main__":
    main()
