import pickle
import traceback
import base64
import subprocess
import sys
import importlib
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from typing import Callable, List, Any, Dict, Tuple, Set
from fastapi import FastAPI, HTTPException, Path
from fastapi.responses import JSONResponse
import cloudpickle
import asyncio
from pydantic import BaseModel
from enum import Enum
import time

app = FastAPI()

# Worker state
executor = ThreadPoolExecutor(max_workers=4)
active_jobs: Dict[str, Tuple[Callable, List[Tuple[int, Any]], Queue]] = {}
result_queues: Dict[str, Queue] = {}
completed_jobs: Dict[str, Dict] = {}
installed_packages: Set[str] = set()  # Track installed packages

class JobStatus(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class StartJobRequest(BaseModel):
    job_id: str
    function: str
    inputs: List[Tuple[int, Any]]
    required_packages: List[str] = []  # New field for package requirements

class JobInfo(BaseModel):
    job_id: str
    status: JobStatus
    completed: int
    total: int
    results: List[Tuple[int, Any]]

def install_package(package_name: str) -> bool:
    """Install a package using pip"""
    try:
        print(f"Installing package: {package_name}")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", package_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        installed_packages.add(package_name)
        print(f"Successfully installed: {package_name}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Failed to install {package_name}: {str(e)}")
        return False

def check_and_install_packages(packages: List[str]) -> Tuple[bool, List[str]]:
    """Check if packages are available, install if missing"""
    missing_packages = []
    failed_packages = []
    
    for package in packages:
        if package in installed_packages:
            continue
            
        try:
            # Try to import the package
            importlib.import_module(package)
            installed_packages.add(package)
        except ImportError:
            # Package not found, try to install
            missing_packages.append(package)
            if not install_package(package):
                failed_packages.append(package)
    
    if failed_packages:
        return False, failed_packages
    return True, []

def extract_imports_from_function(func: Callable) -> List[str]:
    """
    Extract import statements from function's code and closure
    This is a best-effort approach using cloudpickle's globals
    """
    imports = set()
    
    try:
        # Get globals from the function
        if hasattr(func, '__globals__'):
            for key, value in func.__globals__.items():
                # Check if it's a module
                if hasattr(value, '__name__') and hasattr(value, '__package__'):
                    module_name = value.__name__.split('.')[0]
                    # Skip built-in modules
                    if module_name not in sys.builtin_module_names:
                        imports.add(module_name)
    except Exception as e:
        print(f"Warning: Could not extract imports: {str(e)}")
    
    return list(imports)

@app.post("/start_job")
async def start_job(request: StartJobRequest):
    job_id = request.job_id
    
    if job_id in active_jobs or job_id in completed_jobs:
        raise HTTPException(status_code=400, detail=f"Job {job_id} already running or completed")
    
    # Decode and load function
    try:
        decoded_func = base64.b64decode(request.function.encode('utf-8'))
        func = cloudpickle.loads(decoded_func)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to load function: {str(e)}")
    
    # Extract imports from function if not provided
    packages_to_check = request.required_packages
    if not packages_to_check:
        packages_to_check = extract_imports_from_function(func)
        print(f"Auto-detected packages: {packages_to_check}")
    
    # Check and install required packages
    if packages_to_check:
        success, failed = check_and_install_packages(packages_to_check)
        if not success:
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to install required packages: {', '.join(failed)}"
            )
    
    # Create result queue
    result_queue = Queue()
    result_queues[job_id] = result_queue
    
    # Store job state
    active_jobs[job_id] = (func, request.inputs, result_queue)
    
    # Start processing
    try:
        task = asyncio.create_task(process_job(job_id))
    except Exception as e:
        if job_id in active_jobs:
            del active_jobs[job_id]
        if job_id in result_queues:
            del result_queues[job_id]
        raise HTTPException(status_code=500, detail=f"Failed to start job processing: {str(e)}")
    
    return {
        "status": "started",
        "job_id": job_id,
        "total_inputs": len(request.inputs),
        "packages_installed": packages_to_check
    }

async def process_job(job_id: str):
    """Process all inputs for a job in background"""
    if job_id not in active_jobs:
        if job_id in result_queues:
            result_queues[job_id].put(None)
            completed_jobs[job_id] = {
                "status": JobStatus.CANCELLED,
                "results": [],
                "total": 0,
                "completion_time": time.time()
            }
            del result_queues[job_id]
        return
    
    func, inputs, result_queue = active_jobs[job_id]
    
    completed_count = 0
    total_inputs = len(inputs)
    results = []
    
    try:
        for global_idx, input_data in inputs:
            if job_id not in active_jobs:
                completed_jobs[job_id] = {
                    "status": JobStatus.CANCELLED,
                    "results": results,
                    "total": total_inputs,
                    "completion_time": time.time()
                }
                result_queue.put(None)
                break
                
            try:
                future = executor.submit(func, input_data)
                result = future.result(timeout=30)
                
                result_queue.put((global_idx, result))
                results.append((global_idx, result))
                completed_count += 1
                
            except ImportError as e:
                # Handle missing imports during execution
                error_msg = f"Missing dependency: {str(e)}"
                tb = traceback.format_exc()
                result_queue.put((global_idx, None, error_msg, tb))
                results.append((global_idx, None, error_msg, tb))
                completed_count += 1
                
            except Exception as e:
                error_msg = str(e)
                tb = traceback.format_exc()
                result_queue.put((global_idx, None, error_msg, tb))
                results.append((global_idx, None, error_msg, tb))
                completed_count += 1
        
    except Exception as e:
        print(f"Unexpected error processing job {job_id}: {str(e)}")
        error_result = (None, None, f"Processing error: {str(e)}", traceback.format_exc())
        if job_id in result_queues:
            result_queue.put(error_result)
            results.append(error_result)
    
    finally:
        if job_id in active_jobs:
            del active_jobs[job_id]
        
        if job_id in result_queues:
            result_queue.put(None)
        
        completed_jobs[job_id] = {
            "status": JobStatus.COMPLETED,
            "results": results,
            "total": total_inputs,
            "completion_time": time.time()
        }
        
        print(f"Job {job_id} completed: {completed_count}/{total_inputs} inputs processed")
        asyncio.create_task(cleanup_old_jobs())

async def cleanup_old_jobs():
    """Clean up completed jobs older than 5 minutes"""
    await asyncio.sleep(300)
    current_time = time.time()
    expired_jobs = [
        job_id for job_id, data in completed_jobs.items() 
        if current_time - data["completion_time"] > 300
    ]
    for job_id in expired_jobs:
        del completed_jobs[job_id]
        if job_id in result_queues:
            del result_queues[job_id]
    print(f"Cleaned up {len(expired_jobs)} expired jobs")

@app.get("/results/{job_id}")
async def get_results(job_id: str = Path(...)):
    """Get available results for a job"""
    if job_id in completed_jobs:
        job_data = completed_jobs[job_id]
        return {
            "job_id": job_id,
            "status": job_data["status"],
            "completed": len(job_data["results"]),
            "total": job_data["total"],
            "results": job_data["results"]
        }
    
    if job_id in active_jobs:
        func, inputs, _ = active_jobs[job_id]
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
                    status = JobStatus.COMPLETED
                    break
                results.append(item)
            except:
                break
        
        if job_id in active_jobs:
            status = JobStatus.RUNNING
            completed = len(results)
            total = len(active_jobs[job_id][1])
        else:
            status = JobStatus.COMPLETED
            completed = len(results)
            total = len(results)
        
        return {
            "job_id": job_id,
            "status": status,
            "completed": completed,
            "total": total,
            "results": results
        }
    
    raise HTTPException(status_code=404, detail=f"Job {job_id} not found or completed")

@app.delete("/cancel_job/{job_id}")
async def cancel_job(job_id: str = Path(...)):
    """Cancel a running job"""
    if job_id in active_jobs:
        del active_jobs[job_id]
        if job_id not in completed_jobs:
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
    """Get worker status"""
    running_jobs = list(active_jobs.keys())
    completed_count = len(completed_jobs)
    return {
        "is_busy": len(running_jobs) > 0,
        "running_jobs": running_jobs,
        "completed_jobs": completed_count,
        "max_workers": executor._max_workers,
        "queue_capacity": 1000,
        "installed_packages": list(installed_packages)
    }

@app.get("/health")
async def health_check():
    """Simple health check"""
    return {"status": "healthy", "worker_id": id(executor)}

@app.post("/install_package")
async def manual_install_package(package_name: str):
    """Manually install a package"""
    if package_name in installed_packages:
        return {"status": "already_installed", "package": package_name}
    
    success = install_package(package_name)
    if success:
        return {"status": "installed", "package": package_name}
    else:
        raise HTTPException(status_code=500, detail=f"Failed to install {package_name}")

@app.get("/installed_packages")
async def get_installed_packages():
    """Get list of installed packages"""
    return {"packages": list(installed_packages)}