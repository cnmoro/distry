import time
import random
import base64
import inspect
import ast
from typing import Callable, List, Any, Optional, Dict, Tuple, Set
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import cloudpickle
from dataclasses import dataclass
from enum import Enum

@dataclass
class WorkerResult:
    worker_url: str
    job_id: str
    total_inputs: int
    results: List[tuple]

class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

def extract_imports_from_function(func: Callable) -> Set[str]:
    """
    Extract import statements from function source code
    Returns set of top-level package names
    """
    imports = set()
    
    try:
        # Get source code
        source = inspect.getsource(func)
        tree = ast.parse(source)
        
        # Walk the AST to find imports
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    # Get top-level package name
                    package = alias.name.split('.')[0]
                    imports.add(package)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    # Get top-level package name
                    package = node.module.split('.')[0]
                    imports.add(package)
        
        # Also check function's globals for imported modules
        if hasattr(func, '__globals__'):
            for key, value in func.__globals__.items():
                if hasattr(value, '__name__') and hasattr(value, '__package__'):
                    module_name = value.__name__.split('.')[0]
                    imports.add(module_name)
                    
    except Exception as e:
        print(f"Warning: Could not extract imports from function: {str(e)}")
    
    # Filter out built-in modules
    builtin_modules = {
        'sys', 'os', 'time', 'math', 'random', 'datetime', 'json', 
        'collections', 'itertools', 're', 'copy', 'functools'
    }
    imports = imports - builtin_modules
    
    return imports

class WorkerClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})
    
    def start_job(
        self, 
        job_id: str, 
        func: Callable, 
        inputs: List[Tuple[int, Any]],
        required_packages: Optional[List[str]] = None
    ) -> Dict:
        """Start a job on this worker"""
        try:
            pickled_func = cloudpickle.dumps(func)
            encoded_func = base64.b64encode(pickled_func).decode('utf-8')
            
            # Auto-detect packages if not provided
            if required_packages is None:
                required_packages = list(extract_imports_from_function(func))
            
            payload = {
                "job_id": job_id,
                "function": encoded_func,
                "inputs": inputs,
                "required_packages": required_packages
            }
            
            response = self.session.post(
                f"{self.base_url}/start_job",
                json=payload,
                timeout=120  # Increased timeout for package installation
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to start job on worker {self.base_url}: {str(e)}")
    
    def get_results(self, job_id: str) -> Dict:
        """Get results from this worker"""
        try:
            response = self.session.get(
                f"{self.base_url}/results/{job_id}",
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to get results from worker {self.base_url}: {str(e)}")
    
    def cancel_job(self, job_id: str) -> Dict:
        """Cancel a job on this worker"""
        try:
            response = self.session.delete(
                f"{self.base_url}/cancel_job/{job_id}",
                timeout=5
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Warning: Failed to cancel job on {self.base_url}: {str(e)}")
            return {"status": "error"}
    
    def get_status(self) -> Dict:
        """Get worker status"""
        try:
            response = self.session.get(f"{self.base_url}/status", timeout=5)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"is_busy": True, "error": str(e)}
    
    def health_check(self) -> bool:
        """Check if worker is healthy"""
        try:
            response = self.session.get(f"{self.base_url}/health", timeout=5)
            response.raise_for_status()
            return response.json().get("status") == "healthy"
        except:
            return False
    
    def install_package(self, package_name: str) -> bool:
        """Manually install a package on this worker"""
        try:
            response = self.session.post(
                f"{self.base_url}/install_package",
                params={"package_name": package_name},
                timeout=120
            )
            response.raise_for_status()
            return True
        except:
            return False
    
    def get_installed_packages(self) -> List[str]:
        """Get list of installed packages on this worker"""
        try:
            response = self.session.get(f"{self.base_url}/installed_packages", timeout=5)
            response.raise_for_status()
            return response.json().get("packages", [])
        except:
            return []

class Client:
    def __init__(self, worker_urls: List[str], max_concurrent_jobs: int = 10):
        self.worker_urls = [url.rstrip('/') for url in worker_urls]
        self.max_concurrent_jobs = max_concurrent_jobs
        self.worker_clients = []
        self.active_jobs: Dict[str, WorkerResult] = {}
        
        self._initialize_workers()
    
    def _initialize_workers(self):
        """Initialize and health check all workers"""
        print(f"Initializing {len(self.worker_urls)} workers...")
        healthy_workers = []
        
        with ThreadPoolExecutor(max_workers=len(self.worker_urls)) as executor:
            futures = {
                executor.submit(self._test_worker, url): url 
                for url in self.worker_urls
            }
            
            for future in as_completed(futures):
                url = futures[future]
                try:
                    is_healthy = future.result(timeout=10)
                    if is_healthy:
                        worker_client = WorkerClient(url)
                        healthy_workers.append(worker_client)
                        print(f"✓ Worker {url} is healthy")
                    else:
                        print(f"✗ Worker {url} is unhealthy")
                except Exception as e:
                    print(f"✗ Worker {url} failed health check: {str(e)}")
        
        self.worker_clients = healthy_workers
        if not self.worker_clients:
            raise RuntimeError("No healthy workers available!")
        
        print(f"Successfully initialized {len(self.worker_clients)} healthy workers")
    
    def _test_worker(self, url: str) -> bool:
        """Test if a worker is healthy"""
        try:
            client = WorkerClient(url)
            return client.health_check()
        except:
            return False
    
    def _get_available_workers(self, required_capacity: int = 1) -> List[WorkerClient]:
        """Get available workers that can handle the job"""
        available = []
        with ThreadPoolExecutor(max_workers=len(self.worker_clients)) as executor:
            futures = {
                executor.submit(self._check_worker_capacity, worker): worker 
                for worker in self.worker_clients
            }
            
            for future in as_completed(futures):
                worker = futures[future]
                try:
                    capacity = future.result(timeout=3)
                    if capacity >= required_capacity:
                        available.append(worker)
                except:
                    continue
        
        return available
    
    def _check_worker_capacity(self, worker: WorkerClient) -> int:
        """Check how many concurrent jobs a worker can handle"""
        status = worker.get_status()
        if status.get("is_busy", False):
            return 0
        return 1
    
    def _distribute_inputs(self, inputs: List[Any], n_workers: int) -> List[List[Tuple[int, Any]]]:
        """Distribute inputs across workers with global indexing"""
        if n_workers == 0:
            return []
        
        inputs_per_worker = [[] for _ in range(n_workers)]
        
        for i, inp in enumerate(inputs):
            worker_idx = i % n_workers
            inputs_per_worker[worker_idx].append((i, inp))
        
        return [worker_inputs for worker_inputs in inputs_per_worker if worker_inputs]
    
    def map(
        self, 
        func: Callable, 
        inputs: List[Any], 
        max_workers: Optional[int] = None,
        timeout_per_input: int = 60,
        required_packages: Optional[List[str]] = None
    ) -> List[Any]:
        """
        Execute function across inputs in parallel across workers
        
        Args:
            func: Function to execute
            inputs: List of inputs to process
            max_workers: Maximum number of workers to use
            timeout_per_input: Timeout per input in seconds
            required_packages: List of required package names (auto-detected if None)
            
        Returns:
            List of results in input order (None for failed inputs)
        """
        if not inputs:
            return []
        
        if len(self.worker_clients) == 0:
            raise RuntimeError("No workers available")
        
        # Auto-detect required packages if not provided
        if required_packages is None:
            required_packages = list(extract_imports_from_function(func))
            if required_packages:
                print(f"Auto-detected required packages: {required_packages}")
        
        n_available = len(self.worker_clients)
        n_workers = min(max_workers or n_available, n_available)
        
        if n_workers == 0:
            raise RuntimeError("No workers available to process job")
        
        print(f"Distributing {len(inputs)} inputs across {n_workers} workers...")
        
        job_id_base = f"job_{int(time.time())}_{random.randint(1000, 9999)}"
        job_configs = []
        
        worker_inputs = self._distribute_inputs(inputs, n_workers)
        
        for i, (worker, worker_input_list) in enumerate(zip(self.worker_clients[:n_workers], worker_inputs)):
            if worker_input_list:
                job_id = f"{job_id_base}_worker_{i}"
                job_configs.append((worker, job_id, worker_input_list))
        
        if not job_configs:
            raise RuntimeError("No inputs to process")
        
        print(f"Starting {len(job_configs)} worker jobs...")
        
        futures = {}
        with ThreadPoolExecutor(max_workers=len(job_configs)) as executor:
            for worker, job_id, worker_inputs in job_configs:
                future = executor.submit(
                    self._run_worker_job, 
                    worker, 
                    job_id, 
                    func, 
                    worker_inputs,
                    timeout_per_input,
                    required_packages
                )
                futures[future] = (worker, job_id)
        
        results = [None] * len(inputs)
        completed_inputs = 0
        errors = []
        start_time = time.time()
        
        print("Processing inputs...")
        
        while futures and completed_inputs < len(inputs):
            for future in list(futures):
                if future.done():
                    try:
                        worker_result = future.result()
                        if worker_result:
                            worker, job_id = futures.pop(future)
                            
                            for global_idx, *result in worker_result.results:
                                if len(result) == 1:
                                    results[global_idx] = result[0]
                                    completed_inputs += 1
                                else:
                                    error_msg = result[1] if len(result) > 1 else "Unknown error"
                                    errors.append(f"Input {global_idx}: {error_msg}")
                                    results[global_idx] = None
                                    completed_inputs += 1
                            
                            print(f"Worker {worker.base_url}: {len(worker_result.results)} results")
                        
                    except Exception as e:
                        worker, job_id = futures.pop(future)
                        print(f"Worker {worker.base_url} failed: {str(e)}")
                        try:
                            worker.cancel_job(job_id)
                        except:
                            pass
            
            progress = (completed_inputs / len(inputs)) * 100
            elapsed = time.time() - start_time
            rate = completed_inputs / elapsed if elapsed > 0 else 0
            print(f"\rProgress: {completed_inputs}/{len(inputs)} ({progress:.1f}%) "
                  f"[{rate:.1f} inputs/s]", end="")
            
            if time.time() - start_time > timeout_per_input * len(inputs) * 1.5:
                print("\nJob timeout - cancelling remaining jobs")
                for future in list(futures):
                    worker, job_id = futures[future]
                    try:
                        worker.cancel_job(job_id)
                    except:
                        pass
                break
            
            time.sleep(0.1)
        
        print(f"\nCompleted: {completed_inputs}/{len(inputs)}")
        
        if errors:
            print(f"\n{len(errors)} inputs failed:")
            for error in errors[:10]:
                print(f"  - {error}")
            if len(errors) > 10:
                print(f"  ... and {len(errors) - 10} more")
        
        if completed_inputs < len(inputs):
            missing = len(inputs) - completed_inputs
            print(f"\nWarning: {missing} inputs did not complete")
        
        return results
    
    def _run_worker_job(
        self,
        worker: WorkerClient,
        job_id: str,
        func: Callable,
        worker_inputs: List[Tuple[int, Any]],
        timeout_per_input: int,
        required_packages: Optional[List[str]] = None
    ) -> Optional[WorkerResult]:
        """Run a job on a single worker and collect results"""
        try:
            start_response = worker.start_job(job_id, func, worker_inputs, required_packages)
            total_inputs = start_response["total_inputs"]
            
            # Show installed packages if any
            if start_response.get("packages_installed"):
                print(f"Worker {worker.base_url} installed: {start_response['packages_installed']}")
            
            results = []
            start_time = time.time()
            
            while len(results) < total_inputs:
                if time.time() - start_time > timeout_per_input * 2:
                    print(f"Timeout waiting for worker {worker.base_url}")
                    worker.cancel_job(job_id)
                    return None
                
                try:
                    worker_results = worker.get_results(job_id)
                    
                    for result in worker_results["results"]:
                        if result not in results:
                            results.append(result)
                    
                    if worker_results["status"] == "completed":
                        break
                    
                    time.sleep(0.1)
                    
                except Exception as e:
                    print(f"Error getting results from {worker.base_url}: {str(e)}")
                    time.sleep(0.5)
            
            return WorkerResult(
                worker_url=worker.base_url,
                job_id=job_id,
                total_inputs=total_inputs,
                results=results
            )
            
        except Exception as e:
            print(f"Failed to run job on {worker.base_url}: {str(e)}")
            try:
                worker.cancel_job(job_id)
            except:
                pass
            return None
    
    def install_package_on_all_workers(self, package_name: str) -> Dict[str, bool]:
        """
        Manually install a package on all workers
        Returns dict mapping worker URLs to success status
        """
        print(f"Installing {package_name} on all workers...")
        results = {}
        
        with ThreadPoolExecutor(max_workers=len(self.worker_clients)) as executor:
            futures = {
                executor.submit(worker.install_package, package_name): worker 
                for worker in self.worker_clients
            }
            
            for future in as_completed(futures):
                worker = futures[future]
                try:
                    success = future.result(timeout=120)
                    results[worker.base_url] = success
                    if success:
                        print(f"✓ Installed on {worker.base_url}")
                    else:
                        print(f"✗ Failed on {worker.base_url}")
                except Exception as e:
                    results[worker.base_url] = False
                    print(f"✗ Error on {worker.base_url}: {str(e)}")
        
        return results
    
    def get_cluster_status(self) -> Dict:
        """Get status of all workers in cluster"""
        status = {
            "total_workers": len(self.worker_urls),
            "healthy_workers": len(self.worker_clients),
            "active_jobs": len(self.active_jobs)
        }
        
        worker_statuses = []
        with ThreadPoolExecutor(max_workers=len(self.worker_clients)) as executor:
            futures = [executor.submit(w.get_status) for w in self.worker_clients]
            for future in as_completed(futures):
                try:
                    worker_statuses.append(future.result(timeout=5))
                except:
                    worker_statuses.append({"error": "unreachable"})
        
        status["worker_details"] = worker_statuses
        return status
    
    def close(self):
        """Clean up resources"""
        for worker in self.worker_clients:
            worker.session.close()
