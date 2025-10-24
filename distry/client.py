"""
Distry Client - Coordinate distributed task execution
"""

import time
import random
import base64
from typing import Callable, List, Any, Optional, Dict, Tuple
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import cloudpickle
from .exceptions import DistryError, WorkerUnavailableError, JobFailedError, WorkerCommunicationError

def extract_imports_from_function(func: Callable) -> list[str]:
    """Extract required packages from function source."""
    import inspect
    import ast
    
    imports = set()
    try:
        source = inspect.getsource(func)
        tree = ast.parse(source)
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    package = alias.name.split('.')[0]
                    imports.add(package)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    package = node.module.split('.')[0]
                    imports.add(package)
    except Exception:
        pass
    
    # Filter common built-ins
    builtins = {'sys', 'os', 'time', 'math', 'random', 'json'}
    return list(imports - builtins)

class WorkerClient:
    """Client for individual worker communication."""
    
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
        """Start a job on this worker."""
        try:
            pickled_func = cloudpickle.dumps(func)
            encoded_func = base64.b64encode(pickled_func).decode('utf-8')
            
            if required_packages is None:
                required_packages = extract_imports_from_function(func)
            
            payload = {
                "job_id": job_id,
                "function": encoded_func,
                "inputs": inputs,
                "required_packages": required_packages
            }
            
            response = self.session.post(
                f"{self.base_url}/start_job",
                json=payload,
                timeout=120
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise WorkerCommunicationError(f"Failed to start job: {e}")
    
    def get_results(self, job_id: str) -> Dict:
        """Get job results."""
        try:
            response = self.session.get(
                f"{self.base_url}/results/{job_id}",
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise WorkerCommunicationError(f"Failed to get results: {e}")
    
    def health_check(self) -> bool:
        """Check worker health."""
        try:
            response = self.session.get(f"{self.base_url}/health", timeout=5)
            response.raise_for_status()
            return response.json().get("status") == "healthy"
        except:
            return False

class Client:
    """Main client for distributed task execution."""
    
    def __init__(self, worker_urls: List[str], max_concurrent_jobs: int = 10):
        self.worker_urls = [url.rstrip('/') for url in worker_urls]
        self.max_concurrent_jobs = max_concurrent_jobs
        self.worker_clients = []
        self._initialize_workers()
    
    def _initialize_workers(self):
        """Initialize and health check all workers."""
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
                    if future.result(timeout=10):
                        worker_client = WorkerClient(url)
                        healthy_workers.append(worker_client)
                        print(f"✓ Worker {url} ready")
                except Exception as e:
                    print(f"✗ Worker {url} failed: {e}")
        
        self.worker_clients = healthy_workers
        if not self.worker_clients:
            raise WorkerUnavailableError("No healthy workers available")
    
    def _test_worker(self, url: str) -> bool:
        """Test single worker health."""
        return WorkerClient(url).health_check()
    
    def _distribute_inputs(self, inputs: List[Any], n_workers: int) -> List[List[Tuple[int, Any]]]:
        """Distribute inputs across workers."""
        if n_workers == 0:
            return []
        
        inputs_per_worker = [[] for _ in range(n_workers)]
        for i, inp in enumerate(inputs):
            worker_idx = i % n_workers
            inputs_per_worker[worker_idx].append((i, inp))
        
        return [w_inputs for w_inputs in inputs_per_worker if w_inputs]
    
    def map(
        self, 
        func: Callable, 
        inputs: List[Any], 
        max_workers: Optional[int] = None,
        timeout_per_input: int = 60,
        required_packages: Optional[List[str]] = None
    ) -> List[Any]:
        """
        Execute function across inputs in parallel.
        
        Returns results in input order, with None for failed inputs.
        """
        if not self.worker_clients:
            raise WorkerUnavailableError("No workers available")
        
        if not inputs:
            return []
        
        # Auto-detect packages
        if required_packages is None:
            required_packages = extract_imports_from_function(func)
        
        n_available = len(self.worker_clients)
        n_workers = min(max_workers or n_available, n_available)
        
        print(f"Processing {len(inputs)} inputs across {n_workers} workers...")
        
        job_id_base = f"job_{int(time.time())}_{random.randint(1000, 9999)}"
        worker_inputs = self._distribute_inputs(inputs, n_workers)
        job_configs = []
        
        for i, (worker, worker_input_list) in enumerate(
            zip(self.worker_clients[:n_workers], worker_inputs)
        ):
            if worker_input_list:
                job_id = f"{job_id_base}_w{i}"
                job_configs.append((worker, job_id, worker_input_list))
        
        results = [None] * len(inputs)
        completed_count = 0
        
        with ThreadPoolExecutor(max_workers=len(job_configs)) as executor:
            futures = {
                executor.submit(
                    self._run_worker_job,
                    worker, job_id, func, worker_inputs,
                    timeout_per_input, required_packages
                ): (worker, job_id)
                for worker, job_id, worker_inputs in job_configs
            }
            
            for future in as_completed(futures):
                worker, job_id = futures[future]
                try:
                    worker_result = future.result()
                    if worker_result:
                        for global_idx, *result in worker_result['results']:
                            if len(result) == 1:
                                results[global_idx] = result[0]
                                completed_count += 1
                            else:
                                results[global_idx] = None
                                completed_count += 1
                except Exception as e:
                    print(f"Worker {worker.base_url} failed: {e}")
        
        print(f"Completed {completed_count}/{len(inputs)} inputs")
        return results
    
    def _run_worker_job(
        self,
        worker: WorkerClient,
        job_id: str,
        func: Callable,
        worker_inputs: List[Tuple[int, Any]],
        timeout_per_input: int,
        required_packages: Optional[List[str]] = None
    ) -> Optional[Dict]:
        """Execute job on single worker."""
        try:
            start_response = worker.start_job(
                job_id, func, worker_inputs, required_packages
            )
            
            results = []
            start_time = time.time()
            total_inputs = start_response["total_inputs"]
            
            while len(results) < total_inputs:
                if time.time() - start_time > timeout_per_input * 2:
                    raise TimeoutError(f"Worker {worker.base_url} timeout")
                
                worker_results = worker.get_results(job_id)
                
                for result in worker_results["results"]:
                    if result not in results:
                        results.append(result)
                
                if worker_results["status"] == "completed":
                    break
                
                time.sleep(0.1)
            
            return {"results": results}
            
        except Exception as e:
            print(f"Job failed on {worker.base_url}: {e}")
            return None
    
    def get_cluster_status(self) -> Dict:
        """Get cluster status."""
        return {
            "total_workers": len(self.worker_urls),
            "healthy_workers": len(self.worker_clients),
            "available_workers": len([w for w in self.worker_clients if w.health_check()])
        }
    
    def close(self):
        """Close all connections."""
        for worker in self.worker_clients:
            try:
                worker.session.close()
            except:
                pass
