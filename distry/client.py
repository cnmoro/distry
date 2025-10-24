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
    """Extract required packages from function source and global scope."""
    import inspect
    import ast
    import sys
    import textwrap
    
    imports = set()

    # 1. Parse function source for inline imports
    try:
        source = inspect.getsource(func)
        # Dedent the source code to handle nested functions
        source = textwrap.dedent(source)
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
    except (TypeError, OSError):
        # Could fail for dynamically generated functions, etc.
        pass

    # 2. Inspect closure and global namespace for modules used by the function
    if hasattr(func, '__code__'):
        try:
            closure_vars = inspect.getclosurevars(func)

            # Check non-local variables from closure
            for module in closure_vars.nonlocals.values():
                if inspect.ismodule(module):
                    package_name = module.__name__.split('.')[0]
                    imports.add(package_name)

            # Check global variables used by the function
            for name in func.__code__.co_names:
                if name in closure_vars.globals and inspect.ismodule(closure_vars.globals[name]):
                    module = closure_vars.globals[name]
                    package_name = module.__name__.split('.')[0]
                    imports.add(package_name)
        except Exception:
            pass

    # Filter out built-in modules
    builtins = set(sys.builtin_module_names)
    builtins.update({'sys', 'os', 'time', 'math', 'random', 'json'})
    
    return list(imports - builtins)

class WorkerClient:
    """Client for individual worker communication."""
    
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})
        self.max_ram_gb = None
    
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
    
    def get_status(self) -> Dict:
        """Get worker status."""
        try:
            response = self.session.get(f"{self.base_url}/status", timeout=5)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise WorkerCommunicationError(f"Failed to get status: {e}")

    def health_check(self) -> bool:
        """Check worker health."""
        try:
            status = self.get_status()
            self.max_ram_gb = status.get("settings", {}).get("max_ram_gb")
            return True
        except WorkerCommunicationError:
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
                    worker_client = future.result(timeout=10)
                    if worker_client:
                        healthy_workers.append(worker_client)
                        print(f"✓ Worker {url} ready")
                except Exception as e:
                    print(f"✗ Worker {url} failed: {e}")
        
        self.worker_clients = healthy_workers
        if not self.worker_clients:
            raise WorkerUnavailableError("No healthy workers available")
    
    def _test_worker(self, url: str) -> Optional[WorkerClient]:
        """Test single worker health."""
        client = WorkerClient(url)
        if client.health_check():
            return client
        return None
    
    def _estimate_job_size(self, func: Callable, inputs: List[Any]) -> float:
        """Estimate job size in GB."""
        try:
            pickled_func = cloudpickle.dumps(func)
            pickled_inputs = cloudpickle.dumps(inputs)
            total_size_bytes = len(pickled_func) + len(pickled_inputs)
            return total_size_bytes / (1024**3)
        except Exception:
            return 0.0

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
                    self._run_worker_job_batched,
                    worker, job_id_base, func, worker_inputs,
                    timeout_per_input, required_packages
                ): (worker, job_id_base)
                for i, (worker, job_id, worker_inputs) in enumerate(job_configs)
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
    
    def run(
        self,
        func: Callable,
        args: Tuple = (),
        kwargs: Dict = {},
        timeout: int = 60,
        required_packages: Optional[List[str]] = None
    ) -> Any:
        """Execute a single function on a worker."""
        if not self.worker_clients:
            raise WorkerUnavailableError("No workers available")

        # Create a wrapper function to handle args and kwargs
        def func_wrapper(data):
            return func(*data['args'], **data['kwargs'])

        inputs = [{'args': args, 'kwargs': kwargs}]

        # Auto-detect packages
        if required_packages is None:
            required_packages = extract_imports_from_function(func)

        worker = random.choice(self.worker_clients)
        job_id = f"job_{int(time.time())}_{random.randint(1000, 9999)}"

        # The input format for _run_worker_job is List[Tuple[int, Any]]
        worker_inputs = list(enumerate(inputs))

        result = self._run_worker_job(
            worker, job_id, func_wrapper, worker_inputs,
            timeout, required_packages
        )

        if result and result['results']:
            # Result is [(0, <return_value>)]
            return result['results'][0][1]

        raise JobFailedError("Job did not return a result")

    def _run_worker_job_batched(
        self,
        worker: WorkerClient,
        job_id_base: str,
        func: Callable,
        worker_inputs: List[Tuple[int, Any]],
        timeout_per_input: int,
        required_packages: Optional[List[str]] = None
    ) -> Optional[Dict]:
        """Execute job on worker, splitting into batches if needed."""
        if worker.max_ram_gb is None:
            return self._run_worker_job(
                worker, job_id_base, func, worker_inputs,
                timeout_per_input, required_packages
            )

        job_size_gb = self._estimate_job_size(func, [i[1] for i in worker_inputs])

        if job_size_gb <= worker.max_ram_gb:
            return self._run_worker_job(
                worker, job_id_base, func, worker_inputs,
                timeout_per_input, required_packages
            )

        # Split job into batches
        import math
        num_batches = math.ceil(job_size_gb / worker.max_ram_gb)
        batch_size = math.ceil(len(worker_inputs) / num_batches)

        print(
            f"Job for {worker.base_url} is too large ({job_size_gb:.2f} GB > "
            f"{worker.max_ram_gb:.2f} GB). Splitting into {num_batches} batches."
        )

        all_results = []
        for i in range(num_batches):
            batch_inputs = worker_inputs[i * batch_size : (i + 1) * batch_size]
            if not batch_inputs:
                continue

            batch_job_id = f"{job_id_base}_b{i}"
            batch_result = self._run_worker_job(
                worker, batch_job_id, func, batch_inputs,
                timeout_per_input, required_packages
            )

            if batch_result:
                all_results.extend(batch_result["results"])

        return {"results": all_results}

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
                if time.time() - start_time > timeout_per_input * total_inputs:
                    raise TimeoutError(f"Worker {worker.base_url} timeout")
                
                worker_results = worker.get_results(job_id)
                
                # Use a set for efficient checking of existing results
                result_ids = {r[0] for r in results}
                new_results = [r for r in worker_results["results"] if r[0] not in result_ids]
                results.extend(new_results)
                
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
