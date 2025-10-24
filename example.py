from client import Client
import time

# Configure workers
worker_urls = [
    "http://127.0.0.1:8001",
    "http://127.0.0.1:8002",  
    "http://127.0.0.1:8003",
    "http://127.0.0.1:8004",
]

try:
    # Initialize client
    client = Client(worker_urls, max_concurrent_jobs=4)
    
    # Test 1: Simple function (no extra packages)
    def simple_function(x):
        """Simple function that doubles the input"""
        time.sleep(0.1)
        return x * 2
    
    print("=== Test 1: Simple Function (No Extra Packages) ===")
    inputs = list(range(1, 11))
    results = client.map(simple_function, inputs, max_workers=4)
    print(f"Input:  {inputs}")
    print(f"Output: {results}")
    print()
    
    # Test 2: Function using numpy (auto-detected and installed)
    def numpy_function(x):
        """Function using numpy - will auto-install if needed"""
        import numpy as np
        arr = np.array([x, x**2, x**3])
        return float(np.mean(arr))
    
    print("=== Test 2: NumPy Function (Auto-Install) ===")
    inputs = list(range(1, 6))
    results = client.map(numpy_function, inputs, max_workers=2)
    print(f"Input:  {inputs}")
    print(f"Output: {results}")
    print()
    
    # Test 3: Function with manually specified packages
    def scipy_function(x):
        """Function using scipy with manual package specification"""
        from scipy import special
        return float(special.factorial(x))
    
    print("=== Test 3: SciPy Function (Manual Package List) ===")
    inputs = list(range(1, 8))
    # Manually specify packages instead of auto-detection
    results = client.map(
        scipy_function, 
        inputs, 
        max_workers=2,
        required_packages=['scipy']
    )
    print(f"Input:  {inputs}")
    print(f"Output: {results}")
    print()
    
    # Test 4: Pre-install a package on all workers
    print("=== Test 4: Pre-Install Package ===")
    install_results = client.install_package_on_all_workers('requests')
    print(f"Installation results: {install_results}")
    print()
    
    # Test 5: Function using the pre-installed package
    def web_function(url):
        """Function that makes HTTP requests"""
        import requests
        try:
            response = requests.get(url, timeout=5)
            return response.status_code
        except Exception as e:
            return None
    
    print("=== Test 5: Using Pre-Installed Package ===")
    urls = [
        "https://httpbin.org/status/200",
        "https://httpbin.org/status/404",
        "https://httpbin.org/delay/1"
    ]
    results = client.map(web_function, urls, max_workers=2)
    print(f"URLs: {urls}")
    print(f"Status codes: {results}")
    print()
    
    # Test 6: Complex function with multiple imports
    def complex_function(x):
        """Function using multiple packages"""
        import numpy as np
        import json
        
        data = {
            "input": x,
            "square": x**2,
            "sqrt": float(np.sqrt(x)),
            "log": float(np.log(x + 1))
        }
        return json.dumps(data)
    
    print("=== Test 6: Complex Function (Multiple Imports) ===")
    inputs = [1, 4, 9, 16, 25]
    results = client.map(complex_function, inputs, max_workers=3)
    print(f"Input: {inputs}")
    for result in results:
        print(f"  {result}")
    print()
    
    # Show cluster status with installed packages
    print("=== Cluster Status ===")
    status = client.get_cluster_status()
    print(f"Healthy workers: {status['healthy_workers']}/{status['total_workers']}")
    print("\nWorker details:")
    for i, worker_status in enumerate(status['worker_details']):
        print(f"  Worker {i+1}:")
        print(f"    - Busy: {worker_status.get('is_busy', 'unknown')}")
        print(f"    - Running jobs: {worker_status.get('running_jobs', [])}")
        if 'installed_packages' in worker_status:
            print(f"    - Installed packages: {worker_status['installed_packages']}")
    
    print("\n✓ All tests completed successfully!")
    
except KeyboardInterrupt:
    print("\nInterrupted by user")
except Exception as e:
    print(f"Error: {str(e)}")
    import traceback
    traceback.print_exc()
finally:
    if 'client' in locals():
        client.close()
