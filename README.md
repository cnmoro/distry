<img src="assets/logo.svg" alt="Logo" width="220" />

## Distry

Distributed task execution framework. Scale your Python functions across multiple workers.

## Features

* **Zero-config setup** - Auto-detects and installs dependencies
* **Simple API** - Just `client.map(func, inputs)`
* **Fault-tolerant** - Handles worker failures gracefully
* **Automatic Job Batching** - Large jobs are automatically split to fit worker RAM limits.
* **Package management** - Installs required packages on workers
* **Global indexing** - Results returned in input order

## Installation

```plaintext
# Client only (for task submission)
pip install distry-py[client]

# Worker only (for task execution)
pip install distry-py[worker]

# Full installation
pip install distry-py[all]
```

## Quick Start

### 1. Start Workers

```plaintext
# Terminal 1 - Worker 1
distry-worker --port 8001

# Terminal 2 - Worker 2 (with RAM limit)
distry-worker --port 8002 --max-ram 2g
```

### 2. Run Tasks

```python
from distry import Client

# Connect to workers
client = Client(["http://127.0.0.1:8001", "http://127.0.0.1:8002"])

# Define function (any Python function works!)
import numpy as np

def process_data(x):
    return np.mean([x, x**2, x**3])

# Process inputs in parallel
results = client.map(process_data, [1, 2, 3, 4, 5])

print(results)
# [1.0, 6.0, 19.0, 40.0, 69.0]

client.close()
```

### 2b. Using the Decorator (for single function calls)

For simpler cases where you want to execute a single function call on a worker, you can use the `@distry` decorator.

```python
from distry import register_workers, distry
import numpy as np

# Connect to workers
register_workers(["http://127.0.0.1:8001", "http://127.0.0.1:8002"])

@distry
def process_data(x, power=2):
    return np.mean([x, x**power])

# Process a single input on a randomly selected worker
result = process_data(10)
print(result)
# 55.0

# With keyword arguments
result_power_3 = process_data(10, power=3)
print(result_power_3)
# 505.0
```

### 3. Advanced Usage

```python
from distry import Client

client = Client(worker_urls)

# Custom packages (optional - auto-detection works too)
def scipy_func(x):
    from scipy.special import factorial
    return float(factorial(x))

results = client.map(
    scipy_func,
    [1, 2, 3, 4],
    required_packages=['scipy'],  # Manual specification
    max_workers=2
)

# Results with error handling
def risky_func(x):
    if x == 3:
        raise ValueError("Oops!")
    return x * 2

results = client.map(risky_func, [1, 2, 3, 4])  # [2, 4, None, 8]

client.close()
```

## API Reference

### Client

```python
from distry import Client

client = Client(worker_urls, max_concurrent_jobs=10)

# Map function across inputs
results = client.map(
    func,           # Any Python function
    inputs,         # List of inputs
    max_workers=4,  # Limit concurrent workers
    timeout=60,     # Timeout per input
    required_packages=None  # Auto-detected
)

# Cluster status
status = client.get_cluster_status()

client.close()
```

### Worker

```python
from distry import WorkerServer

# Programmatic worker
server = WorkerServer(host="0.0.0.0", port=8000)
server.run()

# Or use CLI
# distry-worker --host 0.0.0.0 --port 8000
```

## CLI

```plaintext
# Start worker
distry-worker --help
distry-worker --host 0.0.0.0 --port 8000 --max-ram 4g

# The client will automatically split large jobs into batches
# to fit the worker's RAM limit.

# View worker endpoints
# GET /health
# GET /status
# GET /installed_packages
```

## What Happens Under the Hood?

1. **Function Analysis**: Auto-detects imports from your function
2. **Package Installation**: Installs missing packages on workers
3. **Task Distribution**: Splits inputs across available workers
4. **Result Collection**: Gathers results with global indexing
5. **Error Handling**: Failed inputs return `None`, others succeed

## Use Cases

* **Data Processing**: Apply functions to large datasets
* **ML Inference**: Scale model predictions across workers
* **API Calls**: Parallelize HTTP requests
* **Computational Tasks**: CPU-intensive calculations
* **Batch Processing**: Process files, images, or documents

## Limitations

* Single function per job (no complex workflows)
* 30s timeout per input (configurable)
* Synchronous function execution on workers
* Basic package management (no virtual environments)