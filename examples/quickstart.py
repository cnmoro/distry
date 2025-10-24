"""
Distry Quickstart Example
"""

from distry import Client

# Define workers (run `distry-worker` on each)
worker_urls = [
    "http://127.0.0.1:8001",
    "http://127.0.0.1:8002",
]

# Initialize client
client = Client(worker_urls)

# Simple function
def square(x):
    return x * x

# Process inputs in parallel
inputs = list(range(10))
results = client.map(square, inputs, max_workers=2)

print(f"Results: {results}")
# Output: [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

client.close()
