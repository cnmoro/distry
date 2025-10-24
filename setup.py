from setuptools import setup, find_packages
import os

# Read requirements from files
def read_requirements(filename):
    requirements = []
    try:
        with open(os.path.join('distry', filename)) as f:
            requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    except FileNotFoundError:
        pass
    return requirements

# Base requirements
install_requires = []

# Extra requirements
extras_require = {
    'client': read_requirements('client_requirements.txt'),
    'worker': read_requirements('worker_requirements.txt'),
    'dev': [
        'pytest',
        'pytest-asyncio',
        'requests',
        'black',
        'flake8',
        'mypy'
    ],
    'all': read_requirements('client_requirements.txt') + read_requirements('worker_requirements.txt')
}

setup(
    name='distry',
    version='0.1.0',
    author='Your Name',
    author_email='your.email@example.com',
    description='Distributed task execution framework',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/yourusername/distry',
    packages=find_packages(),
    python_requires='>=3.8',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
    install_requires=install_requires,
    extras_require=extras_require,
    license='MIT',
    keywords='distributed computing parallel processing',
    platforms=['any'],
)
