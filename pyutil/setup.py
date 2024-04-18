from setuptools import setup, find_packages

setup(
    name="pyutil",  # Name of your package
    version="0.1",  # Version number
    packages=find_packages(),  # List of all Python import packages that should be included in the Distribution Package
    description="A simple example package",  # Short description of your package
    install_requires=[  # List of dependencies required by your package
        "numpy",
        "pandas",
        "pymongo",
        "requests",
        "sqlalchemy"
    ],
    classifiers=[  # Classification and metadata information
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',  # Minimum version requirement of Python
)
