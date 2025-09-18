from setuptools import setup, find_packages

setup(
    name="pyspark_interview_project",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pyspark==3.4.2",
        "delta-spark==2.4.0",
        "pyyaml>=6.0.0",
        "requests>=2.32.0",
        "avro-python3>=1.10.0",
        "azure-identity>=1.15.0",
        "azure-storage-blob>=12.19.0",
        "azure-keyvault-secrets>=4.7.0",
        "azure-mgmt-storage>=21.0.0",
        "azure-mgmt-keyvault>=10.2.0",
        "azure-mgmt-monitor>=5.0.0",
        "azure-mgmt-resource>=23.0.0",
        "azure-mgmt-network>=25.1.0",
        "azure-mgmt-compute>=30.3.0",
        "azure-mgmt-sql>=3.0.0",
        "azure-mgmt-synapse>=2.0.0",
        "azure-mgmt-databricks>=1.0.0",
        "azure-mgmt-datafactory>=2.0.0",
        "azure-mgmt-eventhub>=10.0.0",
        "azure-mgmt-msi>=7.0.0",
        "azure-mgmt-authorization>=3.0.0",
        "kafka-python>=2.0.2",
    ],
    extras_require={
        "dev": [
            "pytest>=8.2.0",
            "pytest-spark>=0.6.0",
            "pytest-cov>=4.1.0",
            "flake8>=7.1.0",
            "black>=24.4.0",
            "isort>=5.13.0",
            "mypy>=1.1.0",
            "bandit>=1.7.0",
        ],
        "airflow": [
            "apache-airflow>=2.8.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "pde-pipeline=pyspark_interview_project.__main__:main"
        ]
    },
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
