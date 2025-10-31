#!/usr/bin/env python3
"""
Setup configuration for pyspark_interview_project package.
"""

from setuptools import setup, find_packages

# Read requirements
with open("requirements.txt", "r") as f:
    requirements = [line.strip() for line in f if line.strip() and not line.startswith("#")]

# Read long description from README
try:
    with open("README.md", "r", encoding="utf-8") as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = "PySpark Data Engineering Project"

setup(
    name="pyspark-interview-project",
    version="1.0.0",
    author="Data Engineering Team",
    description="Enterprise-grade PySpark data engineering platform with Delta Lake, Airflow, and AWS",
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.10",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=requirements,
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)

