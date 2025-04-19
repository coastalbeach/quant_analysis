from setuptools import setup, find_packages

setup(
    name="quant_analysis",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "akshare",
        "pandas",
        "tqdm"
    ],
    python_requires=">=3.6"
)