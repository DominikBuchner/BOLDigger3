import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="boldigger3",
    version="2.0.2",
    author="Dominik Buchner",
    author_email="dominik.buchner@uni-due.de",
    description="A python package to query different databases of boldsystems.org v5!",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/DominikBuchner/BOLDigger3",
    packages=setuptools.find_packages(),
    license="MIT",
    install_requires=[
        "Bio>=1.8.0",
        "biopython>=1.85",
        "dask>=2025.3.0",
        "duckdb>=1.3.2",
        "ijson>=3.4.0",
        "luddite>=1.0.4",
        "more_itertools>=10.5.0",
        "numpy>=2.0.0",
        "pandas>=2.2.3",
        "python_dateutil>=2.9.0",
        "Requests>=2.32.3",
        "requests_html>=0.10.0",
        "setuptools>=65.5.0",
        "tqdm>=4.66.4",
        "urllib3>=1.26.14",
        "tables>=3.9.2",
        "openpyxl>=3.1.1",
        "pyarrow>=11.0.0",
        "xlsxwriter >= 3.0.5",
    ],
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.11",
    entry_points={
        "console_scripts": [
            "boldigger3 = boldigger3.__main__:main",
        ]
    },
)
