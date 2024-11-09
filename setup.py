import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="boldigger3",
    version="1.2.0",
    author="Dominik Buchner",
    author_email="dominik.buchner@uni-due.de",
    description="A python package to query different databases of boldsystems.org v5!",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/DominikBuchner/BOLDigger3",
    packages=setuptools.find_packages(),
    license="MIT",
    install_requires=[
        "beautifulsoup4>=4.12.3",
        "Bio>=1.7.1",
        "biopython>=1.84",
        "get_pypi_latest_version>=0.0.7",
        "more_itertools>=10.5.0",
        "numpy>=1.24.0, <2.0.0",
        "pandas>=2.2.3",
        "playwright>=1.48.0",
        "Requests>=2.32.3",
        "requests_html_playwright>=0.12.3",
        "setuptools>=65.5.0",
        "tqdm>=4.66.4",
        "urllib3>=1.26.14",
        "tables>=3.9.2",
        "html5lib>=1.1",
        "soupsieve>=2.5",
        "openpyxl>=3.1.1",
        "pyarrow>=11.0.0",
        "lxml_html_clean>=0.1.1",
        "xlsxwriter >= 3.0.5",
    ],
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
    entry_points={
        "console_scripts": [
            "boldigger3 = boldigger3.__main__:main",
        ]
    },
)
