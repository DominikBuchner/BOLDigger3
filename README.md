# BOLDigger3
 
![BOLDigger3](https://github.com/user-attachments/assets/99606be9-8ae1-4e90-a5d5-4a6a3311f48c)


[![Downloads](https://pepy.tech/badge/boldigger3)](https://pepy.tech/project/boldigger3)

A Python program to query .fasta files against the databases of www.boldsystems.org v5!

## Introduction
DNA metabarcoding datasets often comprise hundreds of Operational Taxonomic Units (OTUs), requiring querying against databases for taxonomic assignment. The Barcode of Life Data system (BOLD) is a widely used database for this purpose among biologists. However, BOLD's online platform limits users to identifying batches of only 1000, 200 or 100 (depending on operating mode) sequences at a time.

BOLDigger3, the successor to BOLDigger2 and BOLDigger, aims to overcome these limitations. As a pure Python program, BOLDigger3 offers:

- Automated access to BOLD's identification engine
- Downloading of BOLD's latest data package release to access all metadata
- Selection of the best-fitting hit from the returned results

## Overview

**BOLDigger3** is an automated tool designed for DNA sequence identification through **BOLDSystems v5**, supporting integration into bioinformatics pipelines with enhanced functionality and performance. With BOLDigger3, users can identify up to **10,000 sequences per hour** using an optimized data storage and queuing system that improves speed and process safety.

## Key Differences Between BOLDigger3 and BOLDigger2

- **Unified Function**: BOLDigger3 consolidates all actions into a single function, `identify`, which automatically performs identification, additional data downloading, and top-hit selection, making it easier to integrate into pipelines.
- **Enhanced Database Accessibility**: Users have access to all databases offered by **BOLDSystems v5** and can select from three different operating modes.
- **Improved Speeds**: Depending on the operating mode, BOLDigger3 can identify up to **10,000 sequences per hour**, significantly faster than BOLDigger2.
- **BOLD Credentials Required for Database Download**: Users need a BOLD account to download the public database package. Sequence identifications via the ID engine do not require credentials.
- **Streamlined Data Storage**: Data is stored in a **DuckDB database** for faster processing, with final outputs available in `.xlsx` and `.parquet` formats.
- **Process Safety**: BOLDigger3 can resume interrupted executions, continuing exactly where it left off.
- **Dynamic Queuing**: The tool automatically manages request queuing based on the selected operating mode.

## Features

- **Identify Sequences Automatically**: Run DNA sequence identifications with a single command.
- **Flexible Database Options**: Access to all BOLDSystems v5 databases with user-selected operating modes.
- **High-Performance Processing**: Up to 10,000 identifications per hour, depending on settings.
- **Robust Storage**: Data stored in DuckDB format for efficient processing; results in `.xlsx` and `.parquet`.
- **User-Friendly**: Credentials are only required once to download the public database.

## Installation and Usage

BOLDigger3 requires Python version 3.11 or higher and can be easily installed using pip in any command line:

`pip install boldigger3`

This command will install BOLDigger3 along with all its dependencies.

### Step 1: Download the public database

Before running identifications, download the latest BOLD public database package. This step requires a valid BOLD account (register at www.boldsystems.org):

`boldigger3 download_db PATH_TO_OUTPUT_DIR`

BOLDigger3 will prompt for your BOLD username and password, check whether a local database already exists, and download and convert the latest release to a DuckDB file (`.ddb`) if needed.

### Step 2: Run the identification

To run the ```identify``` function, use the following command:

`boldigger3 identify PATH_TO_FASTA PATH_TO_DATABASE --db DATABASE_NR --mode OPERATING_MODE`

`PATH_TO_DATABASE` is the path to the `.ddb` file downloaded in Step 1.

# Databases

The ```--db``` is a number between 1 and 8 corresponding to the eight databases BOLD v5 currently offers:

1: **ANIMAL LIBRARY (PUBLIC)**   
2: **ANIMAL SPECIES-LEVEL LIBRARY (PUBLIC + PRIVATE)**   
3: **ANIMAL LIBRARY (PUBLIC+PRIVATE)**    
4: **VALIDATED CANADIAN ARTHROPOD LIBRARY**   
5: **PLANT LIBRARY (PUBLIC)**   
6: **FUNGI LIBRARY (PUBLIC)**   
7: **ANIMAL SECONDARY MARKERS (PUBLIC)**   
8: **VALIDATED ANIMAL RED LIST LIBRARY**

# Operating modes

The ```--mode``` is a number between 1 and 3, corresponding to the three operating modes BOLD v5 currently offers:

1: **Rapid Species Search**   
2: **Genus and Species Search**   
3: **Exhaustive Search**   

To customize the implemented thresholds for user-specific needs, the thresholds can be passed as an additional (ordered) argument. Up to five different thresholds can be passed for the different taxonomic levels (Species, Genus, Family, Order, Class). Thresholds not passed will be replaced by default, but BOLDigger3 will also inform you about this:

`boldigger3 identify PATH_TO_FASTA PATH_TO_DATABASE --db DATABASE_NR --mode OPERATING_MODE --thresholds 99 97`

Output:

```
19:16:16: Default thresholds changed!
19:16:16: Species: 99, Genus: 97, Family: 90, Order: 85
```

When a new version is released, you can update BOLDigger3 by typing:

`pip install --upgrade boldigger3`

## How to cite

Buchner D, Leese F (2020) BOLDigger – a Python package to identify and organise sequences with the Barcode of Life Data systems. Metabarcoding and Metagenomics 4: e53535. https://doi.org/10.3897/mbmg.4.53535


## The BOLDigger3 Algorithm

### Database download (`download_db`)

1. **Login**: BOLDigger3 prompts for BOLD credentials and establishes an authenticated session.

2. **Check database status**: BOLDigger3 checks whether a local database file already matches the latest BOLD data package release.

3. **Download and compile**: If the local database is missing or outdated, BOLDigger3 downloads the latest Parquet release from BOLD and converts it into a DuckDB file (`.ddb`) for fast lookups.

### Identification (`identify`)

1. **Split the FASTA**: The input FASTA file is divided into chunks that fit the limits of the selected operating mode of the identification engine.

2. **Queue the Chunks**: These chunks are then queued in the identification engine for processing.

3. **Check for Results**: The algorithm periodically checks if any results can be downloaded.

4. **Data Download**: Once results are available, the data is downloaded.

5. **Data Validation**: The algorithm ensures that all data has been correctly downloaded.

6. **Retrieve Additional Data**: Additional metadata (collection site, coordinates, collector, etc.) is joined from the local DuckDB database.

7. **Select Top Hit**: Finally, the algorithm selects the top hit backed by the most database entries for the final output.

### Top hit selection

Different thresholds for the taxonomic levels are used to find the best fitting hit. The default thresholds are: 97%: species, 95%: genus, 90%: family, 85%: order, 75%: class, and 50%: phylum. After determining the threshold for all hits, the most common hit above the threshold will be selected. Note that for all hits below the threshold, the taxonomic resolution will be adjusted accordingly (e.g. for a 96% hit the species-level information will be discarded, and genus-level information will be used as the lowest taxonomic level).

1. **Identify Maximum Similarity**: Find the maximum similarity value among the top 100 hits currently under consideration.
   
2. **Set Threshold**: Set the threshold to this maximum similarity level. Remove all hits with a similarity below this threshold. For example, if the highest hit has a similarity of 100%, the threshold will be set to 97%, and all hits below this threshold will be removed temporarily.

3. **Classification and Sorting**: Count all individual classifications and sort them by abundance.

4. **Filter Missing Data**: Drop all classifications that contain missing data. For instance, if the most common hit is "Arthropoda --> Insecta" with a similarity of 100% but missing values for Order, Family, Genus, and Species.

5. **Identify Common Hit**: Look for the most common hit that has no missing values.

6. **Return Hit**: If a hit with no missing values is found, return that hit.

7. **Threshold Adjustment**: If no hit with no missing values is found, increase the threshold to the next higher level and repeat the process until a hit is found.


### BOLDigger3 Flagging System

BOLDigger3 employs a flagging system to highlight certain conditions, indicating a degree of uncertainty in the selected hit. Currently, there are five flags implemented, which may be updated as needed:

1. **Reverse BIN Taxonomy**: This flag is raised if all of the top 100 hits representing the selected match utilize reverse BIN taxonomy. Reverse BIN taxonomy assigns species names to deposited sequences on BOLD that lack species information, potentially introducing uncertainty.

2. **Differing Taxonomic Information**: If the percentage of hits represented by the selected top hit is smaller than 90%, flag 2 will be raised indicating a potential taxonomic conflict. If your top hit is represented by 99 hits and there is 1 hit with differing taxonomy, this flag will not be raised.

3. **Private Data**: If all of the top 100 hits representing the top hit are private hits, this flag is raised, indicating limited accessibility to data.

4. **Unique Hit**: This flag indicates that the top hit result represents a unique hit among the top 100 hits, potentially requiring further scrutiny.

5. **Multiple BINs**: If the selected species-level hit is composed of more than one BIN, this flag is raised, suggesting potential complexities in taxonomic assignment.

Given the presence of these flags, it is advisable to conduct a closer examination of all flagged hits to better understand and address any uncertainties in the selected hit.
