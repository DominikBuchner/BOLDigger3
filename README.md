# BOLDigger3
 
![boldigger_logo]()

[![Downloads](https://pepy.tech/badge/boldigger2)](https://pepy.tech/project/boldigger3)

A Python program to query .fasta files against the databases of www.boldsystems.org v5!

## Introduction
DNA metabarcoding datasets often comprise hundreds of Operational Taxonomic Units (OTUs), requiring querying against databases for taxonomic assignment. The Barcode of Life Data system (BOLD) is a widely used database for this purpose among biologists. However, BOLD's online platform limits users to identifying batches of only 1000, 200 or 100  (depending on operating mode) sequences at a time.

BOLDigger3, the successor to BOLDigger2 and BOLDigger, aims to overcome these limitations. As a pure Python program, BOLDigger3 offers:

- Automated access to BOLD's identification engine
- Downloading of additional metadata for each hit
- Selection of the best-fitting hit from the returned results

## Overview

**BOLDigger3** is an automated tool designed for DNA sequence identification through **BOLDSystems v5**, supporting integration into bioinformatics pipelines with enhanced functionality and performance. With BOLDigger3, users can identify up to **10,000 sequences per hour** without the need for credentials, using an optimized data storage and queuing system that improves speed and process safety.

## Key Differences Between BOLDigger3 and BOLDigger2

- **Unified Function**: BOLDigger3 consolidates all actions into a single function, `identify`, which automatically performs identification, additional data downloading, and top-hit selection, making it easier to integrate into pipelines.
- **Enhanced Database Accessibility**: Users have access to all databases offered by **BOLDSystems v5** and can select from three different operating modes.
- **Improved Speeds**: Depending on the operating mode, BOLDigger3 can identify up to **10,000 sequences per hour**, significantly faster than BOLDigger2.
- **No Password Required**: Users no longer need credentials to perform identifications—just select the FASTA file, database, and operating mode to start.
- **Streamlined Data Storage**: Data is stored in an **HDF Store** for faster processing, with final outputs available in `.xlsx` and `.parquet` formats.
- **Process Safety**: BOLDigger3 can resume interrupted executions, continuing exactly where it left off.
- **Dynamic Queuing**: The tool automatically manages request queuing based on the selected operating mode.

## Features

- **Identify Sequences Automatically**: Run DNA sequence identifications with a single command.
- **Flexible Database Options**: Access to all BOLDSystems v5 databases with user-selected operating modes.
- **High-Performance Processing**: Up to 10,000 identifications per hour, depending on settings.
- **Robust Storage**: Data stored in HDF format for efficient processing; results in `.xlsx` and `.parquet`.
- **User-Friendly**: No credentials needed for use.

## Installation and Usage

BOLDigger3 requires Python version 3.10 or higher and can be easily installed using pip in any command line:

`pip install boldigger3`

This command will install BOLDigger3 along with all its dependencies.
OLDigger3 uses the Python package ```playwright```, which needs a separate installation prior to first execution:

`playwright install`

To run the ```identify``` function, use the following command:

`boldigger3 identify PATH_TO_FASTA --db DATABASE_NR --mode OPERATING MODE`

# Databases

The ```--db``` is a number between 1 and 7 corresponding to the seven databases BOLD v5 currently offers:

1: **ANIMAL LIBRARY (PUBLIC)**
2: **ANIMAL SPECIES-LEVEL LIBRARY (PUBLIC + PRIVATE)**
3: **ANIMAL LIBRARY (PUBLIC+PRIVATE)**
4: **VALIDATED CANADIAN ARTHROPOD LIBRARY**
5: **PLANT LIBRARY (PUBLIC)**
6: **FUNGI LIBRARY (PUBLIC)**
7: **ANIMAL SECONDARY MARKERS (PUBLIC)**

# Operating modes

The ```--mode``` is a number between 1 and the corresponding to the 3 operating modes BOLD v5 currently offers:

1: **Rapid Species Search**
2: **Genus and Species Search**
3: **Exhaustive Search**

To customize the implemented thresholds for user-specific needs, the thresholds can be passed as an additional (ordered) argument. Up to five different thresholds can be passed for the different taxonomic levels (Species, Genus, Family, Order, Class). Thresholds not passed will be replaced by default, but BOLDigger3 will also inform you about this:

`boldigger2 identify PATH_TO_FASTA --db DATABASE_NR --mode OPERATING MODE --thresholds 99 97`

Output:

```
19:16:16: Default thresholds changed!
19:16:16: Species: 99, Genus: 97, Family: 90, Order: 85
```

When a new version is released, you can update BOLDigger2 by typing:

`pip install --upgrade boldigger3`

## How to cite

Buchner D, Leese F (2020) BOLDigger – a Python package to identify and organise sequences with the Barcode of Life Data systems. Metabarcoding and Metagenomics 4: e53535. https://doi.org/10.3897/mbmg.4.53535


## BOLDigger2 Algorithm

The BOLDigger2 algorithm operates according to the following flowchart:

1. **Log in to BOLD:**
   - Authenticate with the BOLD data systems.

2. **Generate Download Links for Species-Level Barcodes:**
   - Generate the download links for the species-level barcode database for a batch of sequences.

3. **Download Top 100 Hits:**
   - Retrieve the download links from the previous step.
   - Download the top 100 hits for each link.
   - Save the results to an HDF storage with the key `"top_100_hits_unsorted"`.
   - Continue until all sequences are identified.

4. **Identify Sequences Without Species-Level Hits:**
   - Read the unsorted top 100 hits.
   - Identify sequences that did not yield a species-level hit.

5. **Generate Download Links for All Records:**
   - Generate download links for a batch of sequences without species-level hits for the "all records on BOLD" database.

6. **Download Top 100 Hits for All Records:**
   - Retrieve the download links from the previous step.
   - Download the top 100 hits for each link.
   - Save the results to an HDF storage with the key `"top_100_hits_unsorted"`.
   - Continue until all sequences are identified.

7. **Sort and Save Top Hits:**
   - Read all top 100 hits.
   - Remove duplicate entries.
   - Sort the hits in the same order as in the FASTA file.
   - Identify all public records and trigger the additional data download.
   - Save them in the HDF storage with the key `"top_100_hits_sorted"`.

8. **Save Additional Data:**
   - Save the hits including additional data to the HDF storage with the key `"top_100_hits_additional_data"`.

9. **Export Additional Data to Excel:**
   - Save the additional data in Excel format.
   - Split the data into tables of 1,000,000 lines each.

10. **Calculate and Save Top Hits:**
    - Calculate the top hit for each sequence.
    - Save the top hits in both Excel format (`identification_result.xlsx`) and Parquet format (`identification_result.parquet.snappy`) for fast further processing.

![how_it_works drawio](https://github.com/user-attachments/assets/f68dbb11-8ea8-45a9-96a3-8dba107ca5be)



### Top hit selection

Different thresholds (97%: species level, 95%: genus level, 90%: family level, 85%: order level, <85% and >= 50: class level) for the taxonomic levels are used to find the best fitting hit. After determining the threshold for all hits the most common hit above the threshold will be selected. Note that for all hits below the threshold, the taxonomic resolution will be adjusted accordingly (e.g. for a 96% hit the species-level information will be discarded, and genus-level information will be used as the lowest taxonomic level).

The BOLDigger2 algorithm functions as follows:

1. **Identify Maximum Similarity**: Find the maximum similarity value among the top 100 hits currently under consideration.
   
2. **Set Threshold**: Set the threshold to this maximum similarity level. Remove all hits with a similarity below this threshold. For example, if the highest hit has a similarity of 100%, the threshold will be set to 97%, and all hits below this threshold will be removed temporarily.

3. **Classification and Sorting**: Count all individual classifications and sort them by abundance.

4. **Filter Missing Data**: Drop all classifications that contain missing data. For instance, if the most common hit is "Arthropoda --> Insecta" with a similarity of 100% but missing values for Order, Family, Genus, and Species.

5. **Identify Common Hit**: Look for the most common hit that has no missing values.

6. **Return Hit**: If a hit with no missing values is found, return that hit.

7. **Threshold Adjustment**: If no hit with no missing values is found, increase the threshold to the next higher level and repeat the process until a hit is found.


### BOLDigger2 Flagging System

BOLDigger2 employs a flagging system to highlight certain conditions, indicating a degree of uncertainty in the selected hit. Currently, there are five flags implemented, which may be updated as needed:

1. **Reverse BIN Taxonomy**: This flag is raised if all of the top 100 hits representing the selected match utilize reverse BIN taxonomy. Reverse BIN taxonomy assigns species names to deposited sequences on BOLD that lack species information, potentially introducing uncertainty.

2. **Differing Taxonomic Information**: If there are two or more entries with differing taxonomic information above the selected threshold (e.g., two species above 97%), this flag is triggered, suggesting potential discrepancies.

3. **Private or Early-Release Data**: If all of the top 100 hits representing the top hit are private or early-release hits, this flag is raised, indicating limited accessibility to data.

4. **Unique Hit**: This flag indicates that the top hit result represents a unique hit among the top 100 hits, potentially requiring further scrutiny.

5. **Multiple BINs**: If the selected species-level hit is composed of more than one BIN, this flag is raised, suggesting potential complexities in taxonomic assignment.

Given the presence of these flags, it is advisable to conduct a closer examination of all flagged hits to better understand and address any uncertainties in the selected hit.