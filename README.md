# ETL Pipeline

This project implements an **Extract--Transform--Load (ETL)** process to
clean and update the `cademycode` database.\
It reads data from a raw SQLite database, cleans and validates it using
`pandas`, then writes the results to a new SQLite database and CSV
file.\
The pipeline also creates logs and a changelog to track updates.

------------------------------------------------------------------------

## Folder Structure

Course_data_analysis/               ← Project root folder

    ├── dev/                            ← Development/staging environment

    ├── prod/                           ← Production-ready outputs

    ├── notebooks/                      ← Jupyter notebook(s) for exploration
    
    ├── scripts/                        ← Python scripts for pipeline, tests, utilities
    
    | |── run_pipeline.sh               ← Bash script to run the pipeline
    
    ├── README.md                       ← Project overview and documentation
    
    └── requirements.txt                ← List of Python packages used

------------------------------------------------------------------------

## What Each File Does

-   **etl_pipeline.py**
    -   Extracts data from `cademycode.db`.\
    -   Transforms and cleans the data using pandas.\
    -   Loads cleaned data into `prod/cleaned_cademycode_dev.db` and
        `prod/clean_data.csv`.\
    -   Logs run details in `prod/Logs/etl.log`.\
    -   Appends version information and row counts to
        `prod/etl.log`.
-   **run_pipeline.sh**
    -   Simple script to run the ETL pipeline in one command.
-   **test_etl_pipeline.py**
    -   Unit tests that verify:
        -   The cleaned database schema matches the original schema.\
        -   Tables join correctly without missing keys.\
        -   Row counts before and after cleaning match expectations.
-   **prod/etl.log**
    -   Contains logs from each pipeline run.\
    -   Includes row counts before/after cleaning and any error
        messages.
-   **prod/changelog.txt**
    -   Tracks ETL versions and changes over time.\
    -   Includes row count differences and missing data information for
        each run.

------------------------------------------------------------------------

## How to Run the Update Process

1.  **Install dependencies**

    ``` bash
    pip install pandas
    ```

2.  **Ensure the raw database is available**

    -   Place `cademycode.db` in the project root directory.

3.  **Run the pipeline**

    ``` bash
    bash run_pipeline.sh
    ```

    This will:

    -   Execute the ETL process.\
    -   Write logs to `prod/etl.log`.\
    -   Append a changelog entry in `prod/etl.log`.\
    -   Generate `prod/cleaned_cademycode_dev.db` and
        `prod/clean_data.csv`.

4.  **Check the outputs**

    -   Clean database → `prod/cleaned_cademycode_dev.db`\
    -   Clean CSV → `prod/clean_data.csv`\
    -   Logs → `prod/etl.log`\
    -   Changelog → `prod/etl.log`

------------------------------------------------------------------------

## Running Unit Tests

To verify that the pipeline works as intended, run:

``` bash
python -m unittest test_etl_pipeline.py -v
```

The tests confirm: - The cleaned database schema matches the original. -
Tables join successfully. - Row counts and missing data cleanup behave
as expected.

------------------------------------------------------------------------

## Version Control and Logging

-   **Changelog (`prod/etl.log`)**\
    Each ETL run appends a version entry:

        Version 1.1
        Row counts before cleaning: {'students': 1000, 'jobs': 500, 'courses': 50}
        Row counts after cleaning: {'students': 950, 'jobs': 500, 'courses': 50}
        Missing data cleaned: 12 rows dropped, 3 DOB values fixed

-   **Logs (`prod/etl.log`)**\
    Include timestamps, row counts, warnings, and errors. Check this
    after each run to confirm that the ETL executed properly.

------------------------------------------------------------------------

## Error Handling

-   If errors occur (e.g., missing tables or invalid data), they are
    recorded in `prod/etl.log`.\
-   Errors are flagged explicitly and will stop the pipeline rather than
    fail silently.\
-   Always review `etl.log` after running the update.

------------------------------------------------------------------------

## Quick Start

``` bash
# Install dependencies
pip install pandas

# Run the ETL process
bash run_pipeline.sh

# Run unit tests
python -m unittest test_etl_pipeline.py -v
```
