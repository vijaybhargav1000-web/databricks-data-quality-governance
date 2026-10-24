# Databricks Data Quality & Governance Pipeline

A PySpark and Databricks project demonstrating data quality validation, duplicate handling, invalid-record quarantine, and Bronze–Silver–Gold data processing.

## Project Overview

In real-world data engineering, incoming data can contain missing IDs, blank names, duplicate records, and inconsistent formats. These issues can affect downstream analytics and reporting.

This project demonstrates how to identify invalid customer records, separate them for investigation, clean valid data, retain the latest customer record, and generate aggregated results.

## Architecture

```text
         Raw Customer Data
                 |
                 v
        Bronze Layer
       (6 sample records)
                 |
                 v
       Data Quality Checks
          /          \
         v            v
   Valid Records   Invalid Records
         |            |
         v            v
   Silver Layer   Quarantine Layer
   (2 records)    (3 records)
         |
         v
      Gold Layer
   (City-level counts)
```

**Technologies:** Python, PySpark, Delta Lake, Databricks Serverless compute, SQL, pytest, Git, GitHub Actions, and Databricks CLI.

## Key Features

- **Data quality validation:** Detects missing customer IDs and missing or blank customer names.
- **Quarantine handling:** Separates invalid records and assigns rejection reasons.
- **Data cleaning:** Trims whitespace and standardizes the customer name field.
- **Deduplication:** Uses a Spark window function and `row_number()` to retain the latest record for each customer.
- **Data transformation:** Converts the `updated_at` field to a date.
- **Aggregation:** Generates customer counts grouped by city.
- **Automated testing:** Uses pytest to validate data cleaning and invalid-record handling.
- **Continuous integration:** GitHub Actions runs automated tests.
- **Databricks execution:** Runs the PySpark pipeline on Databricks Serverless compute.
- **Delta table validation:** Verifies row counts across four persistent tables.

## Data Quality Results

The sample dataset contains six customer records, including duplicate and invalid entries.

| Layer | Purpose | Verified row count |
|---|---|---:|
| Bronze | Raw customer data | 6 |
| Silver | Valid, cleaned, deduplicated data | 2 |
| Quarantine | Invalid records with rejection reasons | 3 |
| Gold | Aggregated customer counts by city | 2 |

The row counts were verified using a Databricks SQL query against the persistent tables.

## Testing and CI

The local automated test suite covers:

- Cleaning and deduplicating customer data.
- Quarantining invalid records.

**Latest recorded local test result:** 2 tests passed.

GitHub Actions CI was also successfully executed.

## Databricks Deployment and Execution

The project includes a `databricks.yml` configuration file, which was validated and deployed using the Databricks CLI.

A Databricks job was configured through the UI to execute `src/pipeline.py` from the GitHub repository using Serverless compute. A successful execution was observed.

The current pipeline uses hardcoded sample input data and prints the Bronze, Silver, Quarantine, and Gold results.

**Implementation note:** The four persistent Delta tables were created and validated separately. The current job does not automatically write its processing results into those tables. Integrating the pipeline with automated Delta table writes is a future enhancement.

## Project Structure

```text
databricks-data-quality-governance/
├── src/
│   └── pipeline.py
├── tests/
│   └── test_pipeline.py
├── .github/
│   └── workflows/
│       └── ci.yml
├── requirements.txt
├── databricks.yml
└── README.md
```

## How to Run Locally

1. Clone the repository:

   ```bash
   git clone https://github.com/vijaybhargav1000-web/databricks-data-quality-governance.git
   cd databricks-data-quality-governance
   ```

2. Create and activate a Python virtual environment.

   ```bash
   python -m venv .venv
   ```

   Windows PowerShell:

   ```powershell
   .\.venv\Scripts\Activate.ps1
   ```

3. Install the dependencies:

   ```bash
   python -m pip install -r requirements.txt
   ```

4. Run the pipeline:

   ```bash
   python src/pipeline.py
   ```

5. Run the tests:

   ```bash
   python -m pytest tests/test_pipeline.py -v
   ```

The project requires a compatible Python and Java environment for the installed PySpark version.

## Future Enhancements

- Read source data from cloud storage such as Amazon S3.
- Write Bronze, Silver, Quarantine, and Gold outputs directly into Delta tables.
- Implement incremental processing and idempotent pipeline execution.
- Add schema enforcement and schema evolution handling.
- Add pipeline logging, monitoring, and failure alerts.
- Add data quality metrics and reconciliation checks.
- Manage the runnable Databricks job as an Asset Bundle resource.

## Learning Outcomes

This project demonstrates practical experience with PySpark transformations, data quality rules, window functions, deduplication, automated testing, CI, Databricks job execution, and Delta table validation.

