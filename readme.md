<a name="readme-top"></a>




<!-- PROJECT LOGO -->
<br />


<h2 align="center">Banking Transaction Pipeline <br> (Python • Spark • S3)</h2>

<p align="center">
  <img height="250" src="https://git.camcodes.dev/Cameron/Banking_Data_Pipeline/raw/branch/main/images/Banking.jpg" alt="Banking pipeline" />
</p>


    A Python-based Spark pipeline that ingests banking transactions into S3.
    Bronze → Silver → Gold architecture with data quality validation.



> [!NOTE]
> This project is intended to demonstrate **analytics engineering and lakehouse design patterns**

---


<!-- TABLE OF CONTENTS -->
<details open>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about">About The Project</a>
      <ul>
        <li><a href="#key-features">Key Features</a></li>
      </ul>
    </li>
    <li>
      <a href="#architecture">Architecture</a>
      <ul>
        <li><a href="#bronze">Bronze (Raw)</a></li>
        <li><a href="#silver">Silver (Clean & Validated)</a></li>
        <li><a href="#gold">Gold (Curated & Analytics-Ready)</a></li>
      </ul>
    </li>
    <li><a href="#data-quality">Data Quality & Validation</a></li>
    <li><a href="#outputs">Outputs</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Connect With Me</a></li>
  </ol>
</details>



---

<!-- ABOUT THE PROJECT -->
<a id="about"></a>
# About The Project

This project simulates a **banking transaction data pipeline** using **Python + Apache Spark** with an **S3-backed data lake**. 

It demonstrates how raw transactional data can be ingested, validated, transformed, and curated into analytics-ready datasets using a **Bronze → Silver → Gold** architecture.

## **Tech Stack:** Python, PySpark, Apache Spark, S3 storage

<a id="key-features"></a>
### Key Features

- **Batch ingestion** of banking-style transaction data into an S3-backed Bronze layer
- **Bronze → Silver → Gold** lakehouse-style architecture
- **Data validation gates** (required fields, schema enforcement, duplicates, constraints)
- **Curated datasets** designed for BI and ad-hoc analytics
- Designed with **analytics engineering principles**: reliable outputs, repeatability, clear modeling

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<a id="architecture"></a>
# Architecture

The pipeline follows a lakehouse pattern where each layer has a clear responsibility.

<a id="bronze"></a>
## Bronze (Raw)

**Purpose**
- Store transactions “as received” with minimal transformation

**Why it matters**
- Preserves an auditable source of truth
- Enables reprocessing into Silver/Gold without re-ingesting from the source

---

<a id="silver"></a>
## Silver (Clean & Validated)

**Purpose**
- Standardize schema and datatypes
- Validate records and isolate invalid data
- Deduplicate and normalize for analysis

**Typical transformations**
- Datatype casting (timestamps, numeric amounts)
- Standardized column names and formats
- Deduplication rules (e.g., transaction_id collisions)

---

<a id="gold"></a>
## Gold (Curated & Analytics-Ready)

**Purpose**
- Create business-friendly datasets and aggregations for analytics and BI

**Example outputs**
- Daily transaction counts & totals
- Account/customer-level summaries
- Error/invalid transaction metrics


### Notes

- **Bronze** should contain raw ingested data (audit layer)
- **Silver** should contain cleaned and validated records
- **Gold** should contain curated outputs ready for analytics and BI


<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

<a id="data-quality"></a>
# Data Quality & Validation

The pipeline applies checks to prevent bad data from reaching curated datasets.

**Common checks include:**
- Required fields (e.g., `transaction_id`, `account_id`, `amount`, `timestamp`)
- Schema enforcement (consistent datatypes between runs)
- Duplicate detection (e.g., `transaction_id` collisions)
- Value constraints (e.g., amounts must be non-negative)
- Timestamp parsing and validation
- Quarantine routing for invalid records (optional, stored under `errors/`)

These checks keep the Silver and Gold layers consistent and trustworthy for downstream analytics.


---

<a id="outputs"></a>
## Outputs

**Example S3 layout:**
```text
s3://<bucket>/
  bronze/banking/
  silver/banking/
  gold/banking/
  errors/banking/
```

Gold-layer datasets are structured to support:

- Business intelligence tools (Tableau / Power BI)
- Ad-hoc querying (Spark SQL / DuckDB)
- Downstream analytics and metric definitions

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<a id="roadmap"></a>
## Roadmap

 - Add orchestration (Airflow / Dagster)
 - Implement incremental processing and partitioning
 - Add automated pipeline health checks (row counts, null rates, duplicates)
 - Add unit tests for validation logic
 - Add monitoring, alerting, and run logs
 - Add CDC-style ingestion simulation


<a id="license"></a>
## License

Distributed under the MIT License. See [LICENSE.txt](https://git.camcodes.dev/Cameron/Data_Lab/src/branch/main/LICENSE.txt) for more information.

<a id="contact"></a>
## 💬 Connect With Me


<span>[![LinkedIn](https://github.com/user-attachments/assets/d1d2f882-0bda-46cb-9b7c-9f01eff81da9)](https://www.linkedin.com/in/cameron-css/) [![Portfolio](https://github.com/user-attachments/assets/eb2e9672-e765-442f-89d7-149c7e7db0a8)](https://CamDoesData.com) [![Kaggle](https://github.com/user-attachments/assets/ef5fbcf3-067a-4bb1-b5cd-fd4e369df980)](https://www.kaggle.com/cameronseamons) [![Email](https://github.com/user-attachments/assets/12af3cba-137e-498f-abe1-c66108e5e57a)](mailto:CameronSeamons@gmail.com)  [![Resume](https://github.com/user-attachments/assets/1ee4d4d1-22cd-42ff-b2e4-be7185269306)](https://drive.google.com/file/d/1YaM4hDtt2-79ShBVTN06Y3BU79LvFw6J/view?usp=sharing)</span>


<p align="right">(<a href="#readme-top">back to top</a>)</p> 
