# Healthcare Data Quality Framework Module Readme

## Introduction
Data quality is key to the performance and certification of any models. Here I have put together some data quality issues and the points at which this can be approached during data processing. This is based on an understanding of healthcare care data, taking into account the Kahn data quality framework. 

This document will be updated as the relevant parts of the module reflect this checking.

## Data Quality Dimensions
### 1. Conformance
- **Definition**: Data adheres to predefined formats, standards, and expected structures.
- **Issues**:
  - Incorrect data types (e.g., string instead of integer)
  - Format inconsistencies (e.g., varying date formats, comma instead of decimal point)
  - Misplaced decimal points
  - Unexpected null or missing values

### 2. Completeness
- **Definition**: Data contains all required values and records.
- **Issues**:
  - Missing key attributes (e.g., patient ID, admission date)
  - Incomplete records (e.g., missing diagnosis codes)
  - Partial data pulls due to system failures
  - Total number of nulls exceeds expected number or high degree of missingness

### 3. Consistency
- **Definition**: Data across different sources or systems should match and not contain conflicting information.
- **Issues**:
  - Mismatched or conflicting data between sources (e.g., GP vs SUS data)
  - Contradictory data fields (e.g., different birth dates for the same patient across records)
  - Rapid changes in values that are unrealistic (e.g. significant height increases in adults, implausible weight decline or gain)

### 4. Plausibility
- **Definition**: Data values should be logically and clinically reasonable.
- **Issues**:
  - Patients born before hospital records began
  - Admission date after discharge date
  - GP registration start date before birth date
  - Implausible vital signs (e.g., heart rate of 500 bpm)

### 5. Timeliness
- **Definition**: Data is up-to-date and available when needed.
- **Issues**:
  - Delayed data ingestion leading to outdated patient records
  - Stale data being used for real-time decision-making
  - Inconsistent backdating of data (e.g. pulling death data from NHS spine)

### 6. Accuracy
- **Definition**: Data correctly represents the real world
- **Issues**:
  - Typos in names, addresses, or medical conditions
  - Measurement errors in lab results
  - Misclassified categorical variables (e.g., incorrect ICD-10 codes)

### 7. Distributional Anomalies
- **Definition**: Data should align with expected distributions and patterns.
- **Issues**:
  - Skewed distributions or unexpected clustering, particularly when comparing two different columns
  - Unusual outlier values that are not clinically plausible
  - Systematic bias in recorded values (e.g., all temperature readings rounded to whole numbers)

## Data Quality Checks and Rectification Plan
| **Stage**             | **Checks Implemented** | **Actions on Issues** |
|-----------------------|-------------------|-------------------|
| **Data Input**        | Validate input formats and types | Implement strict input validation rules to ensure adherence to data standards, including functions to check and combine different unit types, the dates when different units start or finish and the distributions of these different units|
|                       | Check for typos and out-of-range values | Provide user feedback and implement automated correction suggestions where feasible, using data visualisation with streamlit|
| **Data Extraction**   | Ensure complete data retrieval | Check across multiple tables to ensure all relevant data is pulled across and combined, present completeness in an interactive app |
| **Data Processing**   | Cross-source validation | Use automated reconciliation scripts to flag and resolve inconsistencies between sources. |
|                       | Remove duplicates and reconcile conflicting records | Apply matching techniques to merge duplicate records while preserving accuracy. |
| **Data Validation**   | Check logical plausibility | Build functions to easily define rules to catch implausible data and trigger alerts for manual review and automatically correct where possible |
|                       | Detect temporal inconsistencies | Implement automated rule-based checks to validate date sequences and flag invalid timelines. |
| **Data Checking**     | Analyse distributions for anomalies | Present distributions in streamlit along with outliers and proportion outlying to investigate how reliable data are |
|                       | Identify clusters of unexpected values | Present different columns against each other to look where columns which are expected to correlate don't correlate, potentially perform some clustering |

