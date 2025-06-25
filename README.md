# Data-Governance-Auditoria-BigQuery

# 🕵️ BigQuery Resource Audit

This repository contains a set of queries and logic designed to **audit BigQuery resources** using metadata available through the `INFORMATION_SCHEMA` views and job history.

The goal is to provide visibility into how datasets, tables, and views are being used — and to help identify opportunities for cleanup, optimization, or documentation.

---

## 📌 Overview

The audit generates **five key metadata tables**, which form the basis for resource analysis:

| Table Name               | Description                                                                                        |
| ------------------------ | -------------------------------------------------------------------------------------------------- |
| `job_details`            | Contains information on executed jobs, including query text, user, timestamps, and usage patterns. |
| `dataset_details`        | Metadata about datasets such as location, creation time, labels, and access configuration.         |
| `tables_details`         | Info about all physical tables including schema, size, creation date, and usage insights.          |
| `views_details`          | Metadata about views, with their SQL definitions and last usage extracted from job history.        |
| `dimension_tables_views` | Supporting dimension table to standardize information across tables and views.                     |

---

## 🎯 Actionable Recommendations

Both the `tables_details` and `views_details` tables include an **"action\_recommendation"** column, which provides suggestions such as:

* ✅ **delete** – for unused or outdated resources
* ✅ **review** – for recently inactive or potentially duplicated assets
* ✅ **rename** – if naming conventions are inconsistent
* ✅ **add description** – when metadata is missing


These recommendations are based on usage history, metadata completeness, and organizational best practices.

---

## 🔍 Usage Insights from Job History

The logic behind the analysis uses the `job_details` table as the source of truth for activity tracking.
For each table or view, we extract the **actual usage** by parsing the `query` field of past jobs.

* The field `table_id_in_query` represents the referenced table or view in the job’s SQL text.
* This helps to correlate inactive objects vs. actively queried ones, even if they're not directly joined or materialized.

---

## 🛠️ Requirements

* Google BigQuery
* Permission to access `INFORMATION_SCHEMA` and job metadata
* Sufficient IAM roles (e.g. `bigquery.metadataViewer`, `bigquery.jobUser`)

---

## 📌 Notes

* The logic assumes job history is available for the project (retention varies).
* The solution is intended to support **auditing and data governance** tasks.



