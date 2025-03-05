 Data Engineer Interview Challenge
# Data Engineering Assignment

This repository contains the solution for the data engineering assignment, including the ER diagram, data pipeline, and documentation.

## ER Diagram

The ER diagram represents the structure of the raw data tables and their relationships. Below is a summary of the tables and their relationships:

### Hierarchy (Dimension) Tables
1. **`hier_invloc`**: Inventory locations and types.
2. **`hier_possite`**: Point-of-sale sites and their hierarchy.
3. **`hier_rtlloc`**: Retail locations and their hierarchy.
4. **`hier_invstatus`**: Inventory status codes and their hierarchy.
5. **`hier_pricestate`**: Price states and their hierarchy.
6. **`hier_hldy`**: Holidays.
7. **`hier_prod`**: Products and their hierarchy.
8. **`hier_clnd`**: Calendar information and its hierarchy.

### Fact Tables
1. **`fact_averagecosts`**: Average costs for products over time.
2. **`fact_transactions`**: Sales transactions.

### Relationships
- **`fact_transactions`**:
  - `pos_site_id` references `hier_possite.site_id`.
  - `sku_id` references `hier_prod.sku_id`.
  - `fsclwk_id` references `hier_clnd.fsclwk_id`.
  - `price_substate_id` references `hier_pricestate.substate_id`.
- **`fact_averagecosts`**:
  - `fsclwk_id` references `hier_clnd.fsclwk_id`.
  - `sku_id` references `hier_prod.sku_id`.

## Data Pipeline
The pipeline performs the following steps:
1. Loads raw data from DBFS into Spark DataFrames.
2. Performs data quality checks (non-null, uniqueness, foreign key constraints).
3. Normalizes hierarchy tables into separate tables for each level.
4. Creates a refined table (`mview_weekly_sales`) with aggregated sales data.
5. (Bonus) Implements incremental calculation for partially loaded data.

## How to Run
1. Upload the raw data to DBFS.
2. Run the provided PySpark scripts in Databricks.
3. Check the output tables in the staging and refined schemas.

## Tools Used
- **PySpark**: For data processing and transformations.
- **Databricks**: For running the pipeline.
- **Draw.io**: For creating the ER diagram.

## Assumptions
- The leftmost column in each hierarchy table is the primary key.
- All foreign key relationships are explicitly defined in the fact tables.
