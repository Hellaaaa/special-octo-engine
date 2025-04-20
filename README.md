# B2B Sales Data Warehouse

This project implements a data warehouse solution for B2B sales data, including ETL processes for initial load and incremental updates using Apache Airflow.

## Architecture Overview

The solution consists of:

1. **OLTP Database**: Source transactional database containing B2B sales data
2. **OLAP Database**: Target data warehouse with star schema design
3. **ETL Processes**: Airflow DAGs for initial load and incremental updates
4. **Change Tracking**: Mechanism to detect and process changes in source data

## Data Model

### OLTP Schema (Source)
- `accounts`: Customer account information
- `products`: Product catalog
- `sales_agents`: Sales team information
- `sales_pipeline`: Sales transactions and pipeline data

### OLAP Schema (Target)
- **Dimension Tables**:
  - `DimDate`: Date dimension with hierarchies
  - `DimAccount`: Account dimension (SCD Type 1)
  - `DimProduct`: Product dimension (SCD Type 2)
  - `DimSalesAgent`: Sales agent dimension
  - `DimDealStage`: Deal stage dimension
- **Fact Tables**:
  - `FactSalesPerformance`: Main fact table with sales metrics
  - `FactSalesMonthlyAggregate`: Pre-aggregated monthly sales data

## ETL Processes

### Initial Load (dwh_initial_load.py)
- Creates all required tables in the data warehouse
- Generates the date dimension
- Loads all dimension and fact tables from source data
- Computes initial aggregates
- Validates data integrity

### Incremental Updates (dwh_incremental_update.py)
- Sets up change tracking in the source database
- Detects changes since the last update
- Updates dimensions with appropriate SCD handling
- Updates fact tables with new/modified/deleted records
- Refreshes aggregates
- Validates data integrity

## Slowly Changing Dimensions (SCD)

The solution implements different SCD strategies:

- **Type 1 (Overwrite)**: Used for `DimAccount` and `DimSalesAgent` where historical changes are not required
- **Type 2 (Historical Versioning)**: Used for `DimProduct` to maintain history of product changes

## Error Handling and Monitoring

- **LoadingMetadata**: Tracks the status and progress of each table load
- **ErrorLog**: Records any errors encountered during ETL processes
- **Validation**: Each DAG includes validation steps to ensure data integrity

## Batch Processing

- Data is processed in configurable batches to handle large volumes efficiently
- The batch size can be adjusted based on system capacity

## Change Tracking Mechanism

- Database triggers capture INSERT, UPDATE, and DELETE operations in source tables
- A change tracking table records all modifications with timestamps
- Metadata tables track the last processed change for each table

## Setup Instructions

1. Configure Airflow connections:
   - `b2b_sales`: Connection to the OLTP database
   - `b2b_sales_olap`: Connection to the OLAP database

2. Run the initial load DAG:
   ```bash
   airflow dags trigger dwh_initial_load
   ```

3. Schedule the incremental update DAG:
   ```bash
   airflow dags unpause dwh_incremental_update
   ```

## Monitoring and Maintenance

- Monitor DAG runs through the Airflow UI
- Check the ErrorLog table for any processing errors
- Review validation results for data integrity issues
- Adjust batch sizes if performance issues are encountered

## Performance Considerations

- Indexes are created on key columns to improve query performance
- Batch processing prevents memory issues with large datasets
- Pre-aggregated tables improve reporting query performance
- Change tracking minimizes the amount of data processed in each update

## Future Enhancements

- Add data quality checks for source data
- Implement data lineage tracking
- Add more aggregate tables for common reporting scenarios
- Implement partitioning for large fact tables
- Add data archiving strategy for historical data