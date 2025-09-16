# HR Data Warehouse Project (SQL Server)

## 📌 Overview
This project demonstrates how to build an end-to-end **Data Warehouse** using **SQL Server**.  
It includes staging, transformation, dimension/fact tables, stored procedures, and ETL automation.

## 🏗️ Architecture
- **Staging schema** → Raw data from CSVs
- **DW schema** → Cleaned and structured tables
  - DimEmployee
  - DimTitle
  - DimDate
  - FactPerformance

## ⚙️ Steps
1. Load CSV data into staging (`BULK INSERT`).
2. Run stored procedures:
   ```sql
   EXEC DW.sp_RunETL;
