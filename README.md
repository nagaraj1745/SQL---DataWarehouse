# 📊 HR Data Warehouse Project (SQL Server)

## 📌 Overview
This project demonstrates how to build an end-to-end **Data Warehouse** using **SQL Server**.  
It covers the complete lifecycle:
- Data ingestion (CSV to staging)
- Transformation (ETL logic via Stored Procedure)
- Data Warehouse schema (dimensions & fact tables)
- Automated ETL execution

The project is designed for **portfolio showcasing** and mimics real-world enterprise HR analytics.

---

## 🏗️ Architecture

### 🔹 Staging Schema
Stores raw CSV data before transformation.

### 🔹 Data Warehouse (DW) Schema
- **DimEmployee** → Employee master data  
- **DimTitle** → Employee job titles  
- **DimDate** → Standardized date dimension  
- **FactPerformance** → Performance and bonus metrics  
