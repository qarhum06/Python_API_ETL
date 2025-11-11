# Data ETL Pipeline

A modular **ETL (Extract, Transform, Load)** project for processing carts data from a Dummy API into a SQL Server database using Python.  
The project includes structured logging,configurable database connectivity.

---

## Overview

This ETL pipeline automates the process of reading data from API, cleaning and transforming it, and loading it into a SQL Server table named **`Carts`**.

The process follows a simple 3-step design:

1. **Extract** — Reads raw sales data from Excel files.  
2. **Transform** — Cleans, validates, and formats data.  
3. **Load** — Loads transformed data into SQL Server.  

Comprehensive logging ensures each stage of the ETL process is traceable and auditable.

---

## 📁 Project Structure

### Sales_python_project

- **config.py** — Configuration file (database connection, file paths)
- **extract.py** — Extracts data from Excel
- **transform.py** — Transforms/cleans the extracted data
- **load.py** — Loads data into SQL Server
- **logger_config.py** — Logger setup for consistent logging
- **main.py** — Main ETL execution script
- **README.md** — Project documentation
