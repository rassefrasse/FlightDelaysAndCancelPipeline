# Data pipeline built with Python
## Objective:

The objective of this project is to inspect, process, and load airline and flight data into a relational database for further analysis and reporting. This involves utilizing tools like Pandas for data inspection and transformation, SQL Server Management Studio (SMSS) for database creation and table management, SQLAlchemy for database engine creation, and various Python scripts for data loading and cleaning.

## Project Steps:

Data Inspection with Pandas:
  - Utilized Pandas library to inspect the raw airline and flight data.

Database Creation in SMSS:
  - Created a new database in SQL Server Management Studio to store the processed data.

Database Engine Creation with SQLAlchemy:
  - Installed the required dependencies like pyodbc and UnixODBC using Homebrew.
  - Created a database engine using SQLAlchemy.

Table Creation in SMSS:
  - Created tables for airports, airlines, and flights in SQL Server Management Studio.

Data Loading Challenges:
  - Faced challenges with missing ODBC drivers on macOS.
  - Installed all required ODBC drivers following Microsoft documentation.
  - Exported the database engine to access the database in Python.

SSL Certificate Issue Resolution:
  - Resolved SSL certificate issues during the connection process.

Batch Data Loading for Efficiency:
  - Implemented batch data loading to improve efficiency and handle memory load issues.
  - Monitored data loading progress using print statements.

Python Code Reusability:
  - Leveraged Python to create reusable code such as asynchronous functions for updating RAM information and subprocesses for importing dynamic IPs.

Database Schema Preparation:
  - Created staging and data warehouse databases.
  - Designed a star schema for the data warehouse with dimension and fact tables.

Data Cleaning and Transformation:
  - Cleaned and transformed raw data into a standardized format.
  - Prepared the data for the ELT (Extract, Load, Transform) process.

Troubleshooting Data Loading Issues:
  - Identified and resolved issues with data constantly deleting itself due to incorrect arguments in the for loop.
  - Rectified the problem by changing the argument from 'replace' to 'append'.

Duplication Handling:
  - Detected and removed duplicate records using Common Table Expressions (CTE).

Pipeline Creation:
  - Created a pipeline between the main database and staging area for data cleansing.

Data Re-import and Cleaning Iteration:
  - Re-imported data into the database and repeated the cleaning process.
  - Addressed specific issues related to data handling for airports.

Documentation:
  - Compiled project documentation summarizing the steps taken, challenges faced, and solutions implemented.

## Conclusion:
Through meticulous data inspection, efficient batch loading, and iterative cleaning processes, the project successfully achieved its objective of preparing the airline and flight data for analysis. The documentation serves as a comprehensive guide for future reference and similar projects.
