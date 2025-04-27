# Log-Web-Analysis


## Project Overview
This project analyzes the NASA Kennedy Space Center web server logs for the month of July 1995 using Apache Spark on the Databricks platform. The objective is to process raw unstructured log data, extract important information, and perform basic traffic and error analysis.

This project simulates real-world Data Engineering tasks such as data ingestion, data cleaning, transformation, and basic reporting using Big Data technologies.

## Technologies Used
- Apache Spark (PySpark)
- Databricks (Community Edition)
- Python 3
- Regular Expressions (Regex)
- Databricks File System (DBFS)

## Dataset Information
- Source: [NASA HTTP Web Server Logs](https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html)
- File: NASA_access_log_Jul95.gz
- Approximate Size: 20 MB (around 1.8 million HTTP requests)

## Project Objectives
- Read raw log file data into a Spark DataFrame.
- Parse critical fields such as:
  - IP Address
  - Timestamp
  - HTTP Method
  - Requested URL
  - HTTP Status Code
  - Response Size
- Perform analysis to identify:
  - Most visited URLs
  - Most active IP addresses
  - Total number of server requests
  - Total number of error requests (non-200 status codes)
- Save the processed output as CSV files.

## How the Project was Completed

### 1. Data Loading
The log file was uploaded to Databricks FileStore and read into a Spark DataFrame using the `.read.text()` method.

### 2. Data Parsing
Regular expressions (regex) were used to extract structured information from the unstructured log text into separate columns.

Fields extracted:
- IP Address
- Timestamp
- HTTP Method
- URL
- Status Code
- Size of the response

### 3. Data Analysis
- Grouped by URL to find the most accessed pages.
- Grouped by IP address to find the most active visitors.
- Counted total number of server requests.
- Filtered and counted the number of requests resulting in errors (status code not equal to 200).

### 4. Output Storage
The cleaned and structured DataFrame was written to a CSV file for further analysis or reporting.

## Key Results

### Total Number of Requests
```
Total Requests: 1,891,938
```

### Total Number of Error Requests
```
Total Errors: 312,643
```

### Top 5 Most Visited URLs

| URL | Number of Requests |
|----|--------------------|
| /images/NASA-logosmall.gif | 32,522 |
| /images/KSC-logosmall.gif | 28,895 |
| /images/MOSAIC-logosmall.gif | 22,719 |
| /images/USA-logosmall.gif | 22,179 |
| /shuttle/countdown/ | 18,789 |

### Top 5 Most Active IP Addresses

| IP Address | Number of Requests |
|------------|--------------------|
| piweba3y.prodigy.com | 10,978 |
| piweba4y.prodigy.com | 9,525 |
| piweba1y.prodigy.com | 7,778 |
| edams.ksc.nasa.gov | 6,331 |
| 163.205.3.34 | 6,231 |

## How to Run This Project

### Requirements
- Databricks Community Edition account
- Uploaded NASA log file to /FileStore/tables/NASA_access_log_Jul95.gz
- Apache Spark environment (Databricks Notebook)

### Steps
1. Create and start a Databricks cluster.
2. Create a new notebook in Databricks.
3. Attach the notebook to the running cluster.
4. Upload the NASA log file into Databricks FileStore.
5. Execute the provided PySpark code step-by-step.

## Future Improvements
- Perform time series analysis (requests per hour or day).
- Error type classification (404 Not Found, 500 Internal Server Error, etc.).
- Visualize traffic and error trends using graphs.
- Build an automated pipeline for daily or monthly log analysis.

## Tags
Apache Spark, PySpark, Big Data, Data Engineering, Databricks, NASA Logs, Log Analysis, Data Processing
