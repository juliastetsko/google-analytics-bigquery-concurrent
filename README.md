# Google Analytics Data Pipeline

This project demonstrates how to build a concurrent data pipeline in Python to fetch data from Google Analytics in BigQuery, process the data using pandas, and upload the aggregated results to Google Sheets. The project utilizes `asyncio` for asynchronous operations, `ThreadPoolExecutor` for multithreading, and `gspread_asyncio` for async interacting with Google Sheets.

## Features

1. Concurrently fetches data from BigQuery for a specified date range using threads.
2. Aggregates the data by various dimensions.
3. Uploads the aggregated data to separate sheets in a Google Sheets document.

## Requirements

- Python 3.7+
- Google Cloud BigQuery account
- Google Sheets API enabled
- Service account credentials for both BigQuery and Google Sheets

## Setup

1. **Clone the repository:**

    ```bash
    git clone git@github.com:juliastetsko/google-analytics-bigquery-concurrent.git
    ```

2. **Install the required packages:**

    ```bash
    pip install -r requirements.txt
    ```

3. **Set up environment variables:**

    ```
    GOOGLE_BIGQUERY_CREDENTIALS=/path/to/your/bigquery-service-account.json
    GOOGLE_GSPREAD_CREDENTIALS=/path/to/your/gspread-service-account.json
    GOOGLE_SHEET_LINK=https://docs.google.com/spreadsheets/d/your-google-sheet-id
    ```

## Usage

Run the main script to fetch, process, and upload data:

```bash
python main.py
