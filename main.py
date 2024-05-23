import asyncio
import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import gspread_asyncio
import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from gspread import WorksheetNotFound

BIGQUERY_SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_BIGQUERY_CREDENTIALS"]
GSPREAD_SERVICE_ACCOUNT_JSON = os.environ.get(
    "GOOGLE_GSPREAD_CREDENTIALS", BIGQUERY_SERVICE_ACCOUNT_JSON
)
SHEET_LINK = os.environ["GOOGLE_SHEET_LINK"]
COLUMNS_TO_FETCH = [
    "visitNumber",
    "geoNetwork.country",
    "device.browser",
    "device.operatingSystem",
]
TABLE_PREFIX = "bigquery-public-data.google_analytics_sample.ga_sessions_"
START_DATE = "2017-07-01"
END_DATE = "2017-07-31"


def fetch_dataframes(
        client: bigquery.Client, start_date: datetime, end_date: datetime
) -> list[pd.DataFrame]:
    threads = []
    results = []
    executor = ThreadPoolExecutor(8)
    date = start_date
    while date <= end_date:
        print(f"Querying data from dataset for the period {date}")
        query = f"SELECT {', '.join(COLUMNS_TO_FETCH)} FROM {TABLE_PREFIX}{date.strftime('%Y%m%d')};"
        job = client.query(query)
        threads.append(executor.submit(job.to_dataframe))
        date += datetime.timedelta(days=1)

    for future in as_completed(threads):
        results.append(future.result(timeout=30))
    return results


async def main():
    print(f"BigQuery dataset table: {TABLE_PREFIX}*")
    print(f"Google Sheet link: {SHEET_LINK}")
    bigquery_client = bigquery.Client.from_service_account_json(
        BIGQUERY_SERVICE_ACCOUNT_JSON
    )
    start_date = datetime.datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(END_DATE, "%Y-%m-%d")

    all_dataframes = fetch_dataframes(bigquery_client, start_date, end_date)
    combined_df = pd.concat(all_dataframes)

    visits_per_country = (
        combined_df.groupby("country")["visitNumber"].sum().reset_index()
    )
    visits_per_os = (
        combined_df.groupby("operatingSystem")["visitNumber"].sum().reset_index()
    )
    visits_browser = combined_df.groupby("browser")["visitNumber"].sum().reset_index()

    gspread_client = await authorize_google_sheets()
    await asyncio.gather(
        write_to_google_sheet(
            gspread_client, visits_per_country, SHEET_LINK, "Visits per Country"
        ),
        write_to_google_sheet(
            gspread_client, visits_per_os, SHEET_LINK, "Visits per Operating System"
        ),
        write_to_google_sheet(
            gspread_client, visits_browser, SHEET_LINK, "Visits per Browser"
        ),
    )


def get_gspread_creds():
    creds = Credentials.from_service_account_file(GSPREAD_SERVICE_ACCOUNT_JSON)
    scoped = creds.with_scopes(
        [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
        ]
    )
    return scoped


async def authorize_google_sheets() -> gspread_asyncio.AsyncioGspreadClient:
    a_gspread = gspread_asyncio.AsyncioGspreadClientManager(get_gspread_creds)
    return await a_gspread.authorize()


async def write_to_google_sheet(
        client: gspread_asyncio.AsyncioGspreadClient,
        df: pd.DataFrame,
        sheet_link: str,
        ws_title: str,
):
    print(f"Writing aggregated dataframe to Google Sheet (worksheet {ws_title})")
    sheet = await client.open_by_url(sheet_link)

    try:
        worksheet = await sheet.worksheet(ws_title)
        await worksheet.resize(rows=len(df.axes[0]), cols=len(df.axes[1]))
    except WorksheetNotFound:
        worksheet = await sheet.add_worksheet(title=ws_title, rows=len(df.axes[0]), cols=len(df.axes[1]))

    df_str = df.applymap(str)
    data = [df_str.columns.tolist()] + df_str.values.tolist()
    await worksheet.update(data)


if __name__ == "__main__":
    asyncio.run(main())
