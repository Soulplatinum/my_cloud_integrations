#This script will automate your ETL process for any report of GA4 that you want to replicate to any BI tool such as Power BI, Data Studio due to their flexibility.
#In this case we are going to extract the main KPIs for an e-Marketing team.
#Due to the GA4 API is new, we are truncating all the data with a specific date range. 
#In case you manage big datasets, I recommend you to "WRITE_APPEND" your outputs and then in BQ clean your data with an over_partition. Check my repository called "my_BI_reporting_codes" to know how to do this.
#Pay attention to the incompatibility of dimensions and metrics.
#This is the Cloud Function that you are going to execute every X hours 

#All the libraries that we are going to use
import requests
import pandas as pd
import pyarrow
from datetime import datetime, timedelta, date
from typing import List, Tuple
from google.cloud import bigquery
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (Dimension, Metric, DateRange, Metric, OrderBy, FilterExpression,
                                                MetricAggregation, CohortSpec,FilterExpressionList,FilterExpression,Filter,)
from google.analytics.data_v1beta.types import RunReportRequest, RunRealtimeReportRequest

"""
In case you want to run this Python Script in your PC use the credentials of your service account of GCP
credentials_path = "C:/Python/your_project_file_from_GCP.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
"""

#Calling the BQ's API
client = bigquery.Client()

#Variables, modify them according to your needs

company = "your_company"
property_id = "your_GA4_property"

#GA4 Parameters, take account that in line 68,69,74 and 75, I am specifying some filters. This is optional, take this as an example.
dimensions = ["date","sessionSourceMedium","sessionCampaignName","sessionDefaultChannelGroup"] #*
range_dates = [("2023-01-01", "today")] #*

metrics = ["sessions","totalRevenue","transactions","advertiserAdCost","advertiserAdImpressions","advertiserAdClicks"] #* For GA4ReportWithoutFilters()
bot_metrics = ["sessions"] #* For GA4ReportWithFilters()


#BQ Parameters
write_disposition_method = "WRITE_TRUNCATE" #*"WRITE_APPEND" or "WRITE_TRUNCATE"
bq_table_id_1 = 'your_BQ_project.your_BQ_dataset.your_BQ_table_ID_1' #* For GA4ReportWithoutFilters()
bq_table_id_2 = 'your_BQ_project.your_BQ_dataset.your_BQ_table_ID_2' #* For GA4ReportWithFilters()


#API function that extract all the fields that we specify above with some filters (in this example, I am extracting the bot sessions according to the team) ! Be careful of this, in line 132 there is the same function but without filters.
def GA4ReportWithFilters(property_id, KPIs: List[Metric], offset_row: int = 0, row_limit: int = 100000):
    """Runs a report using a dimension filter. 
    This sample uses relative date range values. See https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/DateRange
    for more information.
    """

    client = BetaAnalyticsDataClient()

    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name=dim) for dim in dimensions],
        metrics=[Metric(name=m) for m in KPIs],
        date_ranges=[DateRange(start_date=date_range[0], end_date=date_range[1]) for date_range in range_dates],
        limit=row_limit,
        offset=offset_row,
        dimension_filter=FilterExpression(
            and_group=FilterExpressionList(
                expressions=[
                    FilterExpression(
                        filter=Filter(
                            field_name="deviceCategory",
                            string_filter=Filter.StringFilter(value="Desktop"),
                        )
                    ),
                    FilterExpression(
                        filter=Filter(
                            field_name="countryId",
                            string_filter=Filter.StringFilter(value="RU"),
                        )
                    ),
                ]
            )
        ),
    )
    response = client.run_report(request)

    output = {}
    if 'property_quota' in response:
        output['quota'] = response.property_quota

    # construct the dataset
    headers = [header.name for header in response.dimension_headers] + [header.name for header in
                                                                        response.metric_headers]
    rows = []
    for row in response.rows:
        rows.append(
            [dimension_value.value for dimension_value in row.dimension_values] + \
            [metric_value.value for metric_value in row.metric_values])

    output['headers'] = headers
    output['rows'] = rows
    output['row_count'] = response.row_count
    output['metadata'] = response.metadata
    output['response'] = response
    return output


#In case your output has more than 100000 rows, the above function is going to give you only the first 100000 values. So that's why we are going to use this loop to extract all the data.
def loopGA4withFilters(property_id,metrics):
    aux = pd.DataFrame()
    df_total = pd.DataFrame()
    offset=0
    limit=100000
    report = GA4ReportWithFilters(property_id, metrics, offset, limit)
    i=0
    x = int(report["row_count"]/100000)+1

    try:
        for i in range(x):
            aux = GA4ReportWithFilters(property_id, metrics, offset, limit)
            aux = pd.DataFrame(columns=aux['headers'], data=aux['rows'])
            df_total = pd.concat([df_total, aux])
            i = i + 1
            offset = offset + 100000

    except:
        print("NA")
        i = i + 1

    return df_total


#Same function but without filters.
def GA4ReportWithoutFilters(property_id, KPIs: List[Metric], offset_row: int = 0, row_limit: int = 100000):
    """Runs a report using a dimension filter. The call returns a time series
    report of `eventCount` when `eventName` is `first_open` for each date.
    This sample uses relative date range values. See https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/DateRange
    for more information.
    """

    client = BetaAnalyticsDataClient()

    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name=dim) for dim in dimensions],
        metrics=[Metric(name=m) for m in KPIs],
        date_ranges=[DateRange(start_date=date_range[0], end_date=date_range[1]) for date_range in range_dates],
        limit=row_limit,
        offset=offset_row,
    )
    response = client.run_report(request)

    output = {}
    if 'property_quota' in response:
        output['quota'] = response.property_quota

    # construct the dataset
    headers = [header.name for header in response.dimension_headers] + [header.name for header in
                                                                        response.metric_headers]
    rows = []
    for row in response.rows:
        rows.append(
            [dimension_value.value for dimension_value in row.dimension_values] + \
            [metric_value.value for metric_value in row.metric_values])

    output['headers'] = headers
    output['rows'] = rows
    output['row_count'] = response.row_count
    output['metadata'] = response.metadata
    output['response'] = response
    return output


#Same loop but with the other function
def loopGA4withoutFilters(property_id,metrics):
    aux = pd.DataFrame()
    df_total = pd.DataFrame()
    offset=0
    limit=100000
    report = GA4ReportWithoutFilters(property_id, metrics, offset, limit)
    i=0
    x = int(report["row_count"]/100000)+1

    try:
        for i in range(x):
            aux = GA4ReportWithoutFilters(property_id, metrics, offset, limit)
            aux = pd.DataFrame(columns=aux['headers'], data=aux['rows'])
            df_total = pd.concat([df_total, aux])
            i = i + 1
            offset = offset + 100000

    except:
        print("NA")
        i = i + 1

    return df_total


#Storing the outputs into data frames
df = loopGA4withoutFilters(property_id,metrics)
df_bots = loopGA4withFilters(property_id,bot_metrics)

#Variable type assignment for both data frames
df['date'] = pd.to_datetime(df['date'], format='%Y%m%d').dt.date
df['sessionSourceMedium'] = df['sessionSourceMedium'].astype('string')
df['sessionCampaignName'] = df['sessionCampaignName'].astype('string')
df['sessionDefaultChannelGroup'] = df['sessionDefaultChannelGroup'].astype('string')
df['sessions'] = df['sessions'].astype('Int64')
df['totalRevenue'] = df['totalRevenue'].astype('float')
df['transactions'] = df['transactions'].astype('Int64')
df['advertiserAdCost'] = df['advertiserAdCost'].astype('float')
df['advertiserAdImpressions'] = df['advertiserAdImpressions'].astype('Int64')
df['advertiserAdClicks'] = df['advertiserAdClicks'].astype('Int64')
df['date_record'] = datetime.today()
df['process_log'] = "CF-eMKT-integration-GA4-" + str(company)

df_bots['date'] = pd.to_datetime(df_bots['date'], format='%Y%m%d').dt.date
df_bots['sessionSourceMedium'] = df_bots['sessionSourceMedium'].astype('string')
df_bots['sessionCampaignName'] = df_bots['sessionCampaignName'].astype('string')
df_bots['sessionDefaultChannelGroup'] = df_bots['sessionDefaultChannelGroup'].astype('string')
df_bots['sessions'] = df_bots['sessions'].astype('Int64')
df['date_record'] = datetime.today()
df['process_log'] = "CF-eMKT-integration-GA4-" + str(company)


def start(a):
    tabla_id_df = bq_table_id_1
    tabla_id_df_bots = bq_table_id_2

    write_disposition = write_disposition_method
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition,
                                        time_partitioning=bigquery.TimePartitioning(
                                                                                    type_=bigquery.TimePartitioningType.DAY,
                                                                                    field="date"
                                                                                    )
                                        )
    
    job1 = client.load_table_from_dataframe(
        df, tabla_id_df, job_config=job_config
    )
    job1.result()

    job2 = client.load_table_from_dataframe(
        df_bots, tabla_id_df_bots, job_config=job_config
    )  # Make an API request.
    job2.result()

    print("eMKT_Cloud_Function executed !!! You can calm down now")
    return "ok"






