import pandas as pd
pd.options.display.max_rows=6
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import time

def initialize_analyticsreporting(KEY_FILE_LOCATION):
    """Initializes an Analytics Reporting API V4 service object.
    Args:
    KEY_FILE_LOCATION: str, ServiceAccount key file
    Returns:
    An authorized Analytics Reporting API V4 service object.
    """

    SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']

    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        KEY_FILE_LOCATION, scopes=SCOPES)

    # Build the service object.
    analytics = build('analytics', 'v4', credentials=credentials)

    return analytics


def get_report(analytics, VIEW_ID, start_date, end_date, ga_metrics, ga_dimensions, next_page='0'):
    """Queries the Analytics Reporting API V4.

    Args:
    analytics: An authorized Analytics Reporting API V4 service object.
    VIEW_ID: str
    start_date: str
    end_date: str
    ga_metrics: list of str
    ga_dimensions: list of str
    Returns:
    The Analytics Reporting API V4 response.
    """
    return analytics.reports().batchGet(
        body = {
            'reportRequests': [
                {
                    'viewId': VIEW_ID,
                    'pageSize': 10000,
                    'pageToken': next_page,
                    'dateRanges': [{'startDate': start_date, 
                                    'endDate': end_date}],
                    'metrics': [{'expression': m} for m in ga_metrics],
                    'dimensions': [{'name': d} for d in ga_dimensions]
                }]
        }
    ).execute()


def response_to_dataframe(response):
    """Parses and prints the Analytics Reporting API V4 response.

    Args:
    response: An Analytics Reporting API V4 response.
    
    Returns:
    An pandas DataFrame
    """
    report = response['reports'][0]
    df_data = []
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

    for row in report.get('data', {}).get('rows', []):
        row_data = dict()
        dimensions = row.get('dimensions', [])
        dateRangeValues = row.get('metrics', [])

        for header, dimension in zip(dimensionHeaders, dimensions):
            row_data[header] = dimension

        for i, values in enumerate(dateRangeValues):
            #print 'Date range: ' + str(i)
            for metricHeader, value in zip(metricHeaders, values.get('values')):
                row_data[metricHeader.get('name')] = value
        df_data.append(row_data)
        
    return pd.DataFrame(df_data)

def get_report_over_10000_rows(analytics, VIEW_ID, start_date, end_date, 
                               ga_metrics, ga_dimensions):
    """Queries the Analytics Reporting API V4 for reports containing over 10000 
    rows.

    Args:
    analytics: An authorized Analytics Reporting API V4 service object.
    VIEW_ID: str
    start_date: str
    end_date: str
    ga_metrics: list of str
    ga_dimensions: list of str
    Returns:
    The Analytics Reporting API V4 response.
    """
    response = get_report(analytics, VIEW_ID, start_date, end_date, ga_metrics, ga_dimensions)
    products_df = response_to_dataframe(response)
    while 'nextPageToken' in response['reports'][0]:
        next_page_token = response['reports'][0]['nextPageToken']
        response = get_report(analytics, VIEW_ID, start_date, end_date, 
                              ga_metrics, ga_dimensions, next_page=next_page_token)
        df = response_to_dataframe(response)
        products_df = products_df.append(df)
        time.sleep(3)
    return products_df