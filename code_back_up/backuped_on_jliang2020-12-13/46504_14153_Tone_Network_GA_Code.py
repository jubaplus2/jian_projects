#################
import pandas as pd
import numpy as np
import json as json
import datetime
pd.options.display.max_rows=6
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
#################

# Input
Start_Date='2017-10-22'
End_Date=str(datetime.datetime.now().date()-datetime.timedelta(days=1)) # Yesterday
output_path="/home/jian/Projects/Tone_Networks/Output/Test_Tone_Network_GA_Data_From_2017-10-22_To_"+End_Date+".xlsx"


#################
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
    return products_df

def num_func_trans(df):
    if 'ga:sessions' in df.columns:
        df['ga:sessions']=df['ga:sessions'].astype(int)
    if 'ga:users' in df.columns:
        df['ga:users']=df['ga:users'].astype(int)
    if 'ga:bounces' in df.columns:
        df['ga:bounces']=df['ga:bounces'].astype(int)
    if 'ga:bounceRate' in df.columns:
        df['ga:bounceRate']=df['ga:bounceRate'].astype(float)
    if 'ga:pageviews' in df.columns:
        df['ga:pageviews']=df['ga:pageviews'].astype(int)
    if 'ga:uniquePageviews' in df.columns:
        df['ga:uniquePageviews']=df['ga:uniquePageviews'].astype(int)
    if 'ga:totalEvents' in df.columns:
        df['ga:totalEvents']=df['ga:totalEvents'].astype(int)
    if 'ga:uniqueEvents' in df.columns:
        df['ga:uniqueEvents']=df['ga:uniqueEvents'].astype(int)
    if 'ga:goalCompletionsAll' in df.columns:
        df['ga:goalCompletionsAll']=df['ga:goalCompletionsAll'].astype(int)
    if 'ga:dateHourMinute' in df.columns:
        df['ga:dateHourMinute']=df['ga:dateHourMinute'].apply(lambda x:x[0:4]+"-"+x[4:6]+"-"+x[6:8]+" "+x[8:10]+":"+x[10:12])
    if 'ga:date' in df.columns:
        df['ga:date']=df['ga:date'].apply(lambda x:x[0:4]+"-"+x[4:6]+"-"+x[6:8])
    
    return df

writer=pd.ExcelWriter(output_path, engine='xlsxwriter')
analytics = initialize_analyticsreporting('/home/jian/Projects/Tone_Networks/GA/Tone Networks - prod-00ada014c4ba.json')
general_matric=['ga:sessions','ga:users','ga:bounces','ga:bounceRate','ga:pageviews','ga:uniquePageviews','ga:totalEvents','ga:uniqueEvents']

######
# 0 Date
Date = get_report_over_10000_rows(analytics=analytics, VIEW_ID='158084228',start_date=Start_Date, end_date=End_Date, 
                               ga_metrics=general_matric, ga_dimensions=['ga:date'])
Date=num_func_trans(Date)
Date=Date[['ga:date']+general_matric]
Date.rename(columns=lambda x:x[3:len(x)],inplace=True)
Date.to_excel(writer,"Date",index=False)

# 1 Goal Completion All
Goal_Completion_All = get_report_over_10000_rows(analytics=analytics, VIEW_ID='158084228',start_date=Start_Date, end_date=End_Date, 
                               ga_metrics=['ga:goalCompletionsAll'], ga_dimensions=['ga:dateHourMinute','ga:goalCompletionLocation'])
Goal_Completion_All=num_func_trans(Goal_Completion_All)
Goal_Completion_All=Goal_Completion_All[['ga:dateHourMinute','ga:goalCompletionLocation']+['ga:goalCompletionsAll']]
Goal_Completion_All.rename(columns=lambda x:x[3:len(x)],inplace=True)
Goal_Completion_All.to_excel(writer,"Goal_Completion_All",index=False)

# 2 Event and Time
Event_Time=get_report_over_10000_rows(analytics=analytics, VIEW_ID='158084228',start_date=Start_Date, end_date=End_Date, 
                               ga_metrics=['ga:totalEvents','ga:uniqueEvents'], ga_dimensions=['ga:dateHourMinute','ga:eventCategory','ga:eventAction','ga:eventLabel'])
Event_Time=num_func_trans(Event_Time)
Event_Time=Event_Time[['ga:dateHourMinute','ga:eventCategory','ga:eventAction','ga:eventLabel']+['ga:totalEvents','ga:uniqueEvents']]
Event_Time.rename(columns=lambda x:x[3:len(x)],inplace=True)
Event_Time.to_excel(writer,"Event_Time",index=False)

# 3 Time Goal
Time_Goal=get_report_over_10000_rows(analytics=analytics, VIEW_ID='158084228',start_date=Start_Date, end_date=End_Date, 
                               ga_metrics=['ga:goalCompletionsAll']+general_matric, ga_dimensions=['ga:dateHourMinute'])
Time_Goal=num_func_trans(Time_Goal)
Time_Goal=Time_Goal[['ga:dateHourMinute']+['ga:goalCompletionsAll']+general_matric]
Time_Goal.rename(columns=lambda x:x[3:len(x)],inplace=True)
Time_Goal.to_excel(writer,"Time_Goal",index=False)

# 4 Time Page
Time_Page=get_report_over_10000_rows(analytics=analytics, VIEW_ID='158084228',start_date=Start_Date, end_date=End_Date, 
                               ga_metrics=general_matric, ga_dimensions=['ga:dateHourMinute','ga:pagePath'])
Time_Page=num_func_trans(Time_Page)
Time_Page=Time_Page[['ga:dateHourMinute','ga:pagePath']+general_matric]
Time_Page.rename(columns=lambda x:x[3:len(x)],inplace=True)
Time_Page.to_excel(writer,"Time_Page",index=False)

# 5 Metro City
Metro_City=get_report_over_10000_rows(analytics=analytics, VIEW_ID='158084228',start_date=Start_Date, end_date=End_Date, 
                               ga_metrics=general_matric, ga_dimensions=['ga:metro','ga:city'])
Metro_City=num_func_trans(Metro_City)
Metro_City=Metro_City[['ga:metro','ga:city']+general_matric]
Metro_City.rename(columns=lambda x:x[3:len(x)],inplace=True)
Metro_City.to_excel(writer,"Metro_City",index=False)

# 6 Source Medium
Source_Medium=get_report_over_10000_rows(analytics=analytics, VIEW_ID='158084228',start_date=Start_Date, end_date=End_Date, 
                               ga_metrics=general_matric, ga_dimensions=['ga:sourceMedium'])
Source_Medium=num_func_trans(Source_Medium)
Source_Medium=Source_Medium[['ga:sourceMedium']+general_matric]
Source_Medium.rename(columns=lambda x:x[3:len(x)],inplace=True)
Source_Medium.to_excel(writer,"Source_Medium",index=False)

# 7 Gender Age
Gender_Age=get_report_over_10000_rows(analytics=analytics, VIEW_ID='158084228',start_date=Start_Date, end_date=End_Date, 
                               ga_metrics=general_matric, ga_dimensions=['ga:userGender','ga:userAgeBracket'])
Gender_Age=num_func_trans(Gender_Age)
Gender_Age=Gender_Age[['ga:userGender','ga:userAgeBracket']+general_matric]
Gender_Age.rename(columns=lambda x:x[3:len(x)],inplace=True)
Gender_Age.to_excel(writer,"Gender_Age",index=False)

# 8 In Market Segment
In_Market_Segment=get_report_over_10000_rows(analytics=analytics, VIEW_ID='158084228',start_date=Start_Date, end_date=End_Date, 
                               ga_metrics=general_matric, ga_dimensions=['ga:interestInMarketCategory'])
In_Market_Segment=num_func_trans(In_Market_Segment)
In_Market_Segment=In_Market_Segment[['ga:interestInMarketCategory']+general_matric]
In_Market_Segment.rename(columns=lambda x:x[3:len(x)],inplace=True)
In_Market_Segment.to_excel(writer,"In_Market_Segment",index=False)

# 9 Affinity
Affinity=get_report_over_10000_rows(analytics=analytics, VIEW_ID='158084228',start_date=Start_Date, end_date=End_Date, 
                               ga_metrics=general_matric, ga_dimensions=['ga:interestAffinityCategory'])
Affinity=num_func_trans(Affinity)
Affinity=Affinity[['ga:interestAffinityCategory']+general_matric]
Affinity.rename(columns=lambda x:x[3:len(x)],inplace=True)
Affinity.to_excel(writer,"Affinity",index=False)

writer.save()
