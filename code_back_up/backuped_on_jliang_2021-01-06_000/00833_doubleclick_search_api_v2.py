import pandas as pd
import numpy as np

import argparse
import httplib2
import pprint
import time
import datetime
from StringIO import StringIO

from apiclient.discovery import build
from oauth2client import GOOGLE_TOKEN_URI
from oauth2client.client import OAuth2Credentials
from googleapiclient.errors import HttpError


def create_credentials():
    """Create Google OAuth2 credentials.

    Args:
        client_id: Client id of a Google Cloud console project.
        client_secret: Client secret of a Google Cloud console project.
        refresh_token: A refresh token authorizing the Google Cloud console 
            project to access the DS data of some Google user.
    Returns:
        OAuth2Credentials
    """
    return OAuth2Credentials(access_token=None,
       client_id=('549790627766-qnth4m8qvuimg87pnsp4b82lhte7dk5a'
                  '.apps.googleusercontent.com'),
       client_secret='Vta4lQLOL49vVYvktkcPGRNb',
       refresh_token='1/ab7pCGMu3K5AveG0UOUpQ0J08vCp6uM357O8qmoPDMs',
       token_expiry=None,
       token_uri="https://accounts.google.com/o/oauth2/token",
       user_agent=None)

def get_service(credentials):
    """Set up a new DoubleClick Search service.

    Args:
        credentials: An OAuth2Credentials generated with create_credentials, or
        flows in the oatuh2client.client package.
    Returns:
        An authorized Doubleclicksearch serivce.
    """
    # Use the authorize() function of OAuth2Credentials to apply necessary 
    # credential headers to all requests.
    http = credentials.authorize(http = httplib2.Http())

    # Construct the service object for the interacting with the DS API.
    service = build('doubleclicksearch', 'v2', http=http)
    return service

def poll_report(service, report_id):
    """Poll the API with the reportId until the report is ready, up to 10 times.

    Args:
        service: An authorized Doublelcicksearch service.
        report_id: The ID DS has assigned to a report.
    Returns:
        pd.DataFrame, report file
    """
    for _ in xrange(20):
        try:
            request = service.reports().get(reportId=report_id)
            json_data = request.execute()
            if json_data['isReportReady']:
                pprint.pprint('The report is ready.')

                # For large reports, DS automatically fragments the report into 
                # multiple files. The 'files' property in the JSON object that 
                # DS returns contains the list of URLs for file fragment. To 
                # download a report, DS needs to know the report ID and the 
                # index of a file fragment.
                report = pd.DataFrame()
                for i in range(len(json_data['files'])):
                    pprint.pprint('Downloading fragment ' + str(i) + \
                        ' for report ' + report_id)
                    report = report.append(download_files(service, 
                        report_id, str(i)), ignore_index = True)
                return report

            else:
                pprint.pprint('Report is not ready. I will try again.')
                time.sleep(3)
        except HttpError as e:
            error = simplejson.loads(e.content)['error']['errors'][0]

            # See Response Codes
            pprint.pprint('HTTP code %d, reason %s' % (e.resp.status, 
                                                       error['reason']))
            break
        
def download_files(service, report_id, report_fragment):
    """Generate and print sample report.

    Args:
        service: An authorized Doublelcicksearch service.
        report_id: The ID DS has assigned to a report.
        report_fragment: The 0-based index of the file fragment from the files array.
    Returns:
        pd.DataFrame report file
    """
    request = service.reports().getFile(reportId=report_id, 
                                        reportFragment=report_fragment)
    return pd.read_csv(StringIO(request.execute()))

def request_report(service, start_date, end_date, advertiser_id, 
		   engine_account_id, columns, filters=[]):
    """Request sample report and print the report ID that DS returns. 
    See Set Up Your Application.

    Args:
        service: An authorized Doublelcicksearch service.
        start_date, end_date: str, eg.'2017-08-30'
	advertiser_id, engin_account_id: str, eg.'21700000001406447'
	columns: list of columns will be in the report
	filters: list of dict, eg.
		[{"column" : { "columnName": "campaign" },
            	  "operator" : "equals",
            	  "values" : ["G_Brand_Callaway Apparel_Collections_J",]
           	  }] 
    Returns:
        The report id.
    """
    request = service.reports().request(
        body={
                "reportScope": {
                    "agencyId": "20100000000000932",
                    "advertiserId": advertiser_id,
		    "engineAccountId": engine_account_id,
		},
                "reportType": "keyword",
                "columns": [{'columnName': column} for column in columns],   
                "timeRange" : {
                    "startDate" : start_date,
                    "endDate" : end_date
                    },
                "filters": filters,
		"downloadFormat": "csv",
                "maxRowsPerFile": 100000000,
                "statisticsCurrency": "agency",
                "verifySingleTimeZone": "false",
                "includeRemovedEntities": "false"
            }
    )
    json_data = request.execute()
    return json_data['id']  

def merge_hva_and_non_hva(hva, non_hva, hva1, hva2, hva3, transactions, match_keys):
    '''merge two reports downloaded by download_reports().
    Args:
        hva: pd.DataFrame
        non_hva: pd.DataFrame
        hva1, hva2, hva3, transactions, match_keys: list of str
    Returns:
        pd.DataFrame
    '''
    # reformat hva file
    result = pd.DataFrame(columns=match_keys+['transactions', 'revenue' ]+hva1+hva2+hva3)
    for keys, group in hva.groupby(match_keys):
        df = pd.DataFrame([dict(zip(match_keys, keys))])
        df['transactions'] = group[group['floodlightActivity'].isin(transactions)]\
            ['dfaTransactions'].sum()
        df['revenue'] =  group[group['floodlightActivity'].isin(transactions)]\
            ['dfaRevenue'].sum()

        for column in hva1+hva2+hva3:
            df[column] = group[group['floodlightActivity'] == column]\
                ['dfaActions'].sum()
        result = result.append(df, ignore_index = True)
     
    # combine hva and non_hva
    non_hva.dropna(how='any', subset=match_keys, inplace=True)
    merged = non_hva.merge(result, on=match_keys, how='left')

    merged.fillna(0, inplace=True)
    
    # generate new fields
    merged['HVA1'] = merged[hva1].sum(axis=1).apply(int) 
    merged['HVA2'] = merged[hva2].sum(axis=1).apply(int)       
    merged['HVA3'] = merged[hva3].sum(axis=1).apply(int)   

    return merged

def download_DS_reports():
    creds = create_credentials()
    service = get_service(creds)

    time_stamp = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M")

    if time_stamp[-5:] == '00:00':
        yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
        end_date = yesterday.strftime("%Y-%m-%d")
        start_date = yesterday.strftime("%Y-%m-%d")
    else:
        today = datetime.datetime.today()
        end_date = today.strftime("%Y-%m-%d")
        start_date = today.strftime("%Y-%m-%d")

    REPORTID_nonHVA = request_report(service, start_date, end_date, 
            ['campaign', 'adGroup', 'keywordText', 'deviceSegment', 'date', 'impr', 
            'clicks', 'cost', 'avgCpc'])
    REPORTID_HVA = request_report(service, start_date, end_date, 
            ['campaign', 'adGroup', 'keywordText', 'deviceSegment', 'date', 
            'floodlightActivity', 'dfaActions', 'dfaRevenue', 'dfaTransactions'])

    non_hva = poll_report(service, REPORTID_nonHVA)
    hva = poll_report(service, REPORTID_HVA)

    # save non_hva and hva files to csv
    base_dir = '/root/jzou/adwords_auto_bidding/DS_minute/'
    non_hva.to_csv(base_dir+'non_hva_%s.csv'%time_stamp, index=False)
    hva.to_csv(base_dir+'hva_%s.csv'%time_stamp, index=False)

    return merge_hva_and_non_hva(hva, non_hva)
