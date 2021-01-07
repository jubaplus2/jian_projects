from celery import Celery
celery = Celery('tasks')
celery.config_from_object('celeryconfig')

import os
import sys
topdir = os.path.join('/home/jzou/jzlib/')
sys.path.append(topdir)

import pandas as pd
import doubleclick_search_api_v2 as ds
import adwords_api as ad

import datetime

@celery.task
def auto_bid(): 
    ad_report = download_ad_reports()
    non_hva, hva = download_ds_reports()


def download_ad_reports():
    client = ad.get_client(config_path='/root/jzou/key_files/googleads_celebrity.yaml')
    report_type = 'KEYWORDS_PERFORMANCE_REPORT'
    fields = ["CampaignName", "AdGroupName", "Criteria", "Device", "Impressions", "Clicks", "Cost"]

    current_time = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M')
    response = ad.download_report_as_stream(client, report_type, fields)
    result = pd.read_csv(response)

    result.to_csv('/root/jzou/adwords_auto_bidding/celebrity/adwords/%s.csv'%current_time,
                   index=False)
    return result

def download_ds_reports():
    credential = ds.create_credentials()
    service = ds.get_service(credential)
    today = (datetime.datetime.today()-datetime.timedelta(minutes=5)).strftime('%Y-%m-%d')
    start_date = today
    end_date = today
    engine_account_id = '700000001217833' #google celebrity cruises
    advertiser_id = '21700000001131725' #celebrity cruises est#2561
    columns_non_hva = ['campaign', 'adGroup', 'keywordText', 'deviceSegment', 'impr',
                       'clicks', 'cost', 'avgPos', 'avgCpc', 'effectiveKeywordMaxCpc']
    '''
    filters = [{"column" : { "columnName": "status" },
                "operator" : "in",
                "values" : ['Active']
               }]
    '''
    current_hour = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M')

    report_id_non_hva = ds.request_report(service, start_date, end_date, advertiser_id,
                                          engine_account_id, columns_non_hva)#, filters)

    columns_hva = ['campaign', 'adGroup', 'keywordText', 'deviceSegment',
                   'floodlightActivity', 'dfaActions', 'dfaRevenue', 'dfaTransactions']
    report_id_hva = ds.request_report(service, start_date, end_date, advertiser_id,
                                      engine_account_id, columns_hva)#, filters)

    non_hva = ds.poll_report(service, report_id_non_hva)
    hva = ds.poll_report(service, report_id_hva)

    non_hva.to_csv('/root/jzou/adwords_auto_bidding/celebrity/non_hva/%s.csv'%current_hour,
                   index=False)
    hva.to_csv('/root/jzou/adwords_auto_bidding/celebrity/hva/%s.csv'%current_hour,
               index=False)
    return non_hva, hva
