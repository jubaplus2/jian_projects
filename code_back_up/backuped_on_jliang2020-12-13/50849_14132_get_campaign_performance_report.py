import pandas as pd
from googleads import adwords

def get_client(config_path='googleads_saatva.yaml'):
    return adwords.AdWordsClient.LoadFromStorage(config_path)

def download_report_as_stream(client, report_type, fields, start_date, end_date):
    '''Download Adwords report, then save as csv file to base_dir
    Args:
        client: adwords.AdWordsClient.LoadFromStorage()
        report_type: str eg. 'KEYWORDS_PERFORMANCE_REPORT'
        fields: list eg. ["CampaignId", "CampaignName", ... ]
        start_date: str, eg. '2018-02-11'
        end_date: str, eg.
    Return:
        str, path to csv file
    '''
    report_downloader = client.GetReportDownloader(version='v201708')
    # Create report definition.
    report = {
        'reportName': report_type,
        'dateRangeType': 'CUSTOM_DATE',
        'reportType': report_type,
        'downloadFormat': 'CSV',
        'selector': {
            'fields': fields,
            'dateRange': {
                'min': start_date,
                'max': end_date}
        }
    }

    # save output
    return report_downloader.DownloadReportAsStream(report, 
        skip_report_header=True, skip_column_header=False,
        skip_report_summary=True)

client = get_client(config_path='googleads_saatva.yaml')
report_type = 'CAMPAIGN_PERFORMANCE_REPORT'
fields = ["AccountDescriptiveName", "CampaignName", "HourOfDay", "Impressions", "Clicks", "Cost",
          "AdvertisingChannelType", "Date"
         ]

response = download_report_as_stream(client, report_type, fields, '2018-01-29', '2018-02-26')
result = pd.read_csv(response)

# filter
#result = result[result['Advertising Channel']=='Search']
#result = result[result['Impressions']>10]
#

# save csv
result.to_csv('campaign_performance_report.csv', index=False)
