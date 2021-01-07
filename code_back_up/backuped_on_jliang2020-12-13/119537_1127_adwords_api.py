from googleads import adwords
import datetime

def get_client(config_path='/root/jzou/key_files/googleads.yaml'):
    return adwords.AdWordsClient.LoadFromStorage(config_path)

def download_report(client, report_type, fields, base_dir):
    '''Download Adwords report, then save as csv file to base_dir
    Args:
        client: adwords.AdWordsClient.LoadFromStorage()
        report_type: str eg. 'KEYWORDS_PERFORMANCE_REPORT'
        fields: list eg. ["CampaignId", "CampaignName", ... ]
    Return:
        str, path to csv file
    '''
    report_downloader = client.GetReportDownloader(version='v201802')
    time_stamp = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M")
    minute = time_stamp[-5:]
    # Create report definition.
    report = {
        'reportName': report_type,
        'dateRangeType': 'TODAY' if minute != '00:00' else 'YESTERDAY',
        #'dateRangeType': 'YESTERDAY',
        'reportType': report_type,
        'downloadFormat': 'CSV',
        'selector': {
            'fields': fields
        }
    }

    # save output
    PATH = base_dir + '%s_%s.csv' % (report_type, time_stamp)
    with open(PATH, 'w') as output_file:
        report_downloader.DownloadReport(report, output_file,
            skip_report_header=True, skip_column_header=False,
            skip_report_summary=True)
        print 'Report was downloaded to \'%s\'.' %PATH
    return PATH

def download_report_as_stream(client, report_type, fields):
    '''Download Adwords report, then save as csv file to base_dir
    Args:
        client: adwords.AdWordsClient.LoadFromStorage()
        report_type: str eg. 'KEYWORDS_PERFORMANCE_REPORT'
        fields: list eg. ["CampaignId", "CampaignName", ... ]
    Return:
        str, path to csv file
    '''
    report_downloader = client.GetReportDownloader(version='v201802')
    time_stamp = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M")
    minute = time_stamp[-5:]
    # Create report definition.
    report = {
        'reportName': report_type,
        'dateRangeType': 'TODAY' if minute != '00:00' else 'YESTERDAY',
        'reportType': report_type,
        'downloadFormat': 'CSV',
        'selector': {
            'fields': fields
        }
    }

    # save output
    return report_downloader.DownloadReportAsStream(report, 
        skip_report_header=True, skip_column_header=False,
        skip_report_summary=True)
    


def add_bid_modifier(client, ad_group_id, device, bid_modifier):
    '''Modify adgroup's bid by device
    Args:
        client: adwords.AdWordsClient.LoadFromStorage()
        ad_group_id: str, eg. '34678797109'
        bid_modifier: str, eg. '1.00' 
        device: str, 'Computers', 'Mobile' or 'Tablets'
    '''
    # Initialize appropriate service.
    ad_group_bid_modifier_service = client.GetService(
        'AdGroupBidModifierService', version='v201708')

    # Platform criterion ID.q
    criterion_id = {
        'Desktop': '30000', 
        'Mobile': '30001', 
        'Tablet': '30002'
    }[device]

    # Prepare to add an ad group level override.
    operation = {
        # Use 'ADD' to add a new modifier and 
        # 'SET' to update an existing one. A
        # modifier can be removed with the 'REMOVE' operator.
        'operator': 'ADD',
        'operand': {
            'adGroupId': ad_group_id,
            'criterion': {
                'xsi_type': 'Platform',
                'id': criterion_id
            },
            'bidModifier': bid_modifier
        }
    }

    # Add ad group level mobile bid modifier.
    response = ad_group_bid_modifier_service.mutate([operation])
    if response and response['value']:
        modifier = response['value'][0]
        value = modifier['bidModifier'] if 'bidModifier' in modifier else 'unset'
        print ('Campaign ID %s, AdGroup ID %s, Criterion ID %s was updated with '
             'ad group level modifier: %s' %
             (modifier['campaignId'], modifier['adGroupId'],
              modifier['criterion']['id'], value))
    else:
        print 'No modifiers were added.'
