import pandas as pd
import datetime
import pandas as pd
from googleads import adwords

from celery import Celery

import logging
logging.basicConfig(filename='/home/jian/Projects/Saatva/Keywords_Report_To_Sp/celery/Saatva_Keywords.log',level=logging.INFO,format='%(asctime)s %(message)s')

celery_task = Celery('Saatva_Keywords_Download_Tasks')
celery_task.config_from_object('celeryconfig_Saatva_Keywords')

@celery_task.task

# In[125]:
def Saatva_Keywords_From_AdWords():
    
    def get_client(config_path):
        return adwords.AdWordsClient.LoadFromStorage(config_path)
    
    
    # In[127]:
    
    time_stamp = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M")
    minute = time_stamp[-5:]
        
    def download_keywords_report_as_stream(client, report_type, fields,minute):
        '''Download Adwords report, then save as csv file to base_dir
        Args:
            client: adwords.AdWordsClient.LoadFromStorage()
            report_type: str eg. 'KEYWORDS_PERFORMANCE_REPORT'
            fields: list eg. ["CampaignId", "CampaignName", ... ]
        Return:
            str, path to csv file
        '''
        report_downloader = client.GetReportDownloader(version='v201809')
        
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
    
    
    # In[128]:
    
    report_type = 'KEYWORDS_PERFORMANCE_REPORT'
    fields = ["CampaignName", "AdGroupName", "Criteria", "Device", "Impressions", "Clicks", "Cost"]
    current_time = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M')
    
    
    # In[129]:
    
    client_Saatva = get_client(config_path='/home/jian/Projects/Saatva/To_Lexie_and_Connor/googleads_saatva_JL_Saatva.yaml')
    response = download_keywords_report_as_stream(client_Saatva, report_type, fields,minute)
    data_1_Saatva = pd.read_csv(response)
    data_1_Saatva['Account']="Saatva"
    data_1_Saatva['client_id']="634-061-0672"
        
    # In[130]:
    
    client_Saatva_Branding = get_client(config_path='/home/jian/Projects/Saatva/To_Lexie_and_Connor/googleads_saatva_JL_Saatva_Branding.yaml')
    response = download_keywords_report_as_stream(client_Saatva_Branding, report_type, fields,minute)
    data_2_Saatva_Branding = pd.read_csv(response)
    data_2_Saatva_Branding['Account']="Saatva_Branding"
    data_2_Saatva_Branding['client_id']="631-365-6046"
    
    
    # In[131]:
    
    client_Saatva_Competitors = get_client(config_path='/home/jian/Projects/Saatva/To_Lexie_and_Connor/googleads_saatva_JL_Saatva_Competitors.yaml')
    response = download_keywords_report_as_stream(client_Saatva_Competitors, report_type, fields,minute)
    data_3_Saatva_Competitors = pd.read_csv(response)
    data_3_Saatva_Competitors['Account']="Saatva_Competitors"
    data_3_Saatva_Competitors['client_id']="183-845-3210"
    
    
    # In[132]:
    
    client_TheSaatvaCompany = get_client(config_path='/home/jian/Projects/Saatva/To_Lexie_and_Connor/googleads_saatva_JL_TheSaatvaCompany.yaml')
    response = download_keywords_report_as_stream(client_Saatva_Competitors, report_type, fields,minute)
    data_4_Saatva_Company = pd.read_csv(response)
    data_4_Saatva_Company['Account']="The_Saatva_Company"
    data_4_Saatva_Company['client_id']="546-629-9909"
    
    
    # In[133]:
    
    client_TheSaatvaCompany_Branding = get_client(config_path='/home/jian/Projects/Saatva/To_Lexie_and_Connor/googleads_saatva_JL_TheSaatvaCompanyBranding.yaml')
    response = download_keywords_report_as_stream(client_Saatva_Competitors, report_type, fields,minute)
    data_5_Saatva_Company = pd.read_csv(response)
    data_5_Saatva_Company['Account']="The_Saatva_Company_Branding"
    data_5_Saatva_Company['client_id']="508-847-9292"
    
    # In[134]:
    
    data_3_Saatva_Competitors=data_3_Saatva_Competitors[data_3_Saatva_Competitors['Campaign'].apply(lambda x: x[len(x)-4:len(x)+1])=="[SM]"]
    data_3_Saatva_Competitors.reset_index(inplace=True)
    del data_3_Saatva_Competitors['index']
    
    output=data_1_Saatva.append(data_2_Saatva_Branding).append(data_3_Saatva_Competitors).append(data_4_Saatva_Company).append(data_5_Saatva_Company)
    output=output[~output['Keyword'].isin(['AutomaticContent','Content'])]
    output['Cost']=output['Cost'].apply(lambda x: x/1000000)
    output=output.reset_index()
    del output['index']
    print("Finished aggregating teh 5 accounts: "+str(datetime.datetime.now()))
    
    # In[136]:
    
    current_time=datetime.datetime.now().strftime('%Y-%m-%d_%H:%M')
    output.to_csv('/home/jian/Projects/Saatva/Keywords_Report_To_Sp/output/%s.csv'%current_time,
                       index=False)
    logging.info("Finished on: "+str(datetime.datetime.now()))
