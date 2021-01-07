from celery import Celery
celery = Celery('SK_weekly_task')
celery.config_from_object('SK_celery_configue')

@celery.task
def Smoothie_King_weekly():
    import pandas as pd
    import numpy as np
    import datetime
    import json
    from haversine import haversine
    import zipcodes
    import os
    import paramiko
    import logging
    import glob
    logging.basicConfig(filename='SK_weekly_celery.log', level=logging.INFO)
    
    zip_centers=json.load(open("/home/jian/Docs/Geo_mapping/updated_zip_centers_JL_2019-05-23.json","r"))
    
    output_path_remote="/home/ruoyi/sk_ta_info/"
    output_path_remote_2="/mnt/drv5/sk_internal_data/sk_ta_info/"
    
    my_output_folder_internal="/home/jian/Projects/Smoothie_King/weekly_TA_updates/"
    
    
    
    # In[126]:
    
    today_date_weekday=datetime.datetime.now().date().weekday()
    recent_monday=datetime.datetime.now().date()-datetime.timedelta(days=today_date_weekday%7)
    
    str_8_digits=str(recent_monday)[:4]+str(recent_monday)[5:7]+str(recent_monday)[8:10]

    
    my_output_folder_internal=my_output_folder_internal+"output_"+str_8_digits+"/"
    try:
        os.stat(my_output_folder_internal)
    except:
        os.mkdir(my_output_folder_internal)
    
    
    # In[3]:
    
    # SK Client Access
    
    host = "64.237.51.251" 
    port = 22
    transport = paramiko.Transport((host, port))
    
    account_SK="SmoothieKing"
    pwd_SK="j$uY_e~23#[:9Zz6"
    
    transport.connect(username = account_SK, password = pwd_SK)
    sftp = paramiko.SFTPClient.from_transport(transport)
    
    
    # In[4]:
    
    # email_meassage="Files received: "
    
    new_file_list=sftp.listdir("/mnt/drv5/SmoothieKing")
    # new_weekly_store_list=["/home/client/BigLotsData/"+x for x in new_weekly_file_list if Client_Today_NUM_STR in x]
    new_weekly_store_list=[x for x in new_file_list if (str_8_digits in x) & ("StoreList" in x)]
    
    if len(new_weekly_store_list)==0:
        print("Store_list_not_received,"+str(datetime.datetime.now()))
    elif len(new_weekly_store_list)>1:
        print("Store_list_More_Than_1: "+str(datetime.datetime.now()))
    else:
        # email_meassage=email_meassage+str(new_weekly_store_list[0])
        print("Store_list_received")
        store_list_file=new_weekly_store_list[0]
        remote_path="/mnt/drv5/SmoothieKing/"+new_weekly_store_list[0]
        local_path_storelist='/home/jian/Projects/Smoothie_King/store_list_weekly/'+store_list_file
        sftp.get(remote_path,local_path_storelist)
        
        
    new_weekly_sales=[x for x in new_file_list if (str_8_digits in x) & ("WeeklySalesSummaryData" in x)]
    
    if len(new_weekly_sales)==0:
        print("Store_WeeklySales_not_received,"+str(datetime.datetime.now()))
    elif len(new_weekly_sales)>1:
        print("Store_WeeklySales_More_Than_1: "+str(datetime.datetime.now()))
    else:
        # email_meassage=email_meassage+" | "+str(new_weekly_store_list[0])
        print("Store_WeeklySales_received")
        store_list_file=new_weekly_sales[0]
        remote_path="/mnt/drv5/SmoothieKing/"+new_weekly_sales[0]
        local_path_sales='/home/jian/Projects/Smoothie_King/Weekly_Sales/'+new_weekly_sales[0]
        sftp.get(remote_path,local_path_sales)
        
    sftp.close()
    
    
    # In[7]:
    
    import smtplib
    
    from string import Template
    
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    
    MY_ADDRESS = 'jubapluscc@gmail.com'
    PASSWORD = 'jubaplus2015'
    
    def get_contacts(filename):
        """
        Return two lists names, emails containing names and email addresses
        read from a file specified by filename.
        """
        
        names = []
        emails = []
        with open(filename, mode='r', encoding='utf-8') as contacts_file:
            for a_contact in contacts_file:
                names.append(a_contact.split()[0])
                emails.append(a_contact.split()[1])
        return names, emails
    
    def read_template(filename):
        """
        Returns a Template object comprising the contents of the 
        file specified by filename.
        """
        
        with open(filename, 'r', encoding='utf-8') as template_file:
            template_file_content = template_file.read()
        return Template(template_file_content)
    
    
    
    # In[9]:
    
    if (len(new_weekly_sales)==1) & (len(new_weekly_sales)==1):
    
        names, emails = get_contacts('/home/jian/Projects/Smoothie_King/SK_celery/mycontacts_success.txt') # read contacts
        message_template = read_template('/home/jian/Projects/Smoothie_King/SK_celery/message_success.txt')
        print(names, emails, message_template)
        # set up the SMTP server
        s = smtplib.SMTP(host='smtp.gmail.com', port=587)
        s.starttls()
        s.login(MY_ADDRESS, PASSWORD)
    
        # For each contact, send the email:
        for name, email in zip(names, emails):
            msg = MIMEMultipart()       # create a message
    
            # add in the actual person name to the message template
            message = message_template.substitute(PERSON_NAME=name.title())
    
            # Prints out the message body for our sake
            print(message)
    
            # setup the parameters of the message
            msg['From']=MY_ADDRESS
            msg['To']=email
            msg['Subject']="Smoothie King Data Received"
    
            # add in the message body
            msg.attach(MIMEText(message, 'plain'))
    
            # send the message via the server set up earlier.
            s.send_message(msg)
            del msg
    
        # Terminate the SMTP session and close the connection
        s.close()
    
    else:
        names, emails = get_contacts('/home/jian/Projects/Smoothie_King/SK_celery/mycontacts_failed.txt') # read contacts
        message_template = read_template('/home/jian/Projects/Smoothie_King/SK_celery/message_failed.txt')
        print(names, emails, message_template)
        # set up the SMTP server
        s = smtplib.SMTP(host='smtp.gmail.com', port=587)
        s.starttls()
        s.login(MY_ADDRESS, PASSWORD)
    
        # For each contact, send the email:
        for name, email in zip(names, emails):
            msg = MIMEMultipart()       # create a message
    
            # add in the actual person name to the message template
            message = message_template.substitute(PERSON_NAME=name.title())
    
            # Prints out the message body for our sake
            print(message)
    
            # setup the parameters of the message
            msg['From']=MY_ADDRESS
            msg['To']=email
            msg['Subject']="Smoothie King Data Not Received yet"
    
            # add in the message body
            msg.attach(MIMEText(message, 'plain'))
    
            # send the message via the server set up earlier.
            s.send_message(msg)
            del msg
    
        # Terminate the SMTP session and close the connection
        s.close()
    


    day_of_month=datetime.datetime.now().date().day
    

    if day_of_month<=7:
        smtpObj = smtplib.SMTP('smtp.gmail.com',587)
        smtpObj.ehlo()
        smtpObj.starttls()
        smtpObj.login('jubapluscc@gmail.com','jubaplus2015')

        message="""From: Jian Liang <jubapluscc@gmail.com>
To: Jian <jian@jubaplus.com>
MIME-Version: 1.0
Content-type: text
Subject: Bi-weekly Big Lots data transfered

Hi Jian,

"SK running this week, please check the status later!!!!!!!!!!!!!!"

Thanks,
Jian
            """


        sender="jubapluscc@gmail.com"
        receivers=['jian@jubaplus.com']
        try:
           smtpObj.sendmail(sender, receivers, message)         
           print("Successfully sent email every month")
        except:
           print("Error: unable to send email every month")




        store_list_df=glob.glob("/home/jian/Projects/Smoothie_King/store_list_weekly/*.xlsx")
        store_list_df=pd.DataFrame({"File_Path":store_list_df})
        store_list_df['Date']=store_list_df['File_Path'].apply(lambda x: os.path.basename(x).split("_")[1].split(".")[0])
        store_list_df['Date']=store_list_df['Date'].apply(lambda x: datetime.datetime.strptime(x,"%Y%m%d").date())
        store_list_df=store_list_df[store_list_df['Date']>=recent_monday-datetime.timedelta(days=21)]
        store_list_df=store_list_df.sort_values("Date",ascending=False)
        print(store_list_df.shape)
        if store_list_df.shape[0]!=4:
            logging.info("Error: recent 4 weeks store list count is not 4")
        else:
            store_list_4_weeks=pd.DataFrame()
		    
            for file_path in store_list_df['File_Path'].tolist():
                df=pd.read_excel(file_path,dtype=str)
                df=df[~pd.isnull(df['storenumber'])]
                df=df[df['storenumber']!="nan"]
                df=df[df['status']!="Closed"]
                df=df.drop_duplicates()
                store_list_4_weeks=store_list_4_weeks.append(df)
                store_list_4_weeks=store_list_4_weeks.drop_duplicates("storenumber")
	    
        print("Make Sure 0 as storenumber length>4 (StoreList):",str(len(store_list_4_weeks[store_list_4_weeks['storenumber'].apply(lambda x: len(x))!=4])))
	    
        store_list_4_weeks=store_list_4_weeks.reset_index()
        del store_list_4_weeks['index']
	    
        print(len(store_list_4_weeks['storenumber'].unique().tolist())==len(store_list_4_weeks))
	    # Filter later
	    
	    
	    # In[7]:
	    
	    
        matched_df_with_SG=pd.read_excel("/home/jian/Projects/Smoothie_King/TA/Celery_SK_TA/QC_Address_With_SG_JL_2019-02-12.xlsx",sheetname="Matched_Stores",dtype=str)
        matched_df_with_SG['SG_zip']=matched_df_with_SG['address_comb_SG'].apply(lambda x: x.split("|")[len(x.split("|"))-1])
        store_zip_dic_updated=matched_df_with_SG.set_index("storenumber").to_dict()['SG_zip']
	    
        store_zip_dic_updated.update({"1152":"70810"})
	    
        store_zip_dic_received=store_list_4_weeks.set_index(['storenumber']).to_dict()['zip']
        def update_zip(x):
	        if x in store_zip_dic_updated.keys():
	            y=store_zip_dic_updated[x]
	        else:
	            y=store_zip_dic_received[x]
	        return y
        
        store_list_4_weeks['zip']=store_list_4_weeks['storenumber'].apply(update_zip)
        store_list_4_weeks['zip']=store_list_4_weeks['zip'].apply(lambda x: x.split("-")[0].replace("DMA",""))
	    
	    
	    # In[8]:
	    
        sales_data_df=glob.glob("/home/jian/Projects/Smoothie_King/Weekly_Sales/*.xlsx")
        sales_data_df=pd.DataFrame({"File_Path":sales_data_df})
        sales_data_df['Date']=sales_data_df['File_Path'].apply(lambda x: os.path.basename(x).split("_")[1].split(".")[0])
        sales_data_df['Date']=sales_data_df['Date'].apply(lambda x: datetime.datetime.strptime(x,"%Y%m%d").date())
        sales_data_df=sales_data_df[sales_data_df['Date']>=recent_monday-datetime.timedelta(days=21)]
        sales_data_df=sales_data_df.sort_values("Date",ascending=False)
        if sales_data_df.shape[0]!=4:
            logging.info("Error: recent 4 weeks weekly sales count is not 4")
        else:
            sales_data_4_weeks=pd.DataFrame()
		    
            for file_path in sales_data_df['File_Path'].tolist():
                df=pd.read_excel(file_path,dtype=str)
                df['NetSalesSmoothie']=df['NetSalesSmoothie'].astype(float)
                df['NetSalesRetail']=df['NetSalesRetail'].astype(float)
                df=df[(df['NetSalesSmoothie']>0) | (df['NetSalesRetail']>0)]
                df=df.drop_duplicates()
                sales_data_4_weeks=sales_data_4_weeks.append(df)
        sales_data_4_weeks_agg_count=sales_data_4_weeks.groupby(['StoreNumber'])['NetSalesSmoothie'].count().to_frame().reset_index()
        sales_data_4_weeks_agg_count=sales_data_4_weeks_agg_count.rename(columns={"NetSalesSmoothie":"Count_Available"})
        sales_data_4_weeks_agg_count=sales_data_4_weeks_agg_count[sales_data_4_weeks_agg_count['Count_Available']>=3]


        print("Make Sure 0 as storenumber length>4 (Sales):",str(len(sales_data_4_weeks_agg_count[sales_data_4_weeks_agg_count['StoreNumber'].apply(lambda x: len(x))!=4])))
        print(len(sales_data_4_weeks_agg_count['StoreNumber'].unique().tolist())==len(sales_data_4_weeks_agg_count))
	    
        store_list_4_weeks=store_list_4_weeks[store_list_4_weeks['storenumber'].isin(sales_data_4_weeks_agg_count['StoreNumber'].unique().tolist())]
        forced_exclude_store_list=['0966','1425']
        store_list_4_weeks=store_list_4_weeks[~store_list_4_weeks['storenumber'].isin(forced_exclude_store_list)]
        store_list_4_weeks=store_list_4_weeks.reset_index()
        del store_list_4_weeks['index']
        store_list_4_weeks.shape
	    
        store_GPS_verified=pd.read_excel("/home/jian/Projects/Smoothie_King/TA/QC_address/SK_store_listed_verified_JL_2019-09-03.xlsx",sheetname="assigned_lat_long",dtype=str)
        store_GPS_verified['assigned_lat']=store_GPS_verified['assigned_lat'].astype(float)
        store_GPS_verified['assigned_long']=store_GPS_verified['assigned_long'].astype(float)
        store_GPS_verified['storenumber'].apply(lambda x: len(x)).unique()
        new_store_list=store_list_4_weeks[~store_list_4_weeks['storenumber'].isin(store_GPS_verified['storenumber'].tolist())]
        cols_store_new=new_store_list.columns.tolist()

        force_added_stores=pd.read_excel("/home/jian/Projects/Smoothie_King/SK_celery/SOTF_forced_adding/SOTF_forced_addition_20190429.xlsx",dtype=str)
        new_store_list=new_store_list.append(force_added_stores)
        new_store_list=new_store_list[cols_store_new]

        new_store_list=new_store_list[~new_store_list['storenumber'].isin(store_GPS_verified['storenumber'].tolist())]
        if len(new_store_list)>0:
            new_store_list.to_csv(my_output_folder_internal+"new_store_appared.csv",index=False)
        else:
            new_store_list.to_csv(my_output_folder_internal+"no_new_store_detected.csv",index=False)
        print(new_store_list.shape)
	    
	    
        verified_store_lat_dict=store_GPS_verified.set_index("storenumber").to_dict()['assigned_lat']
        verified_store_lng_dict=store_GPS_verified.set_index("storenumber").to_dict()['assigned_long']
	    
        store_list_4_weeks=store_list_4_weeks[~store_list_4_weeks['storenumber'].isin(new_store_list['storenumber'].tolist())]
	    
        store_list_4_weeks['used_lat']=store_list_4_weeks['storenumber'].apply(lambda x: float(verified_store_lat_dict[x]))
        store_list_4_weeks['used_long']=store_list_4_weeks['storenumber'].apply(lambda x: float(verified_store_lng_dict[x]))
        store_list_cols=store_list_4_weeks.columns.tolist()

        df_new_forced=store_GPS_verified[store_GPS_verified['SOTF_Forced']=="SOTF"]
        df_new_forced['used_lat']=df_new_forced['assigned_lat']
        df_new_forced['used_long']=df_new_forced['assigned_long']
        store_list_4_weeks=store_list_4_weeks.append(df_new_forced).drop_duplicates('storenumber')
        store_list_4_weeks=store_list_4_weeks[store_list_cols]

	    
	    # # Get 3 miles zips based on lat lng
	    
	    # In[12]:
	    
        store_list_4_weeks=store_list_4_weeks.sort_values('storenumber').reset_index()
        del store_list_4_weeks['index']
        store_list_4_weeks['zips_3_miles']=np.nan
        for i in range(len(store_list_4_weeks)):
	        store_id=store_list_4_weeks['storenumber'][i]
	        store_center=[verified_store_lat_dict[store_id],verified_store_lng_dict[store_id]]
	        list_zip_3_miles=[store_list_4_weeks['zip'][i]]
	        for zip_cd in zip_centers.keys():
	            dist=haversine(store_center,zip_centers[zip_cd],miles=True)
	            if dist<=3:
	                list_zip_3_miles=list_zip_3_miles+[zip_cd]
	        store_list_4_weeks['zips_3_miles'][i]=list(set(list_zip_3_miles))
	                
        data=store_list_4_weeks.copy()
	    
        data['TA']=np.nan
        data=data.reset_index()
        del data['index']
        df_TA_zips=pd.DataFrame({"store":[data['storenumber'][0]]*len(data['zips_3_miles'][0]),
	                             "zip":data['zips_3_miles'][0],
	                             "TA":[1]*len(data['zips_3_miles'][0])},
	                            index=[1]*len(data['zips_3_miles'][0]))
	    
	    
        TA_counter=1
	    
        for i in range(1,len(data)):
	        intersection_zip=list(set(data['zips_3_miles'][i]).intersection(set(df_TA_zips['zip'].unique().tolist())))
	        if len(intersection_zip)==0:
	            TA_counter+=1
	            df_TA_zips=df_TA_zips.append(pd.DataFrame({"store":[data['storenumber'][i]]*len(data['zips_3_miles'][i]),
	                                                       "zip":data['zips_3_miles'][i],
	                                                       "TA":[TA_counter]*len(data['zips_3_miles'][i])},
	                                                      index=[i]*len(data['zips_3_miles'][i]))).drop_duplicates()
	            
	        else:
	            df_intersection=df_TA_zips[df_TA_zips['zip'].isin(intersection_zip)]
	            group_df_intersection=df_intersection.groupby(['TA'])['zip'].count().to_frame().reset_index().sort_values(['zip'],ascending=False)
	            selected_TA=group_df_intersection['TA'][0] 
	            
	            df_TA_zips_0=df_TA_zips[~df_TA_zips['TA'].isin(set(group_df_intersection['TA']))]
	            df_TA_zips_1=df_TA_zips[df_TA_zips['TA'].isin(group_df_intersection['TA'].tolist())]
	            df_TA_zips_1['TA']=selected_TA
	            df_TA_zips=df_TA_zips_0.append(df_TA_zips_1).append(pd.DataFrame({"store":[data['storenumber'][i]]*len(data['zips_3_miles'][i]),
	                                                                              "zip":data['zips_3_miles'][i],
	                                                                              "TA":[selected_TA]*len(data['zips_3_miles'][i])},
	                                                                             index=[i]*len(data['zips_3_miles'][i]))).drop_duplicates()
	            
        dict_TA_store=df_TA_zips.set_index('store').to_dict()['TA']
        data['TA']=data['storenumber'].apply(lambda x: dict_TA_store[x])
	    
	    
	    # In[23]:
	    
        df_ta_num_unique=data[['TA']].drop_duplicates().reset_index()
        del df_ta_num_unique['index']
        df_ta_num_unique['new_TA']=[x+1 for x in range(len(df_ta_num_unique))]
	    
        dict_ta_num_unique=df_ta_num_unique.set_index(['TA']).to_dict()['new_TA']
        data['TA']=data['TA'].apply(lambda x: dict_ta_num_unique[x])
        df_TA_zips['TA']=df_TA_zips['TA'].apply(lambda x: dict_ta_num_unique[x])
	    
	    
	    # In[35]:
	    
        summary_store=data.groupby("TA")['storenumber'].count().to_frame().reset_index().rename(columns={"storenumber":"store_count"})
        summary_zip=df_TA_zips[['TA','zip']].drop_duplicates().groupby("TA")['zip'].count().to_frame().reset_index().rename(columns={"zip":"zip_count"})
	    # summary_store_2=df_TA_zips[['TA','store']].drop_duplicates().groupby("TA")['store'].count().to_frame().reset_index().rename(columns={"store":"store_count_2"})
        summary_store_list=data.groupby("TA")['storenumber'].apply(list).to_frame().reset_index().rename(columns={"storenumber":"store_list"})
        summary_zip_list=df_TA_zips[['TA','zip']].drop_duplicates().groupby("TA")['zip'].apply(list).to_frame().reset_index().rename(columns={"zip":"zip_list"})
	    
	    
	    
        summary_by_TA=pd.merge(summary_store,summary_zip,on="TA",how="outer")
        summary_by_TA=pd.merge(summary_by_TA,summary_store_list,on="TA",how="outer")
        summary_by_TA=pd.merge(summary_by_TA,summary_zip_list,on="TA",how="outer")
	    
	    # summary=pd.merge(summary,summary_store_2,on="TA",how="outer")
        TA_Store_zip_list=data.groupby(['TA'])['zip'].apply(set).to_frame().reset_index().rename(columns={"zip":"store_zip_list"})
        summary_by_TA=pd.merge(summary_by_TA,TA_Store_zip_list,on="TA",how="left")
	    


	    
        summary_by_store_count=summary_by_TA.groupby(['store_count'])['TA'].count().to_frame().reset_index().rename(columns={"TA":"TA_count"})
        summary_by_store_list=summary_by_TA.groupby(['store_count'])['TA'].apply(list).to_frame().reset_index().rename(columns={"TA":"TA_list"})
        summary_by_store_count=pd.merge(summary_by_store_count,summary_by_store_list,on="store_count",how="outer")

	    
        df_zip_dist_in_TA=df_TA_zips[["TA","zip"]].drop_duplicates().reset_index()
        del df_zip_dist_in_TA['index']
	    
        output_distance_zip_in_TA=pd.DataFrame()
        counter_k=0
        for ta,group in df_zip_dist_in_TA.groupby(['TA']):
	        group=group.reset_index()
	        del group['index']
	        
	        if len(group)>1:
	        
	            dist_list=[]
	    
	            for i in range(len(group)):
	                zip_hold=group['zip'][i]
	    
	                if zip_hold not in zip_centers.keys():
	                    try:
	                        zip_hold_center=(zipcodes.matching(zip_hold)[0]['lat'],zipcodes.matching(zip_hold)[0]['long'])
	                    except:
	                        print("zip not found, ",zip_hold)
	    
	                else:
	                    zip_hold_center=zip_centers[zip_hold]
	    
	                for j in range(i+1,len(group)):
	                    zip_var=group['zip'][j]
	                    if zip_var not in zip_centers.keys():
	                        try:
	                            zip_var_center=(zipcodes.matching(zip_var)[0]['lat'],zipcodes.matching(zip_var)[0]['long'])
	                        except:
	                            print("zip not found, ",zip_hold)
	    
	                    else:
	                        zip_var_center=zip_centers[zip_var]
	    
	                    try:
	                        dist=haversine(zip_hold_center,zip_var_center,miles=True)
	                    except:
	                        dist=np.nan
	    
	                    dist_list=dist_list+[dist]
	            df=pd.DataFrame({"TA":ta,"dist_min":min(dist_list),"dist_max":max(dist_list),"dist_median":np.median(dist_list),"All_dist":[dist_list]},index=[counter_k])
	            counter_k+=1
	            output_distance_zip_in_TA=output_distance_zip_in_TA.append(df)
	        else:
	            df=pd.DataFrame({"TA":ta,"dist_min":0,"dist_max":0,"dist_median":0,"All_dist":"single_zip"},index=[counter_k])
	            output_distance_zip_in_TA=output_distance_zip_in_TA.append(df)
	        
	    

	    
        zip_DMA=pd.read_excel("/home/jian/Docs/Geo_mapping/Zips by DMA by County16-17 nielsen.xlsx",dtype=str,skiprows=1)
        zip_DMA=zip_DMA.iloc[:,[0,2]]
        zip_DMA.columns=['zip','DMA']
	    
	    
	    # In[47]:
	    
        df_city_state_in_TA=df_TA_zips[["TA","zip"]].drop_duplicates().reset_index()
        del df_city_state_in_TA['index']
	    
        def city_of_zip(x):
	        try:
	            city=zipcodes.matching(x)[0]['city']
	        except:
	            city=np.nan
	        return city
	    
        def state_of_zip(x):
	        try:
	            state=zipcodes.matching(x)[0]['state']
	        except:
	            state=np.nan
	        return state
	        
        df_city_state_in_TA['city']=df_city_state_in_TA['zip'].apply(lambda x: city_of_zip(x))
        df_city_state_in_TA['state']=df_city_state_in_TA['zip'].apply(lambda x: state_of_zip(x))
        df_city_state_in_TA['city']=df_city_state_in_TA['city']+" ("+df_city_state_in_TA['state']+")"
	    
	    
	    
	    # In[48]:
	    
        df_DMA_in_TA=df_TA_zips[["TA","zip"]].drop_duplicates().reset_index()
        del df_DMA_in_TA['index']
        df_DMA_in_TA=pd.merge(df_DMA_in_TA,zip_DMA,on="zip",how="left")
        df_DMA_in_TA.head(2)
	    
	    
	    # In[49]:
        df_city_TA_list=df_city_state_in_TA.groupby(['TA'])['city'].apply(set).to_frame().reset_index()
        df_city_TA_list=df_city_TA_list.rename(columns={"city":"city_list"})
        df_state_TA_list=df_city_state_in_TA.groupby(['TA'])['state'].apply(set).to_frame().reset_index()
        df_state_TA_list=df_state_TA_list.rename(columns={"state":"state_list"})
        df_DMA_TA_list=df_DMA_in_TA.groupby(['TA'])['DMA'].apply(set).to_frame().reset_index()
        df_DMA_TA_list=df_DMA_TA_list.rename(columns={"DMA":"DMA_list"})
	    
	    
	    # In[50]:
	    
        manually_city_dict_of_zip={"29486":"Summerville"}
        manually_state_dict_of_zip={"29486":"SC"}
        df_city_state_in_TA['city'][df_city_state_in_TA['zip']=='29486']=manually_city_dict_of_zip['29486']
        df_city_state_in_TA['state'][df_city_state_in_TA['zip']=='29486']=manually_state_dict_of_zip['29486']
	    
        df_city_state_in_TA=df_city_state_in_TA[~pd.isnull(df_city_state_in_TA['city'])]
	    # zip "08644","76190" removed to determine cities/states because of not existing as in https://www.unitedstateszipcodes.org/ 
	    
	    
	    # In[55]:
	    
        counter_k=1
        df_primary_city_state=pd.DataFrame()
        df_primary_DMA=pd.DataFrame()
        for ta,group in df_city_state_in_TA.groupby(['TA']):
	        df_city=group.groupby(['city'])['zip'].count().to_frame().reset_index().sort_values(['zip'],ascending=False).reset_index()
	        del df_city['index']
	        primary_city=df_city['city'][0]
	        
	        df_state=group.groupby(['state'])['zip'].count().to_frame().reset_index().sort_values(['zip'],ascending=False).reset_index()
	        del df_state['index']
	        primary_state=df_state['state'][0]
	        
	        df=pd.DataFrame({"TA":ta,"Primary_City":primary_city,"Primary_State":primary_state},index=[counter_k])
	        counter_k+=1
	        df_primary_city_state=df_primary_city_state.append(df)
	    
	        
        for ta,group in df_DMA_in_TA.groupby(['TA']):
	        df_DMA=group.groupby(['DMA'])['zip'].count().to_frame().reset_index().sort_values(['zip'],ascending=False).reset_index()
	        del df_DMA['index']
	        primary_DMA=df_DMA['DMA'][0]
	        
	        df=pd.DataFrame({"TA":ta,"Primary_DMA":primary_DMA},index=[counter_k])
	        counter_k+=1
	        df_primary_DMA=df_primary_DMA.append(df)
	    
	    
	    # In[56]:
	    
        summary_by_TA=pd.merge(summary_by_TA,df_city_TA_list,on="TA",how="left")
        summary_by_TA=pd.merge(summary_by_TA,df_state_TA_list,on="TA",how="left")
        summary_by_TA=pd.merge(summary_by_TA,df_DMA_TA_list,on="TA",how="left")
	    
        summary_by_TA=pd.merge(summary_by_TA,df_primary_city_state,on="TA",how="left")
        summary_by_TA=pd.merge(summary_by_TA,df_primary_DMA,on="TA",how="left")
	    
        summary_by_TA=pd.merge(summary_by_TA,output_distance_zip_in_TA,on="TA",how="left")
	    
	    
	    # In[57]:
	    
        data['Latitude']=data['storenumber'].apply(lambda x: float(verified_store_lat_dict[x]))
        data['Longitude']=data['storenumber'].apply(lambda x: float(verified_store_lng_dict[x]))
        data['TA']=data['TA'].astype(str)
        summary_by_TA['TA']=summary_by_TA['TA'].astype(str)
	    
	    
	    # # Split TA
	    
	    # In[58]:
	    
        summary_by_TA['ratio_max_media']=summary_by_TA['dist_max']/summary_by_TA['dist_median']
	    
        summary_by_TA_to_revise=summary_by_TA[(summary_by_TA['ratio_max_media']>2) &                                      (summary_by_TA['store_count']>=2) &                                     (summary_by_TA['dist_max']>12)]
        summary_by_TA_to_keep=summary_by_TA[(summary_by_TA['ratio_max_media']<=2) |                                      (summary_by_TA['store_count']<2) |                                     (summary_by_TA['dist_max']<=12)]
        summary_by_TA_to_revise=summary_by_TA_to_revise.reset_index()
        del summary_by_TA_to_revise['index']
	    
        summary_by_TA_to_keep=summary_by_TA_to_keep.reset_index()
        del summary_by_TA_to_keep['index']
	    
	    
	    # In[68]:
	    
        loop_counter=0
        while len(summary_by_TA_to_revise)>0:
	        loop_counter+=1
	        if loop_counter>30:
	            break
	        
	        summary_by_TA_to_revise=summary_by_TA_to_revise.reset_index()
	        del summary_by_TA_to_revise['index']
	    
	        summary_by_TA_to_keep=summary_by_TA_to_keep.reset_index()
	        del summary_by_TA_to_keep['index']
	    
	        data_keep=data[data['TA'].isin(summary_by_TA_to_keep['TA'])]
	        data_revise=data[~data['TA'].isin(summary_by_TA_to_keep['TA'])]
	        data_revise=data_revise.reset_index()
	        del data_revise['index']
	    
	    
	        
	    
	        store_sub_df=pd.DataFrame()
	        for i in range(len(summary_by_TA_to_revise)):
	            TA=summary_by_TA_to_revise['TA'][i]
	            store_list=summary_by_TA_to_revise['store_list'][i].copy()
	            initial_dist=0
	            store_pair=[np.nan,np.nan]
	            while len(store_list)>=2:
	                store_hold=store_list[0]
	                store_list.remove(store_hold)
	                store_hold_center=[verified_store_lat_dict[store_hold],verified_store_lng_dict[store_hold]]
	                for store_var in store_list:
	                    store_var_center=[verified_store_lat_dict[store_var],verified_store_lng_dict[store_var]]
	                    dist=haversine(store_hold_center,store_var_center,miles=True)
	                    if dist>initial_dist:
	                        initial_dist=dist
	                        store_pair=[store_hold,store_var]
	            store_a=store_pair[0]
	            store_b=store_pair[1]
	    
	            store_a_center=[verified_store_lat_dict[store_a],verified_store_lng_dict[store_a]]
	            store_b_center=[verified_store_lat_dict[store_b],verified_store_lng_dict[store_b]]
	            store_list=summary_by_TA_to_revise['store_list'][i].copy()
	            for store in store_list:
	                store_center=[verified_store_lat_dict[store],verified_store_lng_dict[store]]
	                dist_a=haversine(store_a_center,store_center,miles=True)
	                dist_b=haversine(store_b_center,store_center,miles=True)
	                if dist_a<dist_b:
	                    sub_group="a"
	                else:
	                    sub_group="b"
	                df=pd.DataFrame({"storenumber":store,"TA":TA,"sub_group":sub_group},index=[store])
	                store_sub_df=store_sub_df.append(df)
	        
	        store_sub_df['TA']=store_sub_df['TA'].astype(str)
	        store_sub_df['TA']=store_sub_df['TA']+"_"+store_sub_df['sub_group']
	        store_sub_df=store_sub_df[['storenumber','TA']]
	        store_sub_df_dic=store_sub_df.set_index(['storenumber']).to_dict()['TA']
	                        
	        data_revise['TA']=data_revise['storenumber'].apply(lambda x: store_sub_df_dic[x])
	        data=data_keep.append(data_revise)
	        data=data.sort_values(['storenumber'])
	        data=data.reset_index()
	        del data['index']
	        data['TA']=data['TA'].apply(lambda x: str(x).zfill(5))
	    
	        df_TA_zips=pd.DataFrame()
	        for i in range(len(data)):
	            df=pd.DataFrame({"store":[data['storenumber'][i]]*len(data['zips_3_miles'][i]),
	                             "TA":[data['TA'][i]]*len(data['zips_3_miles'][i]),
	                             "zip":data['zips_3_miles'][i]},index=data['zips_3_miles'][i])
	            df_TA_zips=df_TA_zips.append(df)
	    
	    
	        summary_store=data.groupby("TA")['storenumber'].count().to_frame().reset_index().rename(columns={"storenumber":"store_count"})
	        summary_zip=df_TA_zips[['TA','zip']].drop_duplicates().groupby("TA")['zip'].count().to_frame().reset_index().rename(columns={"zip":"zip_count"})
	        summary_store_list=data.groupby("TA")['storenumber'].apply(list).to_frame().reset_index().rename(columns={"storenumber":"store_list"})
	        summary_zip_list=df_TA_zips[['TA','zip']].drop_duplicates().groupby("TA")['zip'].apply(list).to_frame().reset_index().rename(columns={"zip":"zip_list"})
	    
	        summary_by_TA=pd.merge(summary_store,summary_zip,on="TA",how="outer")
	        summary_by_TA=pd.merge(summary_by_TA,summary_store_list,on="TA",how="outer")
	        summary_by_TA=pd.merge(summary_by_TA,summary_zip_list,on="TA",how="outer")
	    
	        # summary=pd.merge(summary,summary_store_2,on="TA",how="outer")
	        TA_Store_zip_list=data.groupby(['TA'])['zip'].apply(set).to_frame().reset_index().rename(columns={"zip":"store_zip_list"})
	        summary_by_TA=pd.merge(summary_by_TA,TA_Store_zip_list,on="TA",how="left")
	    
	    
	        summary_by_store_count=summary_by_TA.groupby(['store_count'])['TA'].count().to_frame().reset_index().rename(columns={"TA":"TA_count"})
	        summary_by_store_list=summary_by_TA.groupby(['store_count'])['TA'].apply(list).to_frame().reset_index().rename(columns={"TA":"TA_list"})
	        summary_by_store_count=pd.merge(summary_by_store_count,summary_by_store_list,on="store_count",how="outer")
	    
	        df_zip_dist_in_TA=df_TA_zips[["TA","zip"]].drop_duplicates().reset_index()
	        del df_zip_dist_in_TA['index']
	    
	        output_distance_zip_in_TA=pd.DataFrame()
	        counter_k=0
	        for ta,group in df_zip_dist_in_TA.groupby(['TA']):
	            group=group.reset_index()
	            del group['index']
	    
	            if len(group)>1:
	    
	                dist_list=[]
	    
	                for i in range(len(group)):
	                    zip_hold=group['zip'][i]
	    
	                    if zip_hold not in zip_centers.keys():
	                        try:
	                            zip_hold_center=(zipcodes.matching(zip_hold)[0]['lat'],zipcodes.matching(zip_hold)[0]['long'])
	                        except:
	                            print("zip not found, ",zip_hold)
	    
	                    else:
	                        zip_hold_center=zip_centers[zip_hold]
	    
	                    for j in range(i+1,len(group)):
	                        zip_var=group['zip'][j]
	                        if zip_var not in zip_centers.keys():
	                            try:
	                                zip_var_center=(zipcodes.matching(zip_var)[0]['lat'],zipcodes.matching(zip_var)[0]['long'])
	                            except:
	                                print("zip not found, ",zip_hold)
	    
	                        else:
	                            zip_var_center=zip_centers[zip_var]
	    
	                        try:
	                            dist=haversine(zip_hold_center,zip_var_center,miles=True)
	                        except:
	                            dist=np.nan
	    
	                        dist_list=dist_list+[dist]
	                df=pd.DataFrame({"TA":ta,"dist_min":min(dist_list),"dist_max":max(dist_list),"dist_median":np.median(dist_list),"All_dist":[dist_list]},index=[counter_k])
	                counter_k+=1
	                output_distance_zip_in_TA=output_distance_zip_in_TA.append(df)
	            else:
	                df=pd.DataFrame({"TA":ta,"dist_min":0,"dist_max":0,"dist_median":0,"All_dist":"single_zip"},index=[counter_k])
	                output_distance_zip_in_TA=output_distance_zip_in_TA.append(df)
	    
	        zip_DMA=pd.read_excel("/home/jian/Docs/Geo_mapping/Zips by DMA by County16-17 nielsen.xlsx",dtype=str,skiprows=1)
	        zip_DMA=zip_DMA.iloc[:,[0,2]]
	        zip_DMA.columns=['zip','DMA']
	    
	        df_city_state_in_TA=df_TA_zips[["TA","zip"]].drop_duplicates().reset_index()
	        del df_city_state_in_TA['index']

	    
	        df_city_state_in_TA['city']=df_city_state_in_TA['zip'].apply(lambda x: city_of_zip(x))
	        df_city_state_in_TA['state']=df_city_state_in_TA['zip'].apply(lambda x: state_of_zip(x))
	        
	        manually_city_dict_of_zip={"29486":"Summerville"}
	        manually_state_dict_of_zip={"29486":"SC"}
	        df_city_state_in_TA['city'][df_city_state_in_TA['zip']=='29486']=manually_city_dict_of_zip['29486']
	        df_city_state_in_TA['state'][df_city_state_in_TA['zip']=='29486']=manually_state_dict_of_zip['29486']
	    
	        df_city_state_in_TA=df_city_state_in_TA[~pd.isnull(df_city_state_in_TA['city'])]
	        
	        df_city_state_in_TA['city']=df_city_state_in_TA['city']+" ("+df_city_state_in_TA['state']+")"
	    
	        df_DMA_in_TA=df_TA_zips[["TA","zip"]].drop_duplicates().reset_index()
	        del df_DMA_in_TA['index']
	        df_DMA_in_TA=pd.merge(df_DMA_in_TA,zip_DMA,on="zip",how="left")
	    
	        df_city_TA_list=df_city_state_in_TA.groupby(['TA'])['city'].apply(set).to_frame().reset_index()
	        df_city_TA_list=df_city_TA_list.rename(columns={"city":"city_list"})
	        df_state_TA_list=df_city_state_in_TA.groupby(['TA'])['state'].apply(set).to_frame().reset_index()
	        df_state_TA_list=df_state_TA_list.rename(columns={"state":"state_list"})
	        df_DMA_TA_list=df_DMA_in_TA.groupby(['TA'])['DMA'].apply(set).to_frame().reset_index()
	        df_DMA_TA_list=df_DMA_TA_list.rename(columns={"DMA":"DMA_list"})
	    
	        counter_k
	        df_primary_city_state=pd.DataFrame()
	        df_primary_DMA=pd.DataFrame()
	        
	        df_city_state_in_TA
	        for ta,group in df_city_state_in_TA.groupby(['TA']):
	            df_city=group.groupby(['city'])['zip'].count().to_frame().reset_index().sort_values(['zip'],ascending=False).reset_index()
	            del df_city['index']
	            primary_city=df_city['city'][0]
	    
	            df_state=group.groupby(['state'])['zip'].count().to_frame().reset_index().sort_values(['zip'],ascending=False).reset_index()
	            del df_state['index']
	            primary_state=df_state['state'][0]
	    
	            df=pd.DataFrame({"TA":ta,"Primary_City":primary_city,"Primary_State":primary_state},index=[counter_k])
	            counter_k+=1
	            df_primary_city_state=df_primary_city_state.append(df)
	    
	    
	        for ta,group in df_DMA_in_TA.groupby(['TA']):
	            df_DMA=group.groupby(['DMA'])['zip'].count().to_frame().reset_index().sort_values(['zip'],ascending=False).reset_index()
	            del df_DMA['index']
	            primary_DMA=df_DMA['DMA'][0]
	    
	            df=pd.DataFrame({"TA":ta,"Primary_DMA":primary_DMA},index=[counter_k])
	            counter_k+=1
	            df_primary_DMA=df_primary_DMA.append(df)
	    
	        summary_by_TA=pd.merge(summary_by_TA,df_city_TA_list,on="TA",how="left")
	        summary_by_TA=pd.merge(summary_by_TA,df_state_TA_list,on="TA",how="left")
	        summary_by_TA=pd.merge(summary_by_TA,df_DMA_TA_list,on="TA",how="left")
	    
	        summary_by_TA=pd.merge(summary_by_TA,df_primary_city_state,on="TA",how="left")
	        summary_by_TA=pd.merge(summary_by_TA,df_primary_DMA,on="TA",how="left")
	    
	        summary_by_TA=pd.merge(summary_by_TA,output_distance_zip_in_TA,on="TA",how="left")
	        summary_by_TA['Initial_TA']=summary_by_TA['TA'].apply(lambda x: int(x.split("_")[0]))
	        summary_by_TA=summary_by_TA.sort_values(['Initial_TA','TA'])
	    
	        summary_by_TA['ratio_max_media']=summary_by_TA['dist_max']/summary_by_TA['dist_median']
	    
	        summary_by_TA_to_revise=summary_by_TA[(summary_by_TA['ratio_max_media']>2) &                                          (summary_by_TA['store_count']>=2) &                                         (summary_by_TA['dist_max']>12)]
	        summary_by_TA_to_keep=summary_by_TA[(summary_by_TA['ratio_max_media']<=2) |                                          (summary_by_TA['store_count']<2) |                                         (summary_by_TA['dist_max']<=12)]
	    

	    
	    
	    # In[70]:
	    
        summary_by_TA_output=pd.DataFrame()
        summary_by_TA=summary_by_TA.sort_values(['Initial_TA'])
        for old_ta,group in summary_by_TA.groupby(['Initial_TA']):
            if len(group)>1:
	            group['TA']=[str(old_ta).zfill(3)+"_"+str(x+1) for x in range(len(group))]
            else:
	            group['TA']=str(old_ta).zfill(3)
            summary_by_TA_output=summary_by_TA_output.append(group)
            ta_store_list_dic=summary_by_TA_output.set_index(['TA']).to_dict()['store_list']
	    
	    
	    # In[71]:
	    
        store_to_new_ta_dict={}
        for new_ta in ta_store_list_dic.keys():
	        length=len(ta_store_list_dic[new_ta])
	        df=pd.DataFrame({"store":ta_store_list_dic[new_ta],"new_ta":[new_ta]*length},index=[x for x in range(length)])
	        df_dict=df.set_index(['store']).to_dict()['new_ta']
	        store_to_new_ta_dict.update(df_dict)
        len(store_to_new_ta_dict)    
        data['TA']=data['storenumber'].apply(lambda x: store_to_new_ta_dict[x])
        df_TA_zips['TA']=df_TA_zips['store'].apply(lambda x: store_to_new_ta_dict[x])
        
        summary_by_store_count=summary_by_TA_output.groupby(['store_count'])['TA'].count().to_frame().reset_index().rename(columns={"TA":"TA_count"})
        summary_by_store_list=summary_by_TA_output.groupby(['store_count'])['TA'].apply(list).to_frame().reset_index().rename(columns={"TA":"TA_list"})
        summary_by_store_count=pd.merge(summary_by_store_count,summary_by_store_list,on="store_count",how="outer")
        
        
    # In[73]:
    
        data_for_tableau=data[['storenumber','zip','city','state','Latitude','Longitude','TA']]
        for col in data_for_tableau.columns.tolist()[1:6]:
            data_for_tableau=data_for_tableau.rename(columns={col:"store_"+col})
        data_for_tableau=data_for_tableau.append(df_TA_zips[['TA','zip']])
        data_for_tableau['store_Latitude']=data_for_tableau['store_Latitude'].fillna(0)
        data_for_tableau['store_Longitude']=data_for_tableau['store_Longitude'].fillna(0)
	    
	    
	    # In[137]:
	    
        summary_by_TA_output=summary_by_TA_output.reset_index()
        del summary_by_TA_output['index']
        summary_by_TA_output['latest_TA']=np.nan
        for i in range(len(summary_by_TA_output)):
	        store_count_str=str(summary_by_TA_output['store_count'][i])
	        store_list_str=str(summary_by_TA_output['store_list'][i])
	        summary_by_TA_output['latest_TA'][i]=store_count_str+"_"+store_list_str

	    
	    # In[146]:
	    
        TA_rename_dict=summary_by_TA_output.set_index("TA").to_dict()['latest_TA']
	    
        summary_by_TA_output['TA']=summary_by_TA_output['TA'].apply(lambda x: TA_rename_dict[x])
        data['TA']=data['TA'].apply(lambda x: TA_rename_dict[x])
        df_TA_zips['TA']=df_TA_zips['TA'].apply(lambda x: TA_rename_dict[x])
        
        df_TA_zips['store']=df_TA_zips['store'].astype(str)
        mapping_primary_dma=summary_by_TA_output[['TA','Primary_DMA']]
        df_TA_zips=pd.merge(df_TA_zips,mapping_primary_dma,on="TA",how="left")

        rfa_mapping=pd.read_excel("/home/jian/Projects/Smoothie_King/SK_celery/RFA_region_20190128.xlsx",dtype=str)
        rfa_mapping=rfa_mapping[['Store: Store Number  ↑','DMA','Co-Op/Regional']]
        rfa_mapping=rfa_mapping.rename(columns={"Store: Store Number  ↑":"store"})


        Austin_stores=rfa_mapping[rfa_mapping['DMA'].apply(lambda x: x.lower()=="austin")]
        del rfa_mapping['DMA']

        df_TA_zips=pd.merge(df_TA_zips,rfa_mapping,on="store",how="left")
        df_TA_zips['Co-Op/Regional']=df_TA_zips['Co-Op/Regional'].fillna("Unknown")

        for region,df_group in df_TA_zips.groupby("Co-Op/Regional"):
        	if region!="Unknown":
        		df_group=df_group[['zip','Co-Op/Regional']].drop_duplicates()
        		df_group.to_csv(my_output_folder_internal+"zips_for_the_%s.csv"%region,index=False)
        	else:
        		df_austin=df_group[df_group['store'].isin(Austin_stores['store'].tolist())]
        		df_unknown=df_group[~df_group['store'].isin(Austin_stores['store'].tolist())]

        		df_austin.to_csv(my_output_folder_internal+"zips_for_the_%s.csv"%"AustinDMA",index=False)
        		df_unknown.to_csv(my_output_folder_internal+"zips_for_the_%s.csv"%"UnknownRegion",index=False)
        df_output_overall_zips=df_TA_zips[['zip']].drop_duplicates()
        df_output_overall_zips.to_csv("overall_zips_in_TA.csv",index=False)


	    




	    # In[97]:
	    
        writer=pd.ExcelWriter(my_output_folder_internal+"SmoothieKing_TA_of_3_miles_zips_JL_"+str(datetime.datetime.now().date())+".xlsx",engine="xlsxwriter")
        summary_by_TA_output.to_excel(writer,"summary_by_TA",index=False)
        data=data.sort_values(['storenumber'])
        data.to_excel(writer,"output_TA_by_store",index=False)
        df_TA_zips.to_excel(writer,"zip_TA",index=False)
	    # data_for_tableau.to_excel(writer,"data_for_tableau",index=False)
	    
        del summary_by_store_count['TA_list']
        summary_by_store_count.to_excel(writer,"summary_by_store_count",index=False)
	    
        writer.save()
	    
	    
	    # Copy to Ruoyi
	    
	    # SK Client Access
	    
        host = "64.237.51.251" 
        port = 22
        transport = paramiko.Transport((host, port))
	    
        myaccount="jian"
        pwd_mine="jian@juba2017"
	    
        transport.connect(username = myaccount, password = pwd_mine)
        sftp = paramiko.SFTPClient.from_transport(transport)
	    

        last_saturday=datetime.datetime.now().date()-datetime.timedelta(days=2)-datetime.timedelta(days=datetime.datetime.now().date().weekday())

        remote_path_to_put=output_path_remote+"SmoothieKing_TA_of_3_miles_zips_JL_"+str(last_saturday)+".xlsx"
        remote_path_to_put_2=output_path_remote_2+"SmoothieKing_TA_of_3_miles_zips_JL_"+str(last_saturday)+".xlsx"
	    
	    
	    
        my_file_location=my_output_folder_internal+"SmoothieKing_TA_of_3_miles_zips_JL_"+str(datetime.datetime.now().date())+".xlsx"
        sftp.put(my_file_location,remote_path_to_put)
        sftp.put(my_file_location,remote_path_to_put_2)
	    
        sftp.close()

	    
        zip_list =json.load(open('/home/jian/Projects/Smoothie_King/Weather_Data/zips_for_SmoothieKing_stores.json', 'r'))
        zip_list=zip_list+store_list_4_weeks['zip'].unique().tolist()
        zip_list=list(set(zip_list))
	    
        with open('/home/jian/Projects/Smoothie_King/Weather_Data/zips_for_SmoothieKing_stores.json', 'w') as store_zip_list:
	        json.dump(zip_list, store_zip_list)
	    
	    
        df_example=pd.read_table("/home/jian/Projects/Smoothie_King/SK_celery/Location_Format_from_Joann/location.txt",dtype=str,sep="\t",header=None)
        my_TA_zips=df_TA_zips[['zip']].drop_duplicates()
        my_TA_zips['ZipAll']=[df_example[0][0]]*len(my_TA_zips)
        my_TA_zips['adgeolocation:adzipcode']=[df_example[1][0]]*len(my_TA_zips)
        my_TA_zips=my_TA_zips[['ZipAll','adgeolocation:adzipcode','zip']]
        my_TA_zips.to_csv(my_output_folder_internal+"location_"+str(last_saturday)+".csv",header=None,index=None,sep="\t")
	    
        sftp = paramiko.SFTPClient.from_transport(transport)
        remote_path_to_put_Joann="/mnt/drv5/sk_internal_data/sk_geo/Franchise/"+"location_"+str(last_saturday)+".csv"
	    
        my_file_location=my_output_folder_internal+"location_"+str(last_saturday)+".csv"
        sftp.put(my_file_location,remote_path_to_put_Joann)
	    
        sftp.close()
        transport.close()

        # By RFA region














    
    import smtplib
    smtpObj = smtplib.SMTP('smtp.gmail.com',587)
    smtpObj.ehlo()
    smtpObj.starttls()
    smtpObj.login('jubapluscc@gmail.com','jubaplus2015')
    today_str=str(datetime.datetime.now().date())
    today_str=today_str.replace("-","")
    if os.path.isfile(my_output_folder_internal+"new_store_appared.csv"):
    	message="New store appared and go to check the sotre location!"
    elif os.path.isfile(my_output_folder_internal+"no_new_store_detected.csv"):
    	message="Monthly Smoothie King TA updated done!"

    
    sender="jubapluscc@gmail.com"
    receivers=['jian@jubaplus.com']
    try:
       smtpObj.sendmail(sender, receivers, message)         
       print("Successfully sent email")
    except:
       print("Error: unable to send email")






