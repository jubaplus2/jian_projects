#!/usr/bin/env python
# coding: utf-8

# In[1]:

def update_audience():
    from facebookads.adobjects.customaudience import CustomAudience
    from facebookads.adobjects.adaccount import AdAccount
    from facebookads.api import FacebookAdsApi
    import json
    import pandas as pd
    import os
    import datetime
    import numpy as np
    import glob
    import sqlalchemy
    import time
    dict_config=json.load(open("/mnt/clients/juba/hqjubaapp02/jliang/Projects/Big_Lots/Predictive_Model/Model_Scripts/config.json"))
    high_date=dict_config['pos_end_date']


    username=dict_config['username']
    password=dict_config['password']
    database=dict_config['database']
    BL_engine=sqlalchemy.create_engine("mysql+pymysql://%s:%s@localhost/%s" % (username, password, database))
    output_folder=dict_config['model_output_folder'] # the mother folder


    BigLotsAccount_id="act_271491453638620"
    BigLotsDcart10_id="act_2449979771956630"

    list_account_id=[BigLotsAccount_id,BigLotsDcart10_id]
    # jian_APP_ID="2537704939796694"# jian's fb marketing app id
    jian_token=json.load(open("/mnt/clients/juba/hqjubaapp02/sharefolder/Docs/FB_token/lasest_FB_token_jian.json","r"))['latest_FB_token']

    str_date_last_week=str(datetime.datetime.strptime(high_date,"%Y-%m-%d").date()-datetime.timedelta(days=7))
    print("high_date %s"%high_date)
    print("str_date_last_week %s"%str_date_last_week)


    # In[2]:


    df_id_email=pd.read_sql("select customer_id_hashed,email_address_hash from BL_Rewards_Master order by sign_up_date desc;",con=BL_engine)
    df_id_email=df_id_email.drop_duplicates("customer_id_hashed")


    # In[3]:


    def load_targeting_df(key_df_type,weekly_output_folder):
        # The targeting label is already wrote in the csv file
        # If new changes needed, we can overwrite based on the y_hat value
        list_weekly_output=glob.glob(weekly_output_folder+"/*.csv")
        file_id_list=[x for x in list_weekly_output if "df_target_ids_labeled_20" in x and key_df_type in x]
        if len(file_id_list)==1:
            file_id_list=file_id_list[0]
        else:
            raise ValueError("file_id_list file count is not 1 for %s"%key_df_type)
        df=pd.read_csv(file_id_list)
        df=df[(df['selection_label']=="target") | (df['sign_up_label']=="new_signs") | (df['email_subscription_label']=="unsub")]
        return df


    def load_targeting_df(key_df_type,weekly_output_folder):
        # The targeting label is already wrote in the csv file
        # If new changes needed, we can overwrite based on the y_hat value
        list_weekly_output=glob.glob(weekly_output_folder+"/*.csv")
        file_id_list=[x for x in list_weekly_output if "df_target_ids_labeled_20" in x and key_df_type in x]
        if len(file_id_list)==1:
            file_id_list=file_id_list[0]
        else:
            raise ValueError("file_id_list file count is not 1 for %s"%key_df_type)
        df=pd.read_csv(file_id_list)
        df=df[(df['selection_label']=="target") | (df['sign_up_label']=="new_signs") | (df['email_subscription_label']=="unsub")]
        return df 

    def remove_ids_from_account(token,aud_id,list_ids_to_remove,fb_schema=CustomAudience.Schema.email_hash):
        FacebookAdsApi.init(access_token=token, api_version='v8.0')
        audience = CustomAudience(aud_id)
        listlen=len(list_ids_to_remove)
        
        chunck_size=1000
        count_removed=0
        for i in range(int(np.ceil(listlen/chunck_size))):
            starti = i*chunck_size
            if (i+1)*chunck_size<listlen:
                endi = (i+1)*chunck_size
            else:
                endi = listlen
            list0 = list_ids_to_remove[starti:endi]
            
            count_removed+=len(list0)
            audience.remove_users(fb_schema, list0)
            time.sleep(1.2)
        print("%i IDs removed from the audience id: %i"%(count_removed,aud_id))
        
    def add_ids_to_account(token,aud_id,list_ids_to_add,fb_schema=CustomAudience.Schema.email_hash):
        FacebookAdsApi.init(access_token=token, api_version='v8.0')
        audience = CustomAudience(aud_id)
        listlen=len(list_ids_to_add)
        
        chunck_size=30000
        count_added=0
        for i in range(int(np.ceil(listlen/chunck_size))):
            starti = i*chunck_size
            if (i+1)*chunck_size<listlen:
                endi = (i+1)*chunck_size
            else:
                endi = listlen
            list0 = list_ids_to_add[starti:endi]
            
            count_added+=len(list0)
            audience.add_users(fb_schema, list0)
        print("%i IDs added to the audience id: %i"%(count_added,aud_id))


    # In[4]:


    # Last week
    list_all_output_weekly_folder=os.listdir(output_folder)

    folder_weekly_output_lastweek=[x for x in list_all_output_weekly_folder if "output_LastWeek_NoDCM_%s"%str_date_last_week in x]
    if len(folder_weekly_output_lastweek)==1:
        folder_weekly_output_lastweek=output_folder+folder_weekly_output_lastweek[0]
        list_weekly_output_lastweek=glob.glob(folder_weekly_output_lastweek+"/*.csv")
    else:
        raise ValueError("weekly output folder count is not 1")


    # In[5]:


    list_all_output_weekly_folder=os.listdir(output_folder)
    folder_weekly_output_thisweek=[x for x in list_all_output_weekly_folder if "output_LastWeek_NoDCM_%s"%high_date in x]
    if len(folder_weekly_output_thisweek)==1:
        folder_weekly_output_thisweek=output_folder+folder_weekly_output_thisweek[0]
        list_weekly_output_thisweek=glob.glob(folder_weekly_output_thisweek+"/*.csv")
    else:
        raise ValueError("weekly output folder count is not 1")


    # In[6]:


    dict_predictive_fb_id={"trans_1_only":[23846273583640265,23846635244320632],
                           "trans_2_plus":[23846273655130265,23846635282950632]}


    # In[7]:


    # Update trans_1_only
    key_df_type="trans_1_only"
        
    df_lastweek = load_targeting_df(key_df_type,folder_weekly_output_lastweek)
    df_thisweek = load_targeting_df(key_df_type,folder_weekly_output_thisweek)

    df_lastweek=pd.merge(df_lastweek,df_id_email,on="customer_id_hashed",how="left")
    df_thisweek=pd.merge(df_thisweek,df_id_email,on="customer_id_hashed",how="left")

    df_lastweek=df_lastweek[pd.notnull(df_lastweek['email_address_hash'])]
    df_thisweek=df_thisweek[pd.notnull(df_thisweek['email_address_hash'])]

    df_lastweek=df_lastweek[df_lastweek['email_address_hash'].apply(len)==64]
    df_thisweek=df_thisweek[df_thisweek['email_address_hash'].apply(len)==64]

    df_emails_lastweek=df_lastweek[['email_address_hash']].drop_duplicates()
    df_emails_lastweek['last_week_label']=1

    df_emails_thisweek=df_thisweek[['email_address_hash']].drop_duplicates()
    df_emails_thisweek['this_week_label']=1

    df_both_week=pd.merge(df_emails_lastweek,df_emails_thisweek,on="email_address_hash",how="outer")

    list_email_addin=df_both_week[pd.isnull(df_both_week['last_week_label'])]['email_address_hash'].tolist()
    list_email_remove=df_both_week[pd.isnull(df_both_week['this_week_label'])]['email_address_hash'].tolist()


    print("len(list_email_addin) : %i"%len(list_email_addin))
    print("len(list_email_remove) : %i"%len(list_email_remove))

    for fb_id in dict_predictive_fb_id[key_df_type]:
        remove_ids_from_account(token=jian_token,
                       aud_id=fb_id,
                       list_ids_to_remove=list_email_remove,
                       fb_schema=CustomAudience.Schema.email_hash)
        add_ids_to_account(token=jian_token,
                       aud_id=fb_id,
                       list_ids_to_add=list_email_addin,
                       fb_schema=CustomAudience.Schema.email_hash)


    # In[8]:


    # Update trans_2_plus
    key_df_type="trans_2_plus"


    df_lastweek = load_targeting_df(key_df_type,folder_weekly_output_lastweek)
    df_thisweek = load_targeting_df(key_df_type,folder_weekly_output_thisweek)

    df_lastweek=pd.merge(df_lastweek,df_id_email,on="customer_id_hashed",how="left")
    df_thisweek=pd.merge(df_thisweek,df_id_email,on="customer_id_hashed",how="left")

    df_lastweek=df_lastweek[pd.notnull(df_lastweek['email_address_hash'])]
    df_thisweek=df_thisweek[pd.notnull(df_thisweek['email_address_hash'])]

    df_lastweek=df_lastweek[df_lastweek['email_address_hash'].apply(len)==64]
    df_thisweek=df_thisweek[df_thisweek['email_address_hash'].apply(len)==64]

    df_emails_lastweek=df_lastweek[['email_address_hash']].drop_duplicates()
    df_emails_lastweek['last_week_label']=1

    df_emails_thisweek=df_thisweek[['email_address_hash']].drop_duplicates()
    df_emails_thisweek['this_week_label']=1

    df_both_week=pd.merge(df_emails_lastweek,df_emails_thisweek,on="email_address_hash",how="outer")

    list_email_addin=df_both_week[pd.isnull(df_both_week['last_week_label'])]['email_address_hash'].tolist()
    list_email_remove=df_both_week[pd.isnull(df_both_week['this_week_label'])]['email_address_hash'].tolist()


    print("len(list_email_addin) : %i"%len(list_email_addin))
    print("len(list_email_remove) : %i"%len(list_email_remove))

    for fb_id in dict_predictive_fb_id[key_df_type]:
        remove_ids_from_account(token=jian_token,
                       aud_id=fb_id,
                       list_ids_to_remove=list_email_remove,
                       fb_schema=CustomAudience.Schema.email_hash)
        add_ids_to_account(token=jian_token,
                       aud_id=fb_id,
                       list_ids_to_add=list_email_addin,
                       fb_schema=CustomAudience.Schema.email_hash)


    # In[9]:


    # To be added by you for put in file into LR SFTP
    '''
    1. Once the new folder is created by LR, use that as the remote folder to put in; The new csv file will replace the old
    2. Write out the df_thisweek into a csv with 3 columns: id, email, and segment (feel free to create you segment name value and keep consistent)
    3. put the local csv from step 2 into the folder, and check the UI for first a few times
    4. Added in there into the crontab

    '''


    # In[ ]:




