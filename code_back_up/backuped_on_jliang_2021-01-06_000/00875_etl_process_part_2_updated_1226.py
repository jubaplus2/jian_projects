def get_dist_output_df(input_zip_list,df_store_list,zip_centers):
    import sqlalchemy as sql
    import json
    import pandas as pd
    import datetime
    import os
    import numpy as np
    from haversine import haversine
    import glob
    import random
    from multiprocessing import Pool
    from itertools import repeat


    df_output=pd.DataFrame()

    for zip_cd in input_zip_list:
        z_centroid=zip_centers[zip_cd]
        min_dist=np.inf
        nearest_store=None

        for i,row in df_store_list.iterrows():
            store=row['location_id']
            store_loc=(row['latitude_meas'], row['longitude_meas'])
            dist=haversine(z_centroid,store_loc,unit="mi")
            if dist<=min_dist:
                min_dist=dist
                nearest_store=store
        df=pd.DataFrame({"nearest_BL_store":nearest_store,"nearest_BL_dist":min_dist},index=[zip_cd])
        df=df.reset_index().rename(columns={"index":"zip_cd"})
        df_output=df_output.append(df)
    return df_output

def write_crm_train_test():
    import sqlalchemy as sql
    import json
    import pandas as pd
    import datetime
    import os
    import numpy as np
    from haversine import haversine
    import glob
    import random
    from multiprocessing import Pool
    from itertools import repeat
    from dateutil.relativedelta import relativedelta
    print("Start_Part_2: %s"%str(datetime.datetime.now()))

    with open('./config.json', 'rb') as f:
        dict_config = json.load(f)

    username=dict_config['username']
    password=dict_config['password']
    database=dict_config['database']
    folder_store_list=dict_config['folder_store_list']
    path_TA_excel=dict_config['path_TA_excel']
    path_json_zip_center=dict_config['path_json_zip_center']
    pos_end_date=dict_config['pos_end_date']
    folder_store_list=dict_config['folder_store_list']
    folder_email_unsub=dict_config['folder_email_unsub']
    train_sample_size=dict_config['train_sample_size'] #1000000
    test_ratio=dict_config['test_ratio'] #0.25


    with open('./table_names_%s.json'%str(pos_end_date).replace("-",""), 'rb') as f:
        dict_table_names = json.load(f)
    table_filtered_crm=dict_table_names['table_filtered_crm']


    BL_engine=sql.create_engine("mysql+pymysql://%s:%s@localhost/%s" % (username, password, database))


    # In[3]:


    def create_index(table_name, list_of_columns):
        columns = ', '.join(list_of_columns)
        query = "CREATE INDEX id_index ON %s(%s)" % (table_name, columns)
        print(query)
        with BL_engine.connect() as connection:
            result = connection.execute(query)
            result.close()
        return
    def week_end_dt(date_input):
        weekday_int=date_input.weekday()
        if weekday_int==6:
            return date_input+datetime.timedelta(days=6)
        else:
            return date_input+datetime.timedelta(days=5-weekday_int)


    # In[4]:


    high_date=datetime.datetime.strptime(dict_config['crm_end_date'],"%Y-%m-%d").date()
    if dict_config['recent_n_month']:
        recent_n_month=dict_config['recent_n_month']
        pos_start_date_id_filter = str(high_date-datetime.timedelta(days=int(np.ceil(365*recent_n_month/12))))
    else:
        pos_start_date_id_filter = dict_config["pos_start_date"]

    sql_str_high_date="'%s'"%str(high_date)
    sql_str_lastweekstart_date="'%s'"%str(high_date-datetime.timedelta(days=6))
    # sql_sign_up_start_date="'%s'"%str(sign_up_start_date)
    sql_POS_start_date="'%s'"%str(pos_start_date_id_filter)
    str_week_end_d=str(high_date).replace("-","")
    print("check point 1")


    path_store_list=glob.glob(folder_store_list+"*.txt")
    path_store_list.sort()
    path_store_list_ahead=[x for x in path_store_list if "MediaStormStores%s"%str_week_end_d[:6] in x][0]
    # updated 2020-10-03
    str_month_after=(datetime.datetime.strptime(str_week_end_d, '%Y%m%d') + relativedelta(months=1)).date()
    str_month_after=str(str_month_after).replace("-","")
    # path_store_list_after=[x for x in path_store_list if "MediaStormStores%s"%str_month_after in x][0]

    df_store_list=pd.read_csv(path_store_list_ahead,sep="|")
    df_store_list=df_store_list[['location_id','address_line_1','address_line_2','city_nm','state_nm','zip_cd','latitude_meas','longitude_meas']]
    df_store_list['latitude_meas']=df_store_list['latitude_meas'].astype(float)
    df_store_list['longitude_meas']=df_store_list['longitude_meas'].astype(float)
    df_store_list['zip_cd']=df_store_list['zip_cd'].apply(lambda x: x.split("-")[0].zfill(5))
    df_store_list=df_store_list[~df_store_list['location_id'].isin(['145','6990'])]
    df_store_list['location_id']=df_store_list['location_id'].astype(str)


    # In[5]:


    pos_start_date_id_filter


    # In[6]:


    # 
    TA_zips=pd.ExcelFile(path_TA_excel)
    TA_zips=TA_zips.parse("view_by_store",dtype=str)

    df_temporary=TA_zips[['location_id','trans_P_zips_70_within_TA','trans_S_zips_70_within_TA','zips_in_10']]
    df_zip_by_store=pd.DataFrame()

    for ind,row in df_temporary.iterrows():
        location_id=str(row['location_id'])
        P_zips=eval(row['trans_P_zips_70_within_TA'])
        S_zips=eval(row['trans_S_zips_70_within_TA'])
        zip_10=eval(row['zips_in_10'])


        df_P=pd.DataFrame(zip([location_id]*len(P_zips),P_zips))
        if len(df_P)>0:
            df_P.columns=['location_id','zip_cd']
            df_P['zip_type']="P"

        df_S=pd.DataFrame(zip([location_id]*len(S_zips),S_zips))
        if len(df_S)>0:
            df_S.columns=['location_id','zip_cd']
            df_S['zip_type']="S"

        df_10=pd.DataFrame(zip([location_id]*len(zip_10),zip_10))
        if len(df_10)>0:
            df_10.columns=['location_id','zip_cd']
            df_10['zip_type']="zip_10"

        df_zip_by_store=df_zip_by_store.append(df_P).append(df_S).append(df_10)
    df_zip_by_store['location_id']=df_zip_by_store['location_id'].astype(str)
    df_store_list=df_store_list[['location_id','latitude_meas','longitude_meas']]
    df_store_zip=pd.merge(df_store_list,df_zip_by_store,on="location_id",how="left")
    df_store_zip_new=df_store_zip[pd.isnull(df_store_zip['zip_cd'])]
    df_store_zip_existing=df_store_zip[pd.notnull(df_store_zip['zip_cd'])]

    df_store_zip_new_no_loc=df_store_zip_new[df_store_zip_new['latitude_meas']==0]
    df_store_zip_new_with_loc=df_store_zip_new[df_store_zip_new['latitude_meas']!=0]
    df_store_zip_new_with_loc=df_store_zip_new_with_loc[['location_id','latitude_meas','longitude_meas']]
    df_store_zip_new_no_loc=df_store_zip_new_no_loc[['location_id','latitude_meas','longitude_meas']]
    if len(df_store_zip_new_no_loc)>0:
        store_list_later=[x for x in path_store_list if x.split("MediaStormStores")[1][:6]>str_week_end_d]
        store_list_later=sorted(store_list_later,key=lambda x: os.stat(x).st_mtime)
        for file in store_list_later:
            df=pd.read_csv(file,dtype=str,sep="|",usecols=['location_id','latitude_meas','longitude_meas'])
            df=df[['location_id','latitude_meas','longitude_meas']]
            df['latitude_meas']=df['latitude_meas'].astype(float)
            df['longitude_meas']=df['longitude_meas'].astype(float)
            df['location_id']=df['location_id'].astype(str)
            df=df[df['location_id'].isin(df_store_zip_new_no_loc['location_id'].tolist())]
            df=df[df['latitude_meas']!=0]
            df_store_zip_new_with_loc=df_store_zip_new_with_loc.append(df)
            df_store_zip_new_no_loc=df_store_zip_new_no_loc[~df_store_zip_new_no_loc['location_id'].isin(df['location_id'].tolist())]
            if len(df_store_zip_new_no_loc)==0:
                break
        df_store_zip_new=df_store_zip_new_with_loc.reset_index()
        del df_store_zip_new['index']
        if len(df_store_zip_new_with_loc)>0:
            del df_store_zip_new_with_loc
        if len(df_store_zip_new_no_loc)>0:
            del df_store_zip_new_no_loc

    zip_centers=json.load(open(path_json_zip_center,"r"))
    if len(df_store_zip_new)>0:


        df_all_new_zip=pd.DataFrame()
        for i,row in df_store_zip_new.iterrows():
            store_coor=(row['latitude_meas'],row['longitude_meas'])
            store_num=row['location_id']
            list_store_zip=[]
            for zip_cd, v in zip_centers.items():
                dist=haversine(store_coor,v,unit="mi")
                if dist<=10:
                    list_store_zip.append(zip_cd)
            df=pd.DataFrame({"zip_cd":list_store_zip,"zip_type":["zip_10"]*len(list_store_zip)},index=[store_num]*len(list_store_zip))
            df=df.reset_index().rename(columns={"index":"location_id"})
            df_all_new_zip=df_all_new_zip.append(df)

        df_store_zip_new=pd.merge(df_store_zip_new,df_all_new_zip,on="location_id",how="left")

        df_store_zip=df_store_zip_existing.append(df_store_zip_new)
    else:
        df_store_zip=df_store_zip_existing
    df_zip_type=df_store_zip[['zip_cd','zip_type']].drop_duplicates()
    df_zip_type=df_zip_type.sort_values(['zip_cd','zip_type'])
    print(df_zip_type['zip_type'].unique().tolist())
    df_unique_zip_type=df_zip_type.drop_duplicates("zip_cd")

    list_P_zips=df_zip_type[df_zip_type['zip_type']=="P"]['zip_cd'].tolist()
    list_S_zips=df_zip_type[df_zip_type['zip_type']=="S"]['zip_cd'].tolist()
    list_10_zips=df_zip_type[df_zip_type['zip_type']=="zip_10"]['zip_cd'].tolist()

    df_store_list=df_store_zip[['location_id','latitude_meas','longitude_meas']].drop_duplicates().reset_index()
    del df_store_list['index']
    df_store_list=df_store_zip[['location_id','latitude_meas','longitude_meas']].drop_duplicates().reset_index()
    del df_store_list['index']
    # 
    print("check point 2")


    # In[7]:


    processors=20

    list_all_zips=list(zip_centers.keys())
    len_chunck=int(np.ceil(len(list_all_zips)/processors))
    list_of_input_all_us_zip_list=[]

    for i in range(processors):
        l=list_all_zips[i*len_chunck:(i+1)*len_chunck]
        list_of_input_all_us_zip_list.append(l)

    p = Pool(processors)
    result=p.starmap(get_dist_output_df, zip(list_of_input_all_us_zip_list, repeat(df_store_list), repeat(zip_centers)))
    ## result=p.map(get_dist_output_df, list_of_input_all_us_zip_list)
    # get_dist_output_df defined in the main py file, due to the thread need to be defined top-level
    df_zips_with_BL_store=pd.DataFrame()
    for res in result:
        if res is not None:
            df_zips_with_BL_store=df_zips_with_BL_store.append(res)
    p.close()
    p.join()
    print("check point 3")


    print(df_zips_with_BL_store.shape,df_zips_with_BL_store['zip_cd'].nunique(),df_zips_with_BL_store['nearest_BL_store'].nunique())
    df_zips_with_BL_store['zip_cd']=df_zips_with_BL_store['zip_cd'].astype(str)
    df_zips_with_BL_store['zip_cd']=df_zips_with_BL_store['zip_cd'].apply(lambda x: x.zfill(5))


    # In[8]:


    # IVs

    # distance to sign up location is not a good contributor in the model, so no need to include the part
    print(datetime.datetime.now())
    df_1=pd.read_sql("select t1.customer_id_hashed, sign_up_channel, sign_up_location, customer_zip_code, t1.sign_up_date from BL_Rewards_Master as t1 right join %s as t2 on t1.customer_id_hashed=t2.customer_id_hashed;"%table_filtered_crm, con=BL_engine)
    df_1=df_1.sort_values("sign_up_date",ascending=False)
    df_1=df_1.drop_duplicates("customer_id_hashed")


    df_1_len=df_1.shape[0]
    df_1_id_nunique=df_1['customer_id_hashed'].nunique()
    print("df_1_len",df_1_len)
    print("df_1_id_nunique",df_1_id_nunique)
    print(datetime.datetime.now())

    df_1['customer_zip_code']=df_1['customer_zip_code'].astype(str)
    df_1['customer_zip_code']=df_1['customer_zip_code'].apply(lambda x: x.split("-")[0].split(" ")[0].zfill(5)[:5])
    # df_1['sign_up_date']=pd.to_datetime(df_1['sign_up_date'],format="%Y-%m-%d").dt.date
    # df_1['weeks_since_sign_up']=df_1['sign_up_date'].apply(lambda x: int(np.ceil((high_date-x).days/7)))
    df_1['P_zip']=np.where(df_1['customer_zip_code'].isin(list_P_zips),1,0)
    df_1['S_zip']=np.where(df_1['customer_zip_code'].isin(list_S_zips),1,0)
    df_1['else_10_zip']=np.where(df_1['customer_zip_code'].isin(list_10_zips),1,0)
    # del df_1['customer_zip_code']
    df_1['signed_online']=np.where(df_1['sign_up_channel']=="STORE",0,1)
    del df_1['sign_up_channel']

    df_1['sign_up_location']=df_1['sign_up_location'].fillna("-1")
    df_1['sign_up_location']=df_1['sign_up_location'].astype(float)
    df_1['sign_up_location']=df_1['sign_up_location'].astype(int).astype(str)
    print("check point 4")

    '''
    df_copy_sign_up=df_1[['sign_up_location','customer_zip_code']].drop_duplicates()
    df_copy_sign_up=df_copy_sign_up.reset_index()
    del df_copy_sign_up['index']

    # In[7]:


    # distance to sign up stores
    df_store_all=pd.DataFrame(columns=['location_id','latitude_meas','longitude_meas'])

    list_all_stores=glob.glob(folder_store_list+"*.txt")
    list_all_stores=[x for x in list_all_stores if "MediaStormStores" in x]
    list_all_stores=sorted(list_all_stores,key=lambda x :x.split("MediaStormStores")[1][:8])
    list_all_stores=[x for x in list_all_stores if x.split("MediaStormStores")[1][:8]<=str(high_date+datetime.timedelta(days=2)).replace("-","")]
    list_all_stores.reverse()

    for file in list_all_stores:
        df=pd.read_table(file,dtype=str,sep="|",usecols=['location_id','latitude_meas','longitude_meas'])
        df=df[['location_id','latitude_meas','longitude_meas']]
        df['latitude_meas']=df['latitude_meas'].astype(float)                   
        df['longitude_meas']=df['longitude_meas'].astype(float)   
        df=df[~df['location_id'].isin(['145','6990'])]
        df=df[~df['location_id'].isin(df_store_all['location_id'].tolist())]
        df_store_all=df_store_all.append(df)
    df_store_all['store_coor']=df_store_all[['latitude_meas','longitude_meas']].values.tolist()                      
    dict_store_all=df_store_all.set_index("location_id").to_dict()['store_coor']
    df_copy_sign_up['distc_to_sign_up']=np.nan
    for i,row in df_copy_sign_up.iterrows():
        try:
            store_coor=dict_store_all[row['sign_up_location']]
            zip_center=zip_centers[row['customer_zip_code']]
            dist=haversine(store_coor,zip_center,unit="mi")
            df_copy_sign_up.loc[i,"distc_to_sign_up"]=dist

        except:
            continue
    df_1=pd.merge(df_1,df_copy_sign_up,on=['sign_up_location','customer_zip_code'],how="left")
    print("check point 5")
    '''


    # In[9]:


    #
    list_unsub=glob.glob(folder_email_unsub+"*.csv")
    df_unsub_files=pd.DataFrame({"file_path":list_unsub})
    df_unsub_files['date']=df_unsub_files['file_path'].apply(lambda x: x.split("ile_Refresh__")[1][:8])
    df_unsub_files['date']=pd.to_datetime(df_unsub_files['date']).dt.date
    df_unsub_files['day_diff']=abs(df_unsub_files['date']-high_date)
    path_unsub=df_unsub_files[df_unsub_files['day_diff']==df_unsub_files['day_diff'].min()]['file_path'].values.tolist()[0]
    ###### 
    list_unsunsribe_ids=pd.read_csv(path_unsub,
                             dtype=str,usecols=['customersummary_c_primaryscnhash'])['customersummary_c_primaryscnhash'].unique().tolist()

    print(len(list_unsunsribe_ids))
    df_1['email_unsub_label']=np.where(df_1['customer_id_hashed'].isin(list_unsunsribe_ids),1,0)
    del list_unsunsribe_ids
    df_zips_with_BL_store=df_zips_with_BL_store.rename(columns={"zip_cd":"customer_zip_code"})
    df_1=pd.merge(df_1,df_zips_with_BL_store,on="customer_zip_code",how="left")
    df_1=df_1.reset_index()
    del df_1['index']
    df_1=df_1.reset_index()
    del df_1['index']
    df_1=df_1.reset_index()


    # In[10]:


    path_unsub


    # In[11]:


    # Changed the DV to be pulled in the Predictive Running part, due to the need to apply 2+ and 1 only sepecreately
    # Design as here:
    # today is 2020-12-19, and the latest date in the data is 2020-12-12 (new week not received at this moment)
    # DV -- cumulative 2 weeks -- 2+ buyers, is the range between 2020-11-29 to 2020-12-12
    # DV -- cumulative 3 weeks -- 1 only buyers, is the range between 2020-11-22 to 2020-12-12

    # The script moved to the running part to pull seperately for the buyers
    '''
    dv_start_date=high_date+datetime.timedelta(days=1)
    dv_end_date=high_date+datetime.timedelta(days=21)

    str_sql_dv_start_date="'"+str(dv_start_date)+"'"
    str_sql_dv_end_date="'"+str(dv_end_date)+"'"
    print(str_sql_dv_start_date,str_sql_dv_end_date)
    print(datetime.datetime.now())
    df_dvs=pd.read_sql("select customer_id_hashed, transaction_dt from Pred_POS_Department where transaction_dt between %s and %s and sales >0"%(str_sql_dv_start_date,str_sql_dv_end_date),con=BL_engine).drop_duplicates()
    print(datetime.datetime.now())
    print("check point 6")


    # In[36]:


    df_dvs['week_end_dt']=df_dvs['transaction_dt'].apply(week_end_dt)
    df_dvs=df_dvs[['customer_id_hashed','week_end_dt']].drop_duplicates()
    list_unique_weeks=df_dvs['week_end_dt'].unique().tolist()
    list_unique_weeks.sort()
    df_dv_binary=df_dvs[df_dvs['week_end_dt']==list_unique_weeks[0]][['customer_id_hashed']]
    df_dv_binary['DV_cumulative_week_updated_1']=1
    for i in range(1,3):
        w=list_unique_weeks[i]
        df=df_dvs[df_dvs['week_end_dt']<=w][['customer_id_hashed']].drop_duplicates()
        df['DV_cumulative_week_updated_%d'%(i+1)]=1
        df_dv_binary=pd.merge(df_dv_binary,df,on="customer_id_hashed",how="outer")
        print(w,datetime.datetime.now())
    df_dv_binary=df_dv_binary.fillna(0)

    df_1=pd.merge(df_dv_binary,df_1,on="customer_id_hashed",how="right")

    for i in range(3):
        df_1['DV_cumulative_week_updated_%d'%(i+1)]=df_1['DV_cumulative_week_updated_%d'%(i+1)].fillna(0)
    '''


    # In[12]:


    print(df_1.shape,df_1['customer_id_hashed'].nunique())
    if "index" in df_1.columns.tolist():
        del df_1['index']

    print("check point 7")


    # In[ ]:


    # test crm is still being wrote out in case that a validation summary view in need
    table_crm_id_list_train="crm_table_id_list_train_%s"%str_week_end_d
    table_crm_id_list_test="crm_table_id_list_test_%s"%str_week_end_d
    table_df_1="table_pred_1_crm_up_to_%s"%str_week_end_d

    dict_table_names.update({"table_crm_id_list_train":table_crm_id_list_train})
    dict_table_names.update({"table_crm_id_list_test":table_crm_id_list_test})
    dict_table_names.update({"table_df_1":table_df_1})
    # split
    len_df_1=len(df_1)


    if len_df_1>train_sample_size/(1-test_ratio):
        list_ind_train=random.sample(range(len_df_1), train_sample_size)
    else:
        list_ind_train=random.sample(range(len_df_1), int(len_df_1*(1-test_ratio)))

    df_1=df_1.reset_index()
    df_1_train=df_1[['customer_id_hashed']][df_1['index'].isin(list_ind_train)]
    df_1_test=df_1[['customer_id_hashed']][~df_1['index'].isin(list_ind_train)]
    del df_1['index']


    print("df_1_train.shape",df_1_train.shape)
    print("df_1_test.shape",df_1_test.shape)
    chunksize=10**6

    dtype_id={"customer_id_hashed": sql.types.VARCHAR(length=64)}
    df_1_train.to_sql(name=table_crm_id_list_train,chunksize=chunksize,
        con=BL_engine, index=False, if_exists="replace", dtype=dtype_id)
    df_1_test.to_sql(name=table_crm_id_list_test,chunksize=chunksize,
        con=BL_engine, index=False, if_exists="replace", dtype=dtype_id)

    dtype_df_1={
    'customer_id_hashed':sql.types.VARCHAR(length=64),
    # 'DV_cumulative_week_updated_1':sql.types.Integer,
    # 'DV_cumulative_week_updated_2':sql.types.Integer,
    # 'DV_cumulative_week_updated_3':sql.types.Integer,
    # 'DV_cumulative_week_updated_4':sql.types.Integer,
    'sign_up_location':sql.types.VARCHAR(length=5),
    'customer_zip_code':sql.types.VARCHAR(length=5),
    'P_zip':sql.types.Integer,
    'S_zip':sql.types.Integer,
    'else_10_zip':sql.types.Integer,
    'signed_online':sql.types.Integer,
    'distc_to_sign_up':sql.types.Float,
    'email_unsub_label':sql.types.Integer,
    'nearest_BL_store':sql.types.VARCHAR(length=4),
    'nearest_BL_dist':sql.types.Float
    }

    df_1.to_sql(name=table_df_1,
        con=BL_engine, index=False, if_exists="replace", dtype=dtype_df_1,chunksize=chunksize)
    print("check point 8")
    create_index(table_name=table_crm_id_list_train, list_of_columns=["customer_id_hashed"])
    create_index(table_name=table_crm_id_list_test, list_of_columns=["customer_id_hashed"])
    create_index(table_name=table_df_1, list_of_columns=["customer_id_hashed"])
    # In[38]:


    path_json_table_names="./table_names_%s.json"%str(high_date).replace("-","")
    with open(path_json_table_names,"w") as json_file:
        json.dump(dict_table_names,json_file)
    print("Done_of_part_2: %s"%str(datetime.datetime.now()))

