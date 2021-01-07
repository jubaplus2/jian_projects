#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!/usr/bin/env python
# coding: utf-8

def predict_current_week():

    import sqlalchemy
    import json
    import pandas as pd
    import datetime
    import os
    import numpy as np
    import gc
    import glob

    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import train_test_split
    from sklearn.feature_selection import SelectFromModel
    from sklearn.linear_model import LogisticRegression 
    from sklearn.preprocessing import scale
    from sklearn.feature_selection import VarianceThreshold
    from sklearn.feature_selection import RFE
    from sklearn.metrics import r2_score
    from sklearn import metrics
    import matplotlib.pyplot as plt
    import sklearn
    import statsmodels.api as sm
    from statsmodels.tools.tools import add_constant
    from statsmodels.stats.outliers_influence import variance_inflation_factor

    task_start_time=datetime.datetime.now()
    print("***************\n")
    print("Start predict_with_current_week_data: %s"%str(task_start_time))


    # In[2]:


    # remove high correlated cols

    def remove_cols_with_high_coor(df_X,coorelation_threshold):
        df_coor_X=df_X.corr().abs()
        df_coor_X=df_coor_X.unstack().reset_index()
        df_coor_X.columns=['iv_1','iv_2','coor']

        df_coor_X_high=df_coor_X[df_coor_X['iv_1']!=df_coor_X['iv_2']]
        df_coor_X_high=df_coor_X_high[df_coor_X_high['coor']>coorelation_threshold]
        df_coor_X_high['high_coor_pairs']=df_coor_X_high[['iv_1','iv_2']].values.tolist()

        list_highly_pairs=df_coor_X_high['high_coor_pairs'].tolist()
        list_highly_pairs=[sorted(x) for x in list_highly_pairs]

        list_highly_pairs=[str(x) for x in list_highly_pairs]
        list_highly_pairs=list(set(list_highly_pairs))
        list_highly_pairs=[eval(x) for x in list_highly_pairs]

        list_col_keep_in_priority=['trans_in_store','total_items','total_trans_since_registration']
        list_cols_to_remove=[]
        list_cols_to_keep=[]

        def remove_p_with_v(list_all_pairs,v_remove):
            for p in list_all_pairs:
                if v_remove in p:
                    list_all_pairs.remove(p)
            return list_all_pairs
        def remaining_unique_list(list_all_pairs):
            res=[]
            for x in list_all_pairs:
                res.extend(x)
            res=list(set(res))
            return res
        def update_paired_list_in_priority(l1_to_keep_priority,l2_all_for_now,l3_remove_for_now,l4_keep_for_now):
            list_unique=remaining_unique_list(l2_all_for_now)
            for v_keep in l1_to_keep_priority:
                if v_keep in list_unique:
                    l4_keep_for_now.append(v_keep)
                    list_removed_due_to_vkeep=[]
                    for p in l2_all_for_now:
                        if v_keep in p:
                            v_remove=[x for x in p if x!=v_keep][0]
                            list_removed_due_to_vkeep.append(v_remove)
                    list_removed_due_to_vkeep=list(set(list_removed_due_to_vkeep))
                    if len(list_removed_due_to_vkeep)>0:
                        for v_remove in list_removed_due_to_vkeep:
                            l3_remove_for_now.append(v_remove)
                            l2_all_for_now=remove_p_with_v(list_all_pairs=l2_all_for_now,v_remove=v_remove)

                            if v_remove in l1_to_keep_priority:
                                l1_to_keep_priority.remove(v_remove)
            for p in l2_all_for_now:
                v1=p[0]
                v2=p[1]
                if v1 in (l3_remove_for_now) and (v2 in l3_remove_for_now):
                    l2_all_for_now.remove(p)
                elif v1 in l3_remove_for_now:
                    l2_all_for_now.remove(p)
                elif v2 in l3_remove_for_now:
                    l2_all_for_now.remove(p)

            l3_remove_for_now=list(set(l3_remove_for_now))
            l4_keep_for_now=list(set(l4_keep_for_now))
            return l2_all_for_now, l3_remove_for_now, l4_keep_for_now


        def update_paired_list_v_total(l2_all_for_now,l3_remove_for_now,l4_keep_for_now):
            list_keep_unique_total=[]
            for p in l2_all_for_now:
                for v in p:
                    if "total" in v:
                        list_keep_unique_total.append(v)
            list_keep_unique_total=list(set(list_keep_unique_total))
            for v_keep in list_keep_unique_total:
                list_removed_due_to_vkeep=[]
                for p in l2_all_for_now:
                    if v_keep in p:
                        v_remove=[x for x in p if x!=v_keep][0]
                        list_removed_due_to_vkeep.append(v_remove)
                list_removed_due_to_vkeep=list(set(list_removed_due_to_vkeep))
                if len(list_removed_due_to_vkeep)>0:
                    for v_remove in list_removed_due_to_vkeep:
                        l3_remove_for_now.append(v_remove)
                        l2_all_for_now=remove_p_with_v(list_all_pairs=l2_all_for_now,v_remove=v_remove)

                        if v_remove in list_keep_unique_total:
                            list_keep_unique_total.remove(v_remove)

            for p in l2_all_for_now:
                v1=p[0]
                v2=p[1]
                if v1 in (l3_remove_for_now) and (v2 in l3_remove_for_now):
                    l2_all_for_now.remove(p)
                elif v1 in l3_remove_for_now:
                    l2_all_for_now.remove(p)
                elif v2 in l3_remove_for_now:
                    l2_all_for_now.remove(p)
            l3_remove_for_now=list(set(l3_remove_for_now))
            l4_keep_for_now.extend(list_keep_unique_total)
            return l2_all_for_now, l3_remove_for_now, l4_keep_for_now

        def remove_remaining_arbitrary(l2_all_for_now,l3_remove_for_now):
            list_remove_arbitrary=[]
            if len(l2_all_for_now)==0:
                return l2_all_for_now,l3_remove_for_now
            while len(l2_all_for_now)>0:
                for p in l2_all_for_now:
                    v_remove=p[0]
                    list_remove_arbitrary.append(v_remove)
                    l2_all_for_now.remove(p)
                    for p2 in l2_all_for_now:
                        if v_remove in p2:
                            l2_all_for_now.remove(p2)
            l3_remove_for_now.extend(list_remove_arbitrary)
            l3_remove_for_now=list(set(l3_remove_for_now))
            return l2_all_for_now,l3_remove_for_now 

        list_highly_pairs, list_cols_to_remove, list_cols_to_keep=update_paired_list_in_priority(l1_to_keep_priority=list_col_keep_in_priority,
                                                     l2_all_for_now=list_highly_pairs,
                                                     l3_remove_for_now=list_cols_to_remove,
                                                     l4_keep_for_now=list_cols_to_keep
                                                    )

        list_highly_pairs, list_cols_to_remove, list_cols_to_keep=update_paired_list_v_total(
                                                         l2_all_for_now=list_highly_pairs,
                                                         l3_remove_for_now=list_cols_to_remove,
                                                         l4_keep_for_now=list_cols_to_keep
                                                        )

        list_highly_pairs,list_cols_to_remove=remove_remaining_arbitrary(l2_all_for_now=list_highly_pairs,
                                                                         l3_remove_for_now=list_cols_to_remove)

        for col in list_cols_to_remove:
            del df_X[col]
            print(col, "removed due to high coor with others")
        return df_X


    # In[3]:


    def scoring_2_trans_plus_thresh(tp,tn,fp,fn,pctg):
        total=sum([tp,tn,fp,fn])
        accuracy=(tp+tn)/total
        ppv=tp/(tp+fp) # positive predict value
        fdr=fp/(tp+fp) # false discover rate
        fpr=fp/(tn+fp) # false positive rate

        TPR=tp/(tp+fn) #recall
        PPV=tp/(tp+fp) #precission
        f1_score = 2*(TPR*PPV)/(TPR+PPV)

        score=(9*tp-8*fn*(1-f1_score)-fp)*accuracy
        # the score ignored the f1 and overall accuracy due to low pctg



        # consider the profit vs lost 10:1 (30%*$30) vs (cpc*frequecy or click)
        # which means 1 missed (fn) is 10 times of 1 wrong targeted (fp)
        # very aribitury
        return score

    def scoring_1_trans_only_thresh(tp,tn,fp,fn,pctg):
        total=sum([tp,tn,fp,fn])
        accuracy=(tp+tn)/total
        ppv=tp/(tp+fp) # positive predict value
        fdr=fp/(tp+fp) # false discover rate
        fpr=fp/(tn+fp) # false positive rate

        TPR=tp/(tp+fn) #recall
        PPV=tp/(tp+fp) #precission

        r_1=tn/(tn+fp)
        r_2=tp/(tp+fn)

        # consider the 2 pctgs that matter
        score=(1+r_1)*(1+r_2*(1+pctg))
        '''
        score=tp*4*(1-pctg*3)-fn*pctg*4-fp*0.1*(1-pctg)+tn*pctg*0.025 # just work for this
        # 4=8*0.5 as the benefit * the posible inherit purchase rate
        # the false negative is the missed should be getting benefit, but the pctg is the one that most will be 0s
        # the false positive is only the one that spend money wrong, ~0.25 cost per reach on FB - 3 weeks, 4 times adjust
        # true negative ignored
        '''


        return score

    def write_aggregate_func_gain_chart(list_selected_features,df_pred_table_detail):
        func_dict={"customer_id_hashed":"count"}
        list_cols_for_ratios=['y_true','y_hat']
        for col in list_selected_features:
            if len(df_pred_table_detail[col].unique())==2:
                func_dict.update({col:'sum'})
                list_cols_for_ratios.append(col)
            else:
                func_dict.update({col:"mean"})
        func_dict.update({"y_true":"sum"})
        func_dict.update({"y_hat":"sum"})
        # func_dict.update({"pred_prob":['max','min']})
        return func_dict,list_cols_for_ratios


    def generate_gain_chart_function(df_X,list_y,list_ids,result_sm_model,threshold,list_selected_features):
        df_X['pred_prob']=result_sm_model.predict(sm.add_constant(df_X)).values
        df_X['customer_id_hashed']=list_ids
        df_X['decile']=pd.qcut(df_X['pred_prob'], 10, labels=False)
        df_X['decile']=df_X['decile'].apply(lambda x: "D"+str(10-x).zfill(2))
        df_X['y_true']=list_y
        df_X['y_hat']=np.where(df_X['pred_prob']>threshold,1,0)

        agg_func,list_cols_to_get_ratio=write_aggregate_func_gain_chart(list_selected_features,df_X)
        df_gainchart=df_X.groupby("decile")[['customer_id_hashed']+list_selected_features+['y_true', 'y_hat']].agg(agg_func).reset_index()

        df_prob_max=df_X.groupby("decile")['pred_prob'].max().to_frame().reset_index().rename(columns={"pred_prob":"max_prob"})
        df_prob_min=df_X.groupby("decile")['pred_prob'].min().to_frame().reset_index().rename(columns={"pred_prob":"min_prob"})
        df_gainchart=pd.merge(df_gainchart,df_prob_max,on="decile")
        df_gainchart=pd.merge(df_gainchart,df_prob_min,on="decile")
        df_gainchart.insert(2,"actual_ratio",df_gainchart['y_true']/df_gainchart['customer_id_hashed'])
        df_gainchart.insert(3,"pred_ratio",df_gainchart['y_hat']/df_gainchart['customer_id_hashed'])

        df_gainchart.insert(4,"max_pred_prob",df_gainchart['max_prob'])
        df_gainchart.insert(5,"min_pred_prob",df_gainchart['min_prob'])
        del df_gainchart['max_prob']
        del df_gainchart['min_prob']
        del df_X['customer_id_hashed']
        del df_X['pred_prob']
        del df_X['y_true']
        del df_X['y_hat']

        return df_gainchart


    # In[4]:


    print(sklearn.__version__)
    print("Job start: model building", datetime.datetime.now())

    dict_config=json.load(open("/mnt/clients/juba/hqjubaapp02/jliang/Projects/Big_Lots/Predictive_Model/Model_Scripts/config.json"))
    high_date=dict_config['pos_end_date']
    # dict_tables=json.load(open("/mnt/clients/juba/hqjubaapp02/jliang/Projects/Big_Lots/Predictive_Model/Model_Scripts/table_names_%s.json"%str(high_date).replace("-","")))


    # In[5]:


    '''
    high_date=dict_config['pos_end_date'] # DV high date
    username=dict_config['username']
    password=dict_config['password']
    database=dict_config['database']
    recent_n_month=dict_config['recent_n_month']
    path_dept_name=dict_config["path_dept_name"]
    model_output_folder=dict_config['model_output_folder']
    '''


    # In[6]:


    dict_config


    # In[7]:


    class SM_Logistic_Model_dvN:
        # 1
        def __init__(self,n_week_DV,key_df_type,dict_config=dict_config):
            func_name="__init__"
            start_time=datetime.datetime.now()
            print('start -- %s: %s'%(func_name,str(start_time)))  

    # n_week_DV: 1-4
    # key_df_type: "trans_1_only" or "trans_2_plus" in the keys of dict_df_type
    # df_date_range: defined global df -- df_date_range
    # sql_engine: mysql engine to BigLots database

            high_date=dict_config['pos_end_date'] # DV high date
            username=dict_config['username']
            password=dict_config['password']
            database=dict_config['database']
            self.recent_n_month=dict_config['recent_n_month']
            path_dept_name=dict_config["path_dept_name"]
            model_output_folder=dict_config['model_output_folder']
            self.folder_email_unsub=dict_config['folder_email_unsub']
            self.sql_engine=sqlalchemy.create_engine("mysql+pymysql://%s:%s@localhost/%s" % (username, password, database))
            
            json_file_IV_table=""
            self.table_df_1_train=""
            self.table_2_1_train=""
            self.table_2_2_train=""
            self.table_0_train=""

            self.n_week_DV=n_week_DV
            self.key_df_type=key_df_type
            
            self.df_train_X=pd.DataFrame()
            self.X_train_scaled=pd.DataFrame()
            self.input_y_train_list=[]
            self.list_ids_y_train=[]

            self.X_features=self.df_train_X.columns.tolist()

            self.df_test_X=pd.DataFrame()
            self.input_y_test_list=[]
            self.list_ids_y_test=[]

            self.db_row_counts=pd.DataFrame()
            self.df_y_train_count=pd.DataFrame()
            self.df_y_test_count=pd.DataFrame()
            self.pctg=None
            self.threshold_max_selfdefinedscore=None
            self.df_step_table=pd.DataFrame()
            self.df_confusion_table=pd.DataFrame()
            self.df_gainchart_train=pd.DataFrame()
            self.df_gainchart_test=pd.DataFrame()

            self.df_train_ids_labeled_summary=pd.DataFrame()
            self.df_test_ids_labeled_summary=pd.DataFrame()

            self.df_train_ids_labeled=pd.DataFrame()
            self.df_test_ids_labeled=pd.DataFrame()

            self.output_folder=model_output_folder+"output_LastWeek_NoDCM_%s_%s/"%(high_date,str(datetime.datetime.now().date()))
            try:
                os.stat(self.output_folder)
            except:
                os.mkdir(self.output_folder)

            self.output_path=self.output_folder+"BL_LRModeling_NoDCM_%s_DV%s_%s_JL_%s.xlsx"%(key_df_type,str(n_week_DV),high_date,str(datetime.datetime.now().date()))
            self.df_department_name=pd.read_table(path_dept_name,sep="|").drop_duplicates()
            
            if key_df_type=="trans_1_only":
                date_high_date=datetime.datetime.strptime(high_date,"%Y-%m-%d").date()
                self.DV_high_date=date_high_date
                self.IV_high_date=datetime.datetime.strptime(high_date,"%Y-%m-%d").date()-datetime.timedelta(days=21)
                self.str_IV_high_date=str(self.IV_high_date)
                self.str_8_digit_IV_high_date=self.str_IV_high_date.replace("-","")
                self.sql_str_DV_start="'"+str(date_high_date-datetime.timedelta(days=20))+"'"
                self.sql_str_DV_end="'"+str(high_date)+"'"
                
            elif key_df_type=="trans_2_plus":
                date_high_date=datetime.datetime.strptime(high_date,"%Y-%m-%d").date()
                self.DV_high_date=date_high_date
                self.IV_high_date=datetime.datetime.strptime(high_date,"%Y-%m-%d").date()-datetime.timedelta(days=14)
                self.str_IV_high_date=str(self.IV_high_date)
                self.str_8_digit_IV_high_date=self.str_IV_high_date.replace("-","")
                self.sql_str_DV_start="'"+str(date_high_date-datetime.timedelta(days=13))+"'"
                self.sql_str_DV_end="'"+str(high_date)+"'"
            if self.recent_n_month:
                self.pos_start_date_id_filter = str(pd.to_datetime(self.IV_high_date).date()-datetime.timedelta(days=int(np.ceil(365*self.recent_n_month/12))))
            else:
                self.pos_start_date_id_filter = dict_config["pos_start_date"]
            
            self.df_date_range=pd.DataFrame({"IV":[self.pos_start_date_id_filter,str(self.IV_high_date)],
                                             "DV":[self.sql_str_DV_start.replace("'",""),self.sql_str_DV_end.replace("'","")]
                                            },index=['start','end'])
            
            file_json_table="/mnt/clients/juba/hqjubaapp02/jliang/Projects/Big_Lots/Predictive_Model/Model_Scripts/table_names_%s.json"%str(self.IV_high_date).replace("-","")
            self.dict_tables_for_IV=json.load(open(file_json_table,"r"))
            
            
            self.dict_tables_for_current_week={}
            self.df_target_X=pd.DataFrame()
            self.list_ids_y_target=[]
            end_time=datetime.datetime.now() 
            
            # add two list of email unsub IDs
            list_unsubsription_files=glob.glob(self.folder_email_unsub+"*.csv")
            list_unsubsription_files=[x for x in list_unsubsription_files if "iber_File_Refresh__" in x]
            list_unsubsription_files.sort()
            df_unsub_files=pd.DataFrame({"file_path":list_unsubsription_files})
            df_unsub_files['date']=df_unsub_files['file_path'].apply(lambda x: x.split("ile_Refresh__")[1][:8])
            df_unsub_files['date']=pd.to_datetime(df_unsub_files['date']).dt.date
            
            df_unsub_files['day_diff_IV']=abs(df_unsub_files['date']-self.IV_high_date)
            self.path_unsub_IV=df_unsub_files[df_unsub_files['day_diff_IV']==df_unsub_files['day_diff_IV'].min()]['file_path'].values.tolist()[0]
            
            df_unsub_files['day_diff_DV']=abs(df_unsub_files['date']-self.DV_high_date)
            self.path_unsub_DV=df_unsub_files[df_unsub_files['day_diff_DV']==df_unsub_files['day_diff_DV'].min()]['file_path'].values.tolist()[0]
            
            print('Done -- %s: %s'%(func_name,str(end_time)))
            print("%s\n"%str(end_time-start_time))
                
        # 2 
        def pull_train_df_X(self):
            func_name="pull_train_df_X"
            start_time=datetime.datetime.now()
            print('start -- %s: %s'%(func_name,str(start_time)))  

            self.table_df_1_train=self.dict_tables_for_IV['table_df_1']
            self.table_2_1_train=self.dict_tables_for_IV['table_2_1']
            self.table_2_2_train=self.dict_tables_for_IV['table_2_2']
            self.table_0_train=self.dict_tables_for_IV['table_crm_id_list_train']
            
            
            col_list_df0_train=pd.read_sql("desc %s"%self.table_0_train,con=self.sql_engine)['Field'].values.tolist()
            col_list_df_1_train=pd.read_sql("desc %s"%self.table_df_1_train,con=self.sql_engine)['Field'].values.tolist()
            col_list_2_1_train=pd.read_sql("desc %s"%self.table_2_1_train,con=self.sql_engine)['Field'].values.tolist()
            col_list_2_2_train=pd.read_sql("desc %s"%self.table_2_2_train,con=self.sql_engine)['Field'].values.tolist()
            
            cols_no_need=['sign_up_location','customer_zip_code','nearest_BL_store','distc_to_sign_up','nearest_BL_dist',
                          'week_1st_trans','week_recent_one_trans','week_recent_two_trans',
                          'DV_cumulative_week_updated_1','DV_cumulative_week_updated_2','DV_cumulative_week_updated_3']
            for col_remove in cols_no_need:
                col_list_df_1_train=[x for x in col_list_df_1_train if x != col_remove and (x not in ["customer_id_hashed", "sign_up_date"])]
                col_list_2_1_train=[x for x in col_list_2_1_train if x != col_remove and x!="id"]
                col_list_2_2_train=[x for x in col_list_2_2_train if x != col_remove and x!="id"]
            sql_str_cols_df0_train=str(["t0."+x for x in col_list_df0_train]).replace("'","")[1:-1]  
            sql_str_cols_df_1_train=str(["t1."+x for x in col_list_df_1_train]).replace("'","")[1:-1]
            sql_str_cols_2_1_train=str(["t2_1."+x for x in col_list_2_1_train]).replace("'","")[1:-1]
            sql_str_cols_2_2_train=str(["t2_2."+x for x in col_list_2_2_train]).replace("'","")[1:-1]
            sql_str_cols_all=", ".join([sql_str_cols_df0_train,sql_str_cols_df_1_train,sql_str_cols_2_1_train,sql_str_cols_2_2_train])
            
            queary="SELECT %s from %s as t0         left join %s as t1 on t0.customer_id_hashed=t1.customer_id_hashed         left join %s as t2_1 on t0.customer_id_hashed=t2_1.id         left join %s as t2_2 on t0.customer_id_hashed=t2_2.id"%(sql_str_cols_all,self.table_0_train,self.table_df_1_train,
                                                                    self.table_2_1_train,self.table_2_2_train)
            print(queary)
            df_train_X=pd.read_sql(queary,con=self.sql_engine)
            print(df_train_X.shape)
            
            if self.key_df_type=="trans_1_only":
                self.df_train_X=df_train_X[pd.isnull(df_train_X['total_sales_recent_two_trans'])]
            elif self.key_df_type=="trans_2_plus":
                self.df_train_X=df_train_X[~pd.isnull(df_train_X['total_sales_recent_two_trans'])]
                
            self.db_row_counts=pd.DataFrame({"records":self.df_train_X.shape[0],"IVs":self.df_train_X.shape[1]},index=["X_train"])
            # To add two rows below from 3.2 


            end_time=datetime.datetime.now() 
            print('Done -- %s: %s'%(func_name,str(end_time)))
            print("%s\n"%str(end_time-start_time))
            
        # 3    
        def clean_train_univariate(self,r_variance=0.95):
            func_name="clean_train_univariate"
            start_time=datetime.datetime.now()
            print('start -- %s: %s'%(func_name,str(start_time)))  
            
            # 3.1
            list_cols=self.df_train_X.columns.tolist()
            for col in list_cols:
                if self.df_train_X[col].nunique()<=1:
                    del self.df_train_X[col]
                    print(datetime.datetime.now(),self.key_df_type,"col_nunique<=1 dropped: %s"%col)
                    
            self.df_train_X=self.df_train_X.T.drop_duplicates().T
            print(datetime.datetime.now(),"done self.df_train.T.drop_duplicates().T")
            
            # 3.2
            for col in self.df_train_X.columns.tolist():
                df_na=self.df_train_X[pd.isnull(self.df_train_X[col])]
                if df_na.shape[0]>0:
                    print("Warning: nan detected in the self.df_train col: %s"%col)

            ### These 2 rows below can be moved above into function 2
            self.list_ids_y_train=self.df_train_X['customer_id_hashed'].tolist() #-- to move up
            del self.df_train_X['customer_id_hashed'] #-- to move up

            # 3.3
            threshold_variance_iv=r_variance*(1-r_variance)
            selector = VarianceThreshold(threshold=threshold_variance_iv)
            df_redused_X=selector.fit_transform(self.df_train_X)
            print("self.df_train_X reduced to the shape due to %s variante"%(str(r_variance)),df_redused_X.shape)
            indices = [i for i, x in enumerate(list(selector.get_support())) if x == True]
            self.df_train_X=self.df_train_X.iloc[:,indices]        
            end_time=datetime.datetime.now() 
            print('Done -- %s: %s'%(func_name,str(end_time)))
            print("%s\n"%str(end_time-start_time))
            
        # 4
        def pull_train_df_Y(self):
            func_name="pull_train_df_Y"
            start_time=datetime.datetime.now()
            print('start -- %s: %s'%(func_name,str(start_time)))  
            query="select distinct customer_id_hashed as buyers from Pred_POS_Department where sales>=0 and transaction_dt between %s and %s;"%(self.sql_str_DV_start,self.sql_str_DV_end)
            print(query)
            self.df_buyers=pd.read_sql(query,con=self.sql_engine)
            df_train_ids=pd.DataFrame({"custoemr_id_hashed":self.list_ids_y_train},index=range(len(self.list_ids_y_train)))
            df_train_ids=pd.merge(df_train_ids,self.df_buyers,left_on="custoemr_id_hashed",right_on="buyers",how="left")
            df_train_ids['y_true']=np.where(pd.isnull(df_train_ids['buyers']),0,1)
            self.input_y_train_list=df_train_ids['y_true'].tolist()
            end_time=datetime.datetime.now() 
            print('Done -- %s: %s'%(func_name,str(end_time)))
            print("%s\n"%str(end_time-start_time))
                
        # 5
        def remove_correlated_cols(self,coorelation_threshold=0.8):
            func_name="remove_correlated_cols"
            start_time=datetime.datetime.now()
            print('start -- %s: %s'%(func_name,str(start_time)))  
            for col in self.df_train_X.columns.tolist():
                self.df_train_X[col]=self.df_train_X[col].astype(float)
            self.df_train_X=remove_cols_with_high_coor(df_X=self.df_train_X,coorelation_threshold=coorelation_threshold)
            end_time=datetime.datetime.now() 
            print('Done -- %s: %s'%(func_name,str(end_time)))
            print("%s\n"%str(end_time-start_time))
            
        # 6
        def select_from_model_n_features(self, N_feature_select_from_models):
            func_name="select_from_model_n_features"
            start_time=datetime.datetime.now()
            print('start -- %s: %s'%(func_name,str(start_time)))  
            
            self.X_train_scaled=scale(self.df_train_X)
            print('start',datetime.datetime.now(),"select_from_model_n_features")
            print("Starting select_from_model_n_features: ",datetime.datetime.now())
            selector = SelectFromModel(estimator=LogisticRegression(random_state=0,
                                                                    solver="saga",
                                                                    max_iter=2000,
                                                                    n_jobs=24,
                                                                    tol=0.0001),
                                       max_features=N_feature_select_from_models,
                                       threshold=-np.inf).fit(self.X_train_scaled, self.input_y_train_list)

            print("selector.threshold_",selector.threshold_)
            selector_support_FROMMODEL=selector.get_support()

            self.X_features=[self.df_train_X.columns.tolist()[i] for i,v in enumerate(selector_support_FROMMODEL) if v==True]

            self.df_train_X=self.df_train_X.loc[:,self.X_features]

            print("df_train_X.shape",self.df_train_X.shape)

            self.X_train_scaled=scale(self.df_train_X)
            print("X_train_scaled.shape",self.X_train_scaled.shape)
            end_time=datetime.datetime.now() 
            print('Done -- %s: %s'%(func_name,str(end_time)))
            print("%s\n"%str(end_time-start_time))

        # 7
        def select_RFE(self,n_features_to_select): # should be RFE -- recursive feature elimination, wrong lable name
            func_name="select_RFE"
            start_time=datetime.datetime.now()
            print('start -- %s: %s'%(func_name,str(start_time)))  
            
            estimator = LogisticRegression(fit_intercept=True,solver='saga',max_iter=2000,n_jobs=24,tol=0.001)
            selector = RFE(estimator,step=1,n_features_to_select=n_features_to_select)
            selector = selector.fit(self.X_train_scaled, self.input_y_train_list)
            selector_support_RFE=selector.support_
            print("Done select_RFE: ",datetime.datetime.now())

            self.X_features=[self.df_train_X.columns.tolist()[i] for i,v in enumerate(selector_support_RFE) if v==True]

            self.df_train_X=self.df_train_X.loc[:,self.X_features]

            print("df_train_X.shape",self.df_train_X.shape)
            self.X_train_scaled=scale(self.df_train_X)
            print("X_train_scaled.shape",self.X_train_scaled.shape)
            end_time=datetime.datetime.now() 
            print('Done -- %s: %s'%(func_name,str(end_time)))
            print("%s\n"%str(end_time-start_time))
            
        # 8
        def forwards_feature_elimination_based_on_p_and_vif(self,niter=50,method="lbfgs",p_tol=0.1,vif_tol=5):
            func_name="forwards_feature_elimination_based_on_p_and_vif"
            start_time=datetime.datetime.now()
            print('start -- %s: %s'%(func_name,str(start_time)))  
            len_x_features=self.df_train_X.shape[1]
            len_x_features_new=0
            df_x_dropped=self.df_train_X.copy()
            i_iter=0
            while len_x_features_new<len_x_features and i_iter<=100:
                i_iter+=1
                len_x_features=df_x_dropped.shape[1]
                mod=sm.Logit(self.input_y_train_list,sm.add_constant(df_x_dropped),niter=niter,method=method)
                res=mod.fit()
                table=res.summary2().tables[1]   
                X=add_constant(scale(df_x_dropped))
                list_cols=table.index.tolist()
                table["VIF Factor"] = [variance_inflation_factor(X, i) for i in range(X.shape[1])]

                max_vif=table['VIF Factor'].max()
                max_p=table['P>|z|'].max()

                if max_vif>vif_tol:
                    col_name_to_drop=table.index[table['VIF Factor']==max_vif][0]
                    del df_x_dropped[col_name_to_drop]
                    len_x_features_new=df_x_dropped.shape[1]
                    print(df_x_dropped.shape,"column %s dropped due to high vif"%col_name_to_drop)

                elif max_p>p_tol:
                    col_name_to_drop=table.index[table['P>|z|']==max_p][0]
                    del df_x_dropped[col_name_to_drop]
                    len_x_features_new=df_x_dropped.shape[1]
                    print(df_x_dropped.shape,"column %s dropped due to p value"%col_name_to_drop)
                else:
                    i_iter+=100

            self.df_train_X=df_x_dropped
            self.X_features=df_x_dropped.columns.tolist()
            self.X_train_scaled=scale(self.df_train_X)
            end_time=datetime.datetime.now() 
            print('Done -- %s: %s'%(func_name,str(end_time)))
            print("%s\n"%str(end_time-start_time))


        # 9
        def run_sm_logR_model(self):
            func_name="run_sm_logR_model"
            start_time=datetime.datetime.now()
            print('start -- %s: %s'%(func_name,str(start_time)))  
            self.sm_model=sm.Logit(self.input_y_train_list,sm.add_constant(self.df_train_X),niter=50,method="lbfgs")
            self.res_of_model=self.sm_model.fit()
            self.summary_table_over=self.res_of_model.summary2().tables[0].reset_index()
            self.summary_table_output=self.res_of_model.summary2().tables[1].reset_index()

            std=self.sm_model.exog.std(axis=0)
            std[0] = 1
            tt = self.res_of_model.t_test(np.diag(std))
            df_std_coef=tt.summary_frame()
            list_std_coefficients=df_std_coef['coef'].tolist()
            self.summary_table_output['std_coef']=list_std_coefficients

            self.list_train_pred=self.res_of_model.predict()
            # 

            coefficient_of_dermination = r2_score(self.input_y_train_list, self.list_train_pred)
            self.summary_table_over=self.summary_table_over.append(pd.DataFrame({"index":[8],0:"calculated_r_squared",1:coefficient_of_dermination},index=[8]))

            #VIF
            X=add_constant(self.X_train_scaled)
            list_cols=self.summary_table_output['index'].tolist()
            self.summary_table_output["VIF Factor"] = [variance_inflation_factor(X, i) for i in range(X.shape[1])]
            self.summary_table_output=self.summary_table_output.sort_values("std_coef")
            end_time=datetime.datetime.now() 
            print('Done -- %s: %s'%(func_name,str(end_time)))
            print("%s\n"%str(end_time-start_time))


        # 10
        def select_test_df_from_mysql_past(self,n_limit_test=None):
            # The old version that use the past available DVs in same range to generate gain_chart
            # New version updated with last week IVs which don't have actual DVs yet
            
            # total_test_count=
            # try no split first without saving test scaled X
            func_name="select_test_df_from_mysql_past"
            start_time=datetime.datetime.now()
            print('start -- %s: %s'%(func_name,str(start_time)))  
            cols_in_X=self.summary_table_output.iloc[:,0].values.tolist()
            cols_in_X.remove("const")
            self.X_features=cols_in_X
            table_name_t0=self.dict_tables_for_IV['table_crm_id_list_test']
            col_list_t0=pd.read_sql("select * from %s limit 2"%table_name_t0,con=self.sql_engine).columns.tolist()

            table_name_t1=self.dict_tables_for_IV['table_df_1']
            col_list_t1=pd.read_sql("select * from %s limit 2"%table_name_t1,con=self.sql_engine).columns.tolist()

            table_name_t2_1=self.dict_tables_for_IV['table_2_1']
            col_list_t2_1=pd.read_sql("select * from %s limit 2"%table_name_t2_1,con=self.sql_engine).columns.tolist()

            table_name_t2_2=self.dict_tables_for_IV['table_2_2']
            col_list_t2_2=pd.read_sql("select * from %s limit 2"%table_name_t2_2,con=self.sql_engine).columns.tolist()

            col_list_t1=[x for x in col_list_t1 if x in cols_in_X]
            col_list_t2_1=[x for x in col_list_t2_1 if x in cols_in_X]
            col_list_t2_2=[x for x in col_list_t2_2 if x in cols_in_X]


            sql_str_cols_df0_test=str(["t0."+x for x in col_list_t0]).replace("'","")[1:-1]
            list_query_col_list=[sql_str_cols_df0_test]
            list_tables=["t0"]
            # col_list_dv=[x for x in self.df_train_Y.columns.tolist()]
            # sql_str_cols_dv=str(["t1."+x for x in col_list_dv]).replace("'","")[1:-1]
            # list_query_col_list.append(sql_str_cols_dv)
            if len(col_list_t1)>0:
                sql_str_cols_df_1=str(["t1."+x for x in col_list_t1]).replace("'","")[1:-1]
                list_query_col_list.append(sql_str_cols_df_1)
                list_tables.append("t1")
            if len(col_list_t2_1)>0:
                sql_str_cols_2_1=str(["t2_1."+x for x in col_list_t2_1]).replace("'","")[1:-1]
                list_query_col_list.append(sql_str_cols_2_1)
                list_tables.append("t2_1")
            if len(col_list_t2_2)>0:
                sql_str_cols_2_2=str(["t2_2."+x for x in col_list_t2_2]).replace("'","")[1:-1]
                list_query_col_list.append(sql_str_cols_2_2)
                list_tables.append("t2_2")
            str_cols_test=", ".join(list_query_col_list)
            select_clause="SELECT %s from %s as t0"%(str_cols_test,table_name_t0)

            list_of_join_clause=[]
            if "t1" in list_tables:
                str_join_clause_t1="left join %s as t1 on t0.customer_id_hashed=t1.customer_id_hashed"%table_name_t1
                list_of_join_clause.append(str_join_clause_t1)
            if "t2_1" in list_tables:
                str_join_clause_t2_1="left join %s as t2_1 on t0.customer_id_hashed=t2_1.id"%table_name_t2_1
                list_of_join_clause.append(str_join_clause_t2_1)
            if "t2_2" in list_tables:
                str_join_clause_t2_2="left join %s as t2_2 on t0.customer_id_hashed=t2_2.id"%table_name_t2_2
                list_of_join_clause.append(str_join_clause_t2_2)

            if self.key_df_type=="trans_1_only":
                where_clause="where t2_2.total_sales_recent_two_trans is null"
            elif self.key_df_type=="trans_2_plus":
                where_clause="where t2_2.total_sales_recent_two_trans is not null"
            else:
                where_clause=""
                print("key_df_type not specified, please choose either trans_1_only or trans_2_plus")
            if n_limit_test:
                limit_clause="limit %i"%n_limit_test
                query_full=" ".join([select_clause]+list_of_join_clause+[where_clause]+[limit_clause]).strip() 
            else:
                query_full=" ".join([select_clause]+list_of_join_clause+[where_clause]).strip() 
                
            print(query_full)
            self.df_test_X=pd.read_sql(query_full,con=self.sql_engine)
            if "nearest_BL_dist" in self.df_test_X.columns.tolist():
                self.df_test_X=self.df_test_X[pd.notnull(self.df_test_X['nearest_BL_dist'])]
            for col in self.df_test_X.columns.tolist():
                # df_nan=df[pd.isnull(df[col])]
                if self.df_test_X[pd.isnull(self.df_test_X[col])].shape[0]>0:
                    raise ValueError("%s in the selected test df is null"%col)       

            self.list_ids_y_test=self.df_test_X['customer_id_hashed'].values.tolist()
            del self.df_test_X['customer_id_hashed']

            df_test_ids=pd.DataFrame({"custoemr_id_hashed":self.list_ids_y_test},index=range(len(self.list_ids_y_test)))
            df_test_ids=pd.merge(df_test_ids,self.df_buyers,left_on="custoemr_id_hashed",right_on="buyers",how="left")
            df_test_ids['y_true']=np.where(pd.isnull(df_test_ids['buyers']),0,1)
            self.input_y_test_list=df_test_ids['y_true'].tolist()
            end_time=datetime.datetime.now() 
            print('Done -- %s: %s'%(func_name,str(end_time)))
            print("%s\n"%str(end_time-start_time))
            
        # 11
        def run_updating_df_count(self):
            func_name="run_updating_df_count"
            start_time=datetime.datetime.now()
            print('start -- %s: %s'%(func_name,str(start_time)))          
            df_test_X_count=pd.DataFrame({"records":self.df_test_X.shape[0],"IVs":self.df_test_X.shape[1]},index=["X_test"])
            self.db_row_counts=self.db_row_counts.append(df_test_X_count)
            end_time=datetime.datetime.now() 
            print('Done -- %s: %s'%(func_name,str(end_time)))
            print("%s\n"%str(end_time-start_time))

        # 12
        def generate_DV_distribution(self):
            func_name="generate_DV_distribution"
            start_time=datetime.datetime.now()
            print('start -- %s: %s'%(func_name,str(start_time)))   
            count_1=sum(self.input_y_train_list)
            count_0=len(self.input_y_train_list)-count_1
            df_y_train_count=pd.DataFrame({"0":count_0,"1":count_1},index=[self.n_week_DV])
            df_y_train_count.insert(0,"set","y_train")


            count_1=sum(self.input_y_test_list)
            count_0=len(self.input_y_test_list)-count_1
            df_y_test_count=pd.DataFrame({"0":count_0,"1":count_1},index=[self.n_week_DV])
            df_y_test_count.insert(0,"set","y_test")

            self.df_y_train_count=df_y_train_count
            self.df_y_test_count=df_y_test_count
            self.pctg=(sum(self.input_y_train_list)+sum(self.input_y_test_list))/(len(self.input_y_train_list)+len(self.input_y_test_list))
            end_time=datetime.datetime.now() 
            print('Done -- %s: %s'%(func_name,str(end_time)))
            print("%s\n"%str(end_time-start_time))
        # 13
        def pred_test_Y_past(self):
            func_name="pred_test_Y_past"
            start_time=datetime.datetime.now()
            print('start -- %s: %s'%(func_name,str(start_time)))   
            self.list_test_pred=self.res_of_model.predict(sm.add_constant(self.df_test_X)).tolist()
            end_time=datetime.datetime.now() 
            print('Done -- %s: %s'%(func_name,str(end_time)))
            print("%s\n"%str(end_time-start_time))
        # 14
        def generate_step_table_of_test_SM(self,):
            func_name="generate_step_table_of_test_SM"
            start_time=datetime.datetime.now()
            print('start -- %s: %s'%(func_name,str(start_time)))   
            if self.key_df_type=="trans_2_plus":
                threshold_list = [(x+1)/100 for x in range(0,100)] 
            elif self.key_df_type=="trans_1_only":
                margin=max(np.round(self.pctg/2,2),0.04)
                start_prob_pctg=max(0.001,int(np.floor((self.pctg-margin)*100))/100)
                end_prob_pctg=int(np.floor((self.pctg+margin)*100))/100
                threshold_list = [(x+1)/1000 for x in range(int(start_prob_pctg*1000),int(end_prob_pctg*1000))]
            else:
                print("Error of the key_df_type when run generate_step_table_of_test_SM")

                
            list_prob_test=self.list_test_pred
            df_output=pd.DataFrame()
            for i in threshold_list:
                y_test_pred=[1 if x>i else 0 for x in list_prob_test]

                accuracy_score = metrics.accuracy_score(self.input_y_test_list,y_test_pred)    
                tn, fp, fn, tp = metrics.confusion_matrix(self.input_y_test_list, y_test_pred).ravel()
                # 
                TPR=tp/(tp+fn) #recall
                FNR=fn/(tp+fn)
                FPR=fp/(fp+tn)
                TNR=tn/(fp+tn)

                PPV=tp/(tp+fp) #precission
                f1_score = 2*(TPR*PPV)/(TPR+PPV)

                df=pd.DataFrame({"predicted_positive":len([x for x in y_test_pred if x==1]),
                                 "predicted_negative":len([x for x in y_test_pred if x==0]),
                                 "accuracy_score":accuracy_score,
                                 'true_negative':tn,
                                 'false_positive':fp,
                                 'false_negative':fn,
                                 'true_positive':tp,
                                 'true_positive_rate':TPR,
                                 'false_negative_rate':FNR,
                                 'false_positive_rate':FPR,
                                 'true_negative_rate':TNR,
                                 'precission_(Positive predictive value)':PPV,
                                 'f1_score':f1_score
                                },index=[i])
                df_output=df_output.append(df)

            self.df_step_table=df_output
            end_time=datetime.datetime.now() 
            print('Done -- %s: %s'%(func_name,str(end_time)))
            print("%s\n"%str(end_time-start_time))
        # 15
        def select_best_scored_pred_prob(self):
            func_name="select_best_scored_pred_prob"
            start_time=datetime.datetime.now()
            print('start -- %s: %s'%(func_name,str(start_time)))        
            if self.key_df_type=="trans_2_plus":
                self.df_step_table['self_defined_score']=self.df_step_table.apply(lambda df: scoring_2_trans_plus_thresh(df['true_positive'],df['true_negative'],df['false_positive'],df['false_negative'],self.pctg),axis=1)  
            elif self.key_df_type=="trans_1_only":
                self.df_step_table['self_defined_score']=self.df_step_table.apply(lambda df: scoring_1_trans_only_thresh(df['true_positive'],df['true_negative'],df['false_positive'],df['false_negative'],self.pctg),axis=1)
            else:
                print("Error of the key_df_type when run select_best_scored_pred_prob")
            threshold_max_selfdefinedscore=self.df_step_table[self.df_step_table['self_defined_score']==self.df_step_table['self_defined_score'].max()].index[0]
            self.threshold_max_selfdefinedscore=threshold_max_selfdefinedscore
            print("threshold_max_selfdefinedscore",threshold_max_selfdefinedscore)
            self.df_step_table=self.df_step_table.reset_index()
            self.df_confusion_table=self.df_step_table.loc[self.df_step_table['index']==threshold_max_selfdefinedscore,:]
            end_time=datetime.datetime.now() 
            print('Done -- %s: %s'%(func_name,str(end_time)))
            print("%s\n"%str(end_time-start_time))
            
        # 16
        def generate_gain_chart_past(self):
            func_name="generate_gain_chart_past"
            start_time=datetime.datetime.now()
            print('start -- %s: %s'%(func_name,str(start_time)))
            self.df_gainchart_train=generate_gain_chart_function(df_X=self.df_train_X,
                                                                 list_y=self.input_y_train_list,
                                                                 list_ids=self.list_ids_y_train,
                                                                 result_sm_model=self.res_of_model,
                                                                 threshold=self.threshold_max_selfdefinedscore,
                                                                 list_selected_features=self.X_features)

            self.df_gainchart_test=generate_gain_chart_function(df_X=self.df_test_X,
                                                                list_y=self.input_y_test_list,
                                                                list_ids=self.list_ids_y_test,
                                                                result_sm_model=self.res_of_model,
                                                                threshold=self.threshold_max_selfdefinedscore,
                                                                list_selected_features=self.X_features)
            end_time=datetime.datetime.now() 
            print('Done -- %s: %s'%(func_name,str(end_time)))
            print("%s\n"%str(end_time-start_time))

        # 17
        def check_shopper_type_past(self):
            func_name="check_shopper_type_past"
            start_time=datetime.datetime.now()
            print('start -- %s: %s'%(func_name,str(start_time)))
            
            recent_4_week_sign_up_end_dt=self.IV_high_date
            recent_4_week_sign_up_start_dt=recent_4_week_sign_up_end_dt-datetime.timedelta(days=27)
            str_start_sign_up="'"+str(recent_4_week_sign_up_start_dt)+"'"
            str_end_sign_up="'"+str(recent_4_week_sign_up_end_dt)+"'"
            print("new sign up date range below: \n",recent_4_week_sign_up_start_dt,recent_4_week_sign_up_end_dt)

            df_recent_4_week_new_sings=pd.read_sql("select customer_id_hashed from BL_Rewards_Master where sign_up_date between %s and %s"%(str_start_sign_up,str_end_sign_up),con=self.sql_engine)
            df_recent_4_week_new_sings=df_recent_4_week_new_sings.drop_duplicates()
            df_recent_4_week_new_sings['sign_up_label']="new_signs"
            
            list_unsunsribe_ids_IV=pd.read_csv(self.path_unsub_IV,
                                 dtype=str,usecols=['customersummary_c_primaryscnhash'])['customersummary_c_primaryscnhash'].unique().tolist()
            
            # 
            df_train_ids_labeled=pd.DataFrame({"y_hat":self.list_train_pred},index=self.list_ids_y_train).reset_index().rename(columns={"index":"customer_id_hashed"})
            df_train_ids_labeled['selection_label']=np.where(df_train_ids_labeled['y_hat']>=self.threshold_max_selfdefinedscore,"target","nonselect")
            df_train_ids_labeled=pd.merge(df_train_ids_labeled,df_recent_4_week_new_sings,on="customer_id_hashed",how="left")
            df_train_ids_labeled['sign_up_label']=df_train_ids_labeled['sign_up_label'].fillna("existing")
            df_train_ids_labeled['actual_shopping_label']=self.input_y_train_list
            df_train_ids_labeled['actual_shopping_label']=df_train_ids_labeled['actual_shopping_label'].replace(0,"no").replace(1,"shopper")
            df_train_ids_labeled['email_subscription_label']=np.where(df_train_ids_labeled['customer_id_hashed'].isin(list_unsunsribe_ids_IV),"unsub","default_subs")
            
            
            self.df_train_ids_labeled_summary=df_train_ids_labeled.groupby(['selection_label','sign_up_label','actual_shopping_label','email_subscription_label'])['customer_id_hashed'].nunique().to_frame().reset_index()
            self.df_train_ids_labeled=df_train_ids_labeled

            # 
            df_test_ids_labeled=pd.DataFrame({"y_hat":self.list_test_pred},index=self.list_ids_y_test).reset_index().rename(columns={"index":"customer_id_hashed"})
            df_test_ids_labeled['selection_label']=np.where(df_test_ids_labeled['y_hat']>=self.threshold_max_selfdefinedscore,"target","nonselect")
            df_test_ids_labeled=pd.merge(df_test_ids_labeled,df_recent_4_week_new_sings,on="customer_id_hashed",how="left")
            df_test_ids_labeled['sign_up_label']=df_test_ids_labeled['sign_up_label'].fillna("existing")
            df_test_ids_labeled['actual_shopping_label']=self.input_y_test_list
            df_test_ids_labeled['actual_shopping_label']=df_test_ids_labeled['actual_shopping_label'].replace(0,"no").replace(1,"shopper")
            df_test_ids_labeled['email_subscription_label']=np.where(df_test_ids_labeled['customer_id_hashed'].isin(list_unsunsribe_ids_IV),"unsub","default_subs")
            
            self.df_test_ids_labeled_summary=df_test_ids_labeled.groupby(['selection_label','sign_up_label','actual_shopping_label','email_subscription_label'])['customer_id_hashed'].nunique().to_frame().reset_index()
            self.df_test_ids_labeled=df_test_ids_labeled        
            end_time=datetime.datetime.now() 
            print('Done -- %s: %s'%(func_name,str(end_time)))
            print("%s\n"%str(end_time-start_time))


        # 18
        def save_outputs_past(self):
            func_name="save_outputs_past"
            start_time=datetime.datetime.now()
            print('start -- %s: %s'%(func_name,str(start_time)))
            writer=pd.ExcelWriter(self.output_path,engine="xlsxwriter")

            self.db_row_counts.to_excel(writer,"df_dataset_shape")
            self.df_date_range.to_excel(writer,"df_date_range")
            self.df_y_train_count.to_excel(writer,"df_y_train_count")
            self.df_y_test_count.to_excel(writer,"df_y_test_count")
            self.summary_table_over.to_excel(writer,"summary_table_over")
            self.summary_table_output.to_excel(writer,"summary_table_output")
            self.df_step_table.to_excel(writer,"step_table",index=True)
            self.df_confusion_table.to_excel(writer,"select_score_matrix",index=False)

            self.df_gainchart_train.to_excel(writer,"gainchart_train",index=False)
            self.df_gainchart_test.to_excel(writer,"gainchart_test",index=False)
            self.df_department_name.to_excel(writer,"department_name",index=False)

            self.df_train_ids_labeled_summary.to_excel(writer,"train_id_summary",index=False)
            self.df_test_ids_labeled_summary.to_excel(writer,"test_id_summary",index=False)

            writer.save()
            str_IV_high_date=str(self.IV_high_date).replace("'","")
            str_dv_type="DV%i_%s"%(self.n_week_DV,self.key_df_type)
            str_DV_high_date=str(self.DV_high_date).replace("'","")
            
            self.df_train_ids_labeled.to_csv(self.output_folder+"df_train_ids_labeled_%s_%s_%s.csv"%(str_IV_high_date,str_dv_type,str_DV_high_date),index=False)
            self.df_test_ids_labeled.to_csv(self.output_folder+"df_test_ids_labeled_%s_%s_%s.csv"%(str_IV_high_date,str_dv_type,str_DV_high_date),index=False)
            end_time=datetime.datetime.now() 
            print('Done -- %s: %s'%(func_name,str(end_time)))
            print("%s\n"%str(end_time-start_time))
            
        # 19
        def select_target_df_from_mysql_current(self,n_limit_test=None):
            # Pull all the 18 months buyers up to the high date in our DB, and apply the previous model
            func_name="select_target_df_from_mysql_current"
            start_time=datetime.datetime.now()
            print('start -- %s: %s'%(func_name,str(start_time)))
            
            del self.df_train_X
            del self.df_test_X
            del self.df_train_ids_labeled
            del self.df_test_ids_labeled
            self.df_target_X=pd.DataFrame()
            gc.collect()
            
            
            file_json_table="/mnt/clients/juba/hqjubaapp02/jliang/Projects/Big_Lots/Predictive_Model/Model_Scripts/table_names_%s.json"%str(self.DV_high_date).replace("-","")
            self.dict_tables_for_current_week=json.load(open(file_json_table,"r"))        
            
            cols_in_X=self.summary_table_output.iloc[:,0].values.tolist()
            cols_in_X.remove("const")
            self.X_features=cols_in_X
            table_name_t0=self.dict_tables_for_current_week['table_crm_id_list_test']
            col_list_t0=pd.read_sql("select * from %s limit 2"%table_name_t0,con=self.sql_engine).columns.tolist()

            table_name_t1=self.dict_tables_for_current_week['table_df_1']
            col_list_t1=pd.read_sql("select * from %s limit 2"%table_name_t1,con=self.sql_engine).columns.tolist()

            table_name_t2_1=self.dict_tables_for_current_week['table_2_1']
            col_list_t2_1=pd.read_sql("select * from %s limit 2"%table_name_t2_1,con=self.sql_engine).columns.tolist()

            table_name_t2_2=self.dict_tables_for_current_week['table_2_2']
            col_list_t2_2=pd.read_sql("select * from %s limit 2"%table_name_t2_2,con=self.sql_engine).columns.tolist()

            col_list_t1=[x for x in col_list_t1 if x in cols_in_X]
            col_list_t2_1=[x for x in col_list_t2_1 if x in cols_in_X]
            col_list_t2_2=[x for x in col_list_t2_2 if x in cols_in_X]


            sql_str_cols_df0_test=str(["t0."+x for x in col_list_t0]).replace("'","")[1:-1]
            list_query_col_list=[sql_str_cols_df0_test]
            list_tables=["t0"]
            # col_list_dv=[x for x in self.df_train_Y.columns.tolist()]
            # sql_str_cols_dv=str(["t1."+x for x in col_list_dv]).replace("'","")[1:-1]
            # list_query_col_list.append(sql_str_cols_dv)
            if len(col_list_t1)>0:
                sql_str_cols_df_1=str(["t1."+x for x in col_list_t1]).replace("'","")[1:-1]
                list_query_col_list.append(sql_str_cols_df_1)
                list_tables.append("t1")
            if len(col_list_t2_1)>0:
                sql_str_cols_2_1=str(["t2_1."+x for x in col_list_t2_1]).replace("'","")[1:-1]
                list_query_col_list.append(sql_str_cols_2_1)
                list_tables.append("t2_1")
            if len(col_list_t2_2)>0:
                sql_str_cols_2_2=str(["t2_2."+x for x in col_list_t2_2]).replace("'","")[1:-1]
                list_query_col_list.append(sql_str_cols_2_2)
                list_tables.append("t2_2")
            str_cols_test=", ".join(list_query_col_list)
            select_clause="SELECT %s from %s as t0"%(str_cols_test,table_name_t0)

            list_of_join_clause=[]
            if "t1" in list_tables:
                str_join_clause_t1="left join %s as t1 on t0.customer_id_hashed=t1.customer_id_hashed"%table_name_t1
                list_of_join_clause.append(str_join_clause_t1)
            if "t2_1" in list_tables:
                str_join_clause_t2_1="left join %s as t2_1 on t0.customer_id_hashed=t2_1.id"%table_name_t2_1
                list_of_join_clause.append(str_join_clause_t2_1)
            if "t2_2" in list_tables:
                str_join_clause_t2_2="left join %s as t2_2 on t0.customer_id_hashed=t2_2.id"%table_name_t2_2
                list_of_join_clause.append(str_join_clause_t2_2)

            if self.key_df_type=="trans_1_only":
                where_clause="where t2_2.total_sales_recent_two_trans is null"
            elif self.key_df_type=="trans_2_plus":
                where_clause="where t2_2.total_sales_recent_two_trans is not null"
            else:
                where_clause=""
                print("key_df_type not specified, please choose either trans_1_only or trans_2_plus")
            if n_limit_test:
                limit_clause="limit %i"%n_limit_test
                query_full=" ".join([select_clause]+list_of_join_clause+[where_clause]+[limit_clause]).strip() 
            else:
                query_full=" ".join([select_clause]+list_of_join_clause+[where_clause]).strip() 
                
            print(query_full)
            self.df_target_X=pd.read_sql(query_full,con=self.sql_engine)
            if "nearest_BL_dist" in self.df_target_X.columns.tolist():
                self.df_test_X=self.df_target_X[pd.notnull(self.df_target_X['nearest_BL_dist'])]
            for col in self.df_target_X.columns.tolist():
                # df_nan=df[pd.isnull(df[col])]
                if self.df_target_X[pd.isnull(self.df_target_X[col])].shape[0]>0:
                    raise ValueError("%s in the selected test df is null"%col)       

            self.list_ids_y_target=self.df_target_X['customer_id_hashed'].values.tolist()
            del self.df_target_X['customer_id_hashed']
            
            # Below passed since the true is not available not and will be predicted from the model
            '''
            df_test_ids=pd.DataFrame({"custoemr_id_hashed":self.list_ids_y_test},index=range(len(self.list_ids_y_test)))
            df_test_ids=pd.merge(df_test_ids,self.df_buyers,left_on="custoemr_id_hashed",right_on="buyers",how="left")
            df_test_ids['y_true']=np.where(pd.isnull(df_test_ids['buyers']),0,1)
            self.input_y_test_list=df_test_ids['y_true'].tolist()
            '''
            end_time=datetime.datetime.now() 
            print('Done -- %s: %s'%(func_name,str(end_time)))
            print("%s\n"%str(end_time-start_time))
            
        # 20
        def pred_target_Y_current_week(self):
            func_name="pred_target_Y_current_week"
            start_time=datetime.datetime.now()
            print('start -- %s: %s'%(func_name,str(start_time)))
            self.list_target_pred=self.res_of_model.predict(sm.add_constant(self.df_target_X)).tolist()
            end_time=datetime.datetime.now() 
            print('Done -- %s: %s'%(func_name,str(end_time)))
            print("%s\n"%str(end_time-start_time))
            
        # 21
        def generate_and_write_target_id_with_label(self):
            func_name="generate_and_write_target_id_with_label"
            start_time=datetime.datetime.now()
            print('start -- %s: %s'%(func_name,str(start_time)))
            
            recent_4_week_sign_up_end_dt=self.DV_high_date
            recent_4_week_sign_up_start_dt=recent_4_week_sign_up_end_dt-datetime.timedelta(days=27)
            str_start_sign_up="'"+str(recent_4_week_sign_up_start_dt)+"'"
            str_end_sign_up="'"+str(recent_4_week_sign_up_end_dt)+"'"
            print("new sign up date range below: \n",recent_4_week_sign_up_start_dt,recent_4_week_sign_up_end_dt)

            df_recent_4_week_new_sings=pd.read_sql("select customer_id_hashed from BL_Rewards_Master where sign_up_date between %s and %s"%(str_start_sign_up,str_end_sign_up),con=self.sql_engine)
            df_recent_4_week_new_sings=df_recent_4_week_new_sings.drop_duplicates()
            df_recent_4_week_new_sings['sign_up_label']="new_signs"
            # 
            df_target_ids_labeled=pd.DataFrame({"y_hat":self.list_target_pred},index=self.list_ids_y_target).reset_index().rename(columns={"index":"customer_id_hashed"})
            df_target_ids_labeled['selection_label']=np.where(df_target_ids_labeled['y_hat']>=self.threshold_max_selfdefinedscore,"target","nonselect")
            df_target_ids_labeled=pd.merge(df_target_ids_labeled,df_recent_4_week_new_sings,on="customer_id_hashed",how="left")
            df_target_ids_labeled['sign_up_label']=df_target_ids_labeled['sign_up_label'].fillna("existing")
            
            list_unsunsribe_ids_DV=pd.read_csv(self.path_unsub_DV,
                                 dtype=str,usecols=['customersummary_c_primaryscnhash'])['customersummary_c_primaryscnhash'].unique().tolist()
            df_target_ids_labeled['email_subscription_label']=np.where(df_target_ids_labeled['customer_id_hashed'].isin(list_unsunsribe_ids_DV),"unsub","default_subs")
            self.df_target_ids_labeled=df_target_ids_labeled
            self.df_target_ids_labeled_summary=df_target_ids_labeled.groupby(['selection_label','sign_up_label','email_subscription_label'])['customer_id_hashed'].nunique().to_frame().reset_index()
            
            self.df_target_ids_labeled.to_csv(self.output_folder+"df_target_ids_labeled_%s_%s_%s.csv"%(self.str_IV_high_date,self.key_df_type,str(self.DV_high_date)),index=False)
            self.df_target_ids_labeled_summary.to_csv(self.output_folder+"df_target_ids_labeled_summary_%s_%s_%s.csv"%(self.str_IV_high_date,self.key_df_type,str(self.DV_high_date)),index=False)
            end_time=datetime.datetime.now()
            print('Done -- %s: %s'%(func_name,str(end_time)))
            print("%s\n"%str(end_time-start_time))
            
            


    # In[8]:


    # 1
    n_week_DV=3
    key_df_type="trans_1_only"


    print("%s DV %i start: "%(key_df_type,n_week_DV),datetime.datetime.now())
    SM_Logistic_Model_dvN.__init__(self=SM_Logistic_Model_dvN,
                                   n_week_DV=n_week_DV,
                                   key_df_type=key_df_type
                                  )
    # 2
    SM_Logistic_Model_dvN.pull_train_df_X(SM_Logistic_Model_dvN)
    # 3
    SM_Logistic_Model_dvN.clean_train_univariate(SM_Logistic_Model_dvN,r_variance=0.95)
    # 4
    SM_Logistic_Model_dvN.pull_train_df_Y(SM_Logistic_Model_dvN)
    # 5
    SM_Logistic_Model_dvN.remove_correlated_cols(SM_Logistic_Model_dvN,coorelation_threshold=0.8)
    # 6
    SM_Logistic_Model_dvN.select_from_model_n_features(SM_Logistic_Model_dvN, 
                                                       N_feature_select_from_models=min(60,int(SM_Logistic_Model_dvN.df_train_X.shape[1]*0.7)))
    # 7
    SM_Logistic_Model_dvN.select_RFE(SM_Logistic_Model_dvN,n_features_to_select=40)
    # 8
    SM_Logistic_Model_dvN.forwards_feature_elimination_based_on_p_and_vif(SM_Logistic_Model_dvN)
    # 9
    SM_Logistic_Model_dvN.run_sm_logR_model(SM_Logistic_Model_dvN)
    # 10
    SM_Logistic_Model_dvN.select_test_df_from_mysql_past(SM_Logistic_Model_dvN,n_limit_test=3*10**6)
    # 11
    SM_Logistic_Model_dvN.run_updating_df_count(SM_Logistic_Model_dvN)
    # 12
    SM_Logistic_Model_dvN.generate_DV_distribution(SM_Logistic_Model_dvN)
    # 13
    SM_Logistic_Model_dvN.pred_test_Y_past(SM_Logistic_Model_dvN)
    # 14
    SM_Logistic_Model_dvN.generate_step_table_of_test_SM(SM_Logistic_Model_dvN)
    # 15
    SM_Logistic_Model_dvN.select_best_scored_pred_prob(SM_Logistic_Model_dvN)
    # 16
    SM_Logistic_Model_dvN.generate_gain_chart_past(SM_Logistic_Model_dvN)
    # 17
    SM_Logistic_Model_dvN.check_shopper_type_past(SM_Logistic_Model_dvN)
    # 18
    SM_Logistic_Model_dvN.save_outputs_past(SM_Logistic_Model_dvN)
    # 19
    SM_Logistic_Model_dvN.select_target_df_from_mysql_current(SM_Logistic_Model_dvN,n_limit_test=None)
    # 20
    SM_Logistic_Model_dvN.pred_target_Y_current_week(SM_Logistic_Model_dvN)
    # 21
    SM_Logistic_Model_dvN.generate_and_write_target_id_with_label(SM_Logistic_Model_dvN)
            
    print("%s DV %i done: "%(key_df_type,n_week_DV),datetime.datetime.now())


    # In[9]:


    # 1
    n_week_DV=2
    key_df_type="trans_2_plus"


    print("%s DV %i start: "%(key_df_type,n_week_DV),datetime.datetime.now())
    SM_Logistic_Model_dvN.__init__(self=SM_Logistic_Model_dvN,
                                   n_week_DV=n_week_DV,
                                   key_df_type=key_df_type
                                  )
    # 2
    SM_Logistic_Model_dvN.pull_train_df_X(SM_Logistic_Model_dvN)
    # 3
    SM_Logistic_Model_dvN.clean_train_univariate(SM_Logistic_Model_dvN,r_variance=0.95)
    # 4
    SM_Logistic_Model_dvN.pull_train_df_Y(SM_Logistic_Model_dvN)
    # 5
    SM_Logistic_Model_dvN.remove_correlated_cols(SM_Logistic_Model_dvN,coorelation_threshold=0.8)
    # 6
    SM_Logistic_Model_dvN.select_from_model_n_features(SM_Logistic_Model_dvN, 
                                                       N_feature_select_from_models=min(60,int(SM_Logistic_Model_dvN.df_train_X.shape[1]*0.7)))
    # 7
    SM_Logistic_Model_dvN.select_RFE(SM_Logistic_Model_dvN,n_features_to_select=40)
    # 8
    SM_Logistic_Model_dvN.forwards_feature_elimination_based_on_p_and_vif(SM_Logistic_Model_dvN)
    # 9
    SM_Logistic_Model_dvN.run_sm_logR_model(SM_Logistic_Model_dvN)
    # 10
    SM_Logistic_Model_dvN.select_test_df_from_mysql_past(SM_Logistic_Model_dvN,n_limit_test=3*10**6)
    # 11
    SM_Logistic_Model_dvN.run_updating_df_count(SM_Logistic_Model_dvN)
    # 12
    SM_Logistic_Model_dvN.generate_DV_distribution(SM_Logistic_Model_dvN)
    # 13
    SM_Logistic_Model_dvN.pred_test_Y_past(SM_Logistic_Model_dvN)
    # 14
    SM_Logistic_Model_dvN.generate_step_table_of_test_SM(SM_Logistic_Model_dvN)
    # 15
    SM_Logistic_Model_dvN.select_best_scored_pred_prob(SM_Logistic_Model_dvN)
    # 16
    SM_Logistic_Model_dvN.generate_gain_chart_past(SM_Logistic_Model_dvN)
    # 17
    SM_Logistic_Model_dvN.check_shopper_type_past(SM_Logistic_Model_dvN)
    # 18
    SM_Logistic_Model_dvN.save_outputs_past(SM_Logistic_Model_dvN)
    # 19
    SM_Logistic_Model_dvN.select_target_df_from_mysql_current(SM_Logistic_Model_dvN,n_limit_test=None)
    # 20
    SM_Logistic_Model_dvN.pred_target_Y_current_week(SM_Logistic_Model_dvN)
    # 21
    SM_Logistic_Model_dvN.generate_and_write_target_id_with_label(SM_Logistic_Model_dvN)
            
    print("%s DV %i done: "%(key_df_type,n_week_DV),datetime.datetime.now())


    # In[10]:


    task_end_time=datetime.datetime.now()

    print("***************\n")
    print("Finished predict_with_current_week_data: %s"%str(task_end_time))
    print("***************\n")
    print("Duration: %s"%str(task_end_time-task_start_time))

