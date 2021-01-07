#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sqlalchemy
import json
import pandas as pd
import datetime
import os
import numpy as np
import gc

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

def run_predicting():

    print(sklearn.__version__)
    print("Job start: model building", datetime.datetime.now())



    dict_config=json.load(open("/home/jian/Projects/Big_Lots/Predictive_Model/extract_from_MySQL/config.json"))
    high_date=dict_config['pos_end_date']
    username=dict_config['username']
    password=dict_config['password']
    database=dict_config['database']
    recent_n_month=dict_config['recent_n_month']
    path_dept_name=dict_config["path_dept_name"]

    dict_tables=json.load(open("/home/jian/Projects/Big_Lots/Predictive_Model/extract_from_MySQL/table_names_%s.json"%str(high_date).replace("-","")))
    table_df_1=dict_tables['table_df_1']
    table_2_1=dict_tables['table_2_1']
    table_2_2=dict_tables['table_2_2']
    table_0_train=dict_tables['table_crm_id_list_train']
    table_0_test=dict_tables['table_crm_id_list_test']

    BL_engine=sqlalchemy.create_engine("mysql+pymysql://%s:%s@localhost/%s" % (username, password, database))


    # In[3]:


    for t in [table_df_1,table_2_1,table_2_2,table_0_train]:
        print(pd.read_sql("select count(*) from %s"%t,con=BL_engine).iloc[0],t)


    # In[4]:


    col_list_df0_train=pd.read_sql("desc %s"%table_0_train,con=BL_engine)['Field'].values.tolist()
    col_list_df_1=pd.read_sql("desc %s"%table_df_1,con=BL_engine)['Field'].values.tolist()
    col_list_2_1=pd.read_sql("desc %s"%table_2_1,con=BL_engine)['Field'].values.tolist()
    col_list_2_2=pd.read_sql("desc %s"%table_2_2,con=BL_engine)['Field'].values.tolist()


    # In[5]:


    cols_no_need=['sign_up_location','customer_zip_code','nearest_BL_store','distc_to_sign_up',
                  'week_1st_trans','week_recent_one_trans','week_recent_two_trans']
    for col_remove in cols_no_need:
        col_list_df_1=[x for x in col_list_df_1 if x != col_remove and (x not in ["customer_id_hashed", "sign_up_date"])]
        col_list_2_1=[x for x in col_list_2_1 if x != col_remove and x!="id"]
        col_list_2_2=[x for x in col_list_2_2 if x != col_remove and x!="id"]
        
    sql_str_cols_df0_train=str(["t0."+x for x in col_list_df0_train]).replace("'","")[1:-1]  
    sql_str_cols_df_1=str(["t1."+x for x in col_list_df_1]).replace("'","")[1:-1]
    sql_str_cols_2_1=str(["t2_1."+x for x in col_list_2_1]).replace("'","")[1:-1]
    sql_str_cols_2_2=str(["t2_2."+x for x in col_list_2_2]).replace("'","")[1:-1]
    sql_str_cols_all=", ".join([sql_str_cols_df0_train,sql_str_cols_df_1,sql_str_cols_2_1,sql_str_cols_2_2])
    # sql_str_cols_all


    # In[6]:


    dict_cols_in_table={
        "t0_train":{"table_name":table_0_train,"cols":['customer_id_hashed']},
        "t0_test":{"table_name":table_0_test,"cols":['customer_id_hashed']},
        "t1":{"table_name":table_df_1,"cols":col_list_df_1},
        "t2_1":{"table_name":table_2_1,"cols":col_list_2_1},
        "t2_2":{"table_name":table_2_2,"cols":col_list_2_2}
    }


    # In[7]:


    queary="SELECT %s from %s as t0 left join %s as t1 on t0.customer_id_hashed=t1.customer_id_hashed left join %s as t2_1 on t0.customer_id_hashed=t2_1.id left join %s as t2_2 on t0.customer_id_hashed=t2_2.id"%(sql_str_cols_all,table_0_train,table_df_1,table_2_1,table_2_2)
    # queary


    # In[8]:


    print(datetime.datetime.now())
    df_train=pd.read_sql(queary,con=BL_engine)
    print(df_train.shape)
    print(datetime.datetime.now())

    df_train_trans_1_only=df_train[pd.isnull(df_train['total_sales_recent_two_trans'])]
    df_train_trans_2_plus=df_train[pd.notnull(df_train['total_sales_recent_two_trans'])]


    # In[9]:


    list_cols=df_train.columns.tolist()
    del df_train

    for col in list_cols:
        if df_train_trans_1_only[col].nunique()<=1:
            del df_train_trans_1_only[col]
            print("df_train_trans_1_only, col_nunique<=1 dropped: %s"%col)
            
    for col in list_cols:
        if df_train_trans_2_plus[col].nunique()<=1:
            del df_train_trans_2_plus[col]
            print("df_train_trans_2_plus, col_nunique<=1 dropped: %s"%col)
            
    gc.collect()


    # In[10]:


    print("df_train_trans_1_only",datetime.datetime.now())
    print("df_train_trans_1_only",df_train_trans_1_only.shape)
    df_train_trans_1_only=df_train_trans_1_only.T.drop_duplicates().T
    print("df_train_trans_1_only",datetime.datetime.now())
    print(df_train_trans_1_only.shape)

    ## 
    print("df_train_trans_2_plus",datetime.datetime.now())
    df_train_trans_2_plus_copy=df_train_trans_2_plus.head(3*10**5)
    print("df_train_trans_2_plus_copy",df_train_trans_2_plus_copy.shape)
    df_train_trans_2_plus_copy=df_train_trans_2_plus_copy.T.drop_duplicates().T
    print("df_train_trans_2_plus_copy",datetime.datetime.now())
    print(df_train_trans_2_plus_copy.shape)

    list_cols_keep=df_train_trans_2_plus_copy.columns.tolist()
    df_train_trans_2_plus=df_train_trans_2_plus[list_cols_keep]
    print("df_train_trans_2_plus",df_train_trans_2_plus.shape)

    del df_train_trans_2_plus_copy
    gc.collect()


    # In[11]:


    list_cols_remove_na_rows=['nearest_BL_dist']

    for col in df_train_trans_2_plus.columns.tolist():
        df_na=df_train_trans_2_plus[pd.isnull(df_train_trans_2_plus[col])]
        if df_na.shape[0]>0:
            if col in list_cols_remove_na_rows:
                df_train_trans_2_plus=df_train_trans_2_plus[pd.notnull(df_train_trans_2_plus[col])]
                print(col,"with na to delete", df_na.shape[0])
            else:
                print("Warning: nan detected in the df_train_trans_2_plus col -- %s"%col)
       

    for col in df_train_trans_1_only.columns.tolist():
        df_na=df_train_trans_1_only[pd.isnull(df_train_trans_1_only[col])]
        if df_na.shape[0]>0:
            if col in list_cols_remove_na_rows:
                df_train_trans_1_only=df_train_trans_1_only[pd.notnull(df_train_trans_1_only[col])]
                print(col,"with na to delete", df_na.shape[0])
            else:
                print("Warning: nan detected in the df_train_trans_1_only col -- %s"%col)


    # In[12]:


    list_train_trans_2_plus_id=df_train_trans_2_plus.iloc[:,0].values.tolist()
    df_train_trans_2_plus_Y=df_train_trans_2_plus.iloc[:,1:5]
    df_train_trans_2_plus_X=df_train_trans_2_plus.iloc[:,5:]
    df_train_trans_2_plus_X=df_train_trans_2_plus_X.astype(float)
    del df_train_trans_2_plus

    list_train_trans_1_only_id=df_train_trans_1_only.iloc[:,0].values.tolist()
    df_train_trans_1_only_Y=df_train_trans_1_only.iloc[:,1:5]
    df_train_trans_1_only_X=df_train_trans_1_only.iloc[:,5:]
    df_train_trans_1_only_X=df_train_trans_1_only_X.astype(float)

    del df_train_trans_1_only


    gc.collect()


    # In[13]:


    # remove_low_variance columns

    r_variance=0.98
    threshold_variance_iv=r_variance*(1-r_variance)
    # df_train_trans_2_plus_X
    selector = VarianceThreshold(threshold=threshold_variance_iv)
    df_redused_X=selector.fit_transform(df_train_trans_2_plus_X)
    print("df_train_trans_2_plus_X reduced to the shape due to %s variante"%(str(r_variance)),df_redused_X.shape)
    indices = [i for i, x in enumerate(list(selector.get_support())) if x == True]
    df_train_trans_2_plus_X=df_train_trans_2_plus_X.iloc[:,indices]

    # df_train_trans_1_only_X
    selector = VarianceThreshold(threshold=threshold_variance_iv)
    df_redused_X=selector.fit_transform(df_train_trans_1_only_X)
    print("df_train_trans_1_only_X reduced to the shape due to %s variante"%(str(r_variance)),df_redused_X.shape)
    indices = [i for i, x in enumerate(list(selector.get_support())) if x == True]
    df_train_trans_1_only_X=df_train_trans_1_only_X.iloc[:,indices]

    del df_redused_X
    gc.collect()


    # In[14]:


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


    # In[15]:


    coorelation_threshold=0.8

    print(datetime.datetime.now(),"Start remove correlated cols: df_train_trans_2_plus_X",df_train_trans_2_plus_X.shape)
    df_train_trans_2_plus_X=remove_cols_with_high_coor(df_X=df_train_trans_2_plus_X,coorelation_threshold=coorelation_threshold)
    print(datetime.datetime.now(),"Done remove correlated cols: df_train_trans_2_plus_X",df_train_trans_2_plus_X.shape)
    ###### 
    print(datetime.datetime.now(),"Start remove correlated cols: df_train_trans_1_only_X",df_train_trans_1_only_X.shape)
    df_train_trans_1_only_X=remove_cols_with_high_coor(df_X=df_train_trans_1_only_X,coorelation_threshold=coorelation_threshold)
    print(datetime.datetime.now(),"Done remove correlated cols: df_train_trans_1_only_X",df_train_trans_1_only_X.shape)


    # In[16]:


    dict_df_type={
        "trans_1_only":{
            "df_X":df_train_trans_1_only_X,
            "df_Y":df_train_trans_1_only_Y,
            "list_id":list_train_trans_1_only_id
        },
        "trans_2_plus":{
            "df_X":df_train_trans_2_plus_X,
            "df_Y":df_train_trans_2_plus_Y,
            "list_id":list_train_trans_2_plus_id
        },
    }


    # In[17]:


    # iv_start_date
    iv_end_date=datetime.datetime.strptime(high_date,"%Y-%m-%d").date()
    iv_start_date=iv_end_date-datetime.timedelta(days=int(np.ceil(365*recent_n_month/12)))

    dv_end_date=iv_end_date+datetime.timedelta(days=28)
    dv_start_date=iv_end_date+datetime.timedelta(days=1)  
    df_date_range=pd.DataFrame({"start":[iv_start_date,dv_start_date],"end":[iv_end_date,dv_end_date]},index=['IVs',"DVs"])


    # In[18]:


    def scoring_above_5pctg_thresh(tp,tn,fp,fn,pctg):
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

    def scoring_below_5pctg_thresh(tp,tn,fp,fn,pctg):
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
        list_pred_prob=result_sm_model.predict(sm.add_constant(df_X)).values
        df_pred_by_id=pd.DataFrame({"customer_id_hashed":list_ids,"pred_prob":list_pred_prob},index=range(len(list_pred_prob)))
        copy_xtrain=df_X.copy().reset_index()
        del copy_xtrain['index']
        df_pred_by_id=pd.concat([df_pred_by_id,copy_xtrain],axis=1,ignore_index=False)

        df_pred_by_id['decile']=pd.qcut(df_pred_by_id['pred_prob'], 10, labels=False)
        df_pred_by_id['decile']=df_pred_by_id['decile'].apply(lambda x: "D"+str(x+1).zfill(2))
        df_pred_by_id['y_true']=list_y
        df_pred_by_id['y_hat']=np.where(df_pred_by_id['pred_prob']>threshold,1,0)

        agg_func,list_cols_to_get_ratio=write_aggregate_func_gain_chart(list_selected_features,df_pred_by_id)
        df_gainchart=df_pred_by_id.groupby("decile")[['customer_id_hashed']+list_selected_features].agg(agg_func).reset_index()

        df_prob_max=df_pred_by_id.groupby("decile")['pred_prob'].max().to_frame().reset_index().rename(columns={"pred_prob":"max_prob"})
        df_prob_min=df_pred_by_id.groupby("decile")['pred_prob'].min().to_frame().reset_index().rename(columns={"pred_prob":"min_prob"})
        df_gainchart=pd.merge(df_gainchart,df_prob_max,on="decile")
        df_gainchart=pd.merge(df_gainchart,df_prob_min,on="decile")
        df_gainchart.insert(2,"actual_ratio",df_gainchart['y_true']/df_gainchart['customer_id_hashed'])
        df_gainchart.insert(3,"pred_ratio",df_gainchart['y_hat']/df_gainchart['customer_id_hashed'])


        df_gainchart.insert(4,"max_pred_prob",df_gainchart['max_prob'])
        df_gainchart.insert(5,"min_pred_prob",df_gainchart['min_prob'])
        del df_gainchart['max_prob']
        del df_gainchart['min_prob']

        return df_gainchart


    # In[21]:


    class SM_Logistic_Model_dvN:
        # 1
        def __init__(self,n_week_DV,key_df_type,df_date_range,sql_engine=BL_engine,dict_cols_in_table=dict_cols_in_table):

    # n_week_DV: 1-4
    # key_df_type: "trans_1_only" or "trans_2_plus" in the keys of dict_df_type
    # df_date_range: defined global df -- df_date_range
    # sql_engine: mysql engine to BigLots database

            self.n_week_DV=n_week_DV
            self.dict_cols_in_table=dict_cols_in_table
            self.key_df_type=key_df_type
            self.sql_engine=BL_engine
            self.df_train_X=dict_df_type[key_df_type]['df_X']
            self.X_train_scaled=scale(self.df_train_X) # will be wroten later in #2, 3 & 4

            self.df_train_Y=dict_df_type[key_df_type]['df_Y']
            self.input_y_train_list=self.df_train_Y["DV_cumulative_week_updated_%i"%n_week_DV].values.tolist()
            
            self.list_ids_y_train=dict_df_type[key_df_type]['list_id']
            
            self.X_features=self.df_train_X.columns.tolist()

            self.df_test_X=pd.DataFrame()
            self.df_test_Y=pd.DataFrame()
            self.df_test_id=pd.DataFrame()
            self.input_y_test_list=[]
            
            self.df_date_range=df_date_range
            
            self.db_row_counts=pd.DataFrame({"records":self.df_train_X.shape[0],"IVs":self.df_train_X.shape[1]},index=["X_train"])        
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
            
            
            self.output_folder="./output_No_DCM_%s_%s/"%(str(self.df_date_range.iloc[0,1]),str(datetime.datetime.now().date()))
            
            try:
                os.stat(self.output_folder)
            except:
                os.mkdir(self.output_folder)
                
            self.output_path=self.output_folder+"BL_LRModeling_NoDCM_%s_DV%s_%s_JL_%s.xlsx"%(key_df_type,str(n_week_DV),str(self.df_date_range.iloc[0,1]),str(datetime.datetime.now()))
            self.df_department_name=pd.read_table(path_dept_name,sep="|").drop_duplicates()
        # 2
        def select_from_model_n_features(self, N_feature_select_from_models):
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

            self.X_features=[self.X_features[i] for i,v in enumerate(selector_support_FROMMODEL) if v==True]

            self.df_train_X=self.df_train_X.loc[:,self.X_features]

            print("df_train_X.shape",self.df_train_X.shape)

            self.X_train_scaled=scale(self.df_train_X)
            print("X_train_scaled.shape",self.X_train_scaled.shape)
            print("Done select_from_model_n_features %d: "%N_feature_select_from_models,datetime.datetime.now())
            
        #3
        def select_REF(self,n_features_to_select):
            print("Starting select_REF: ",datetime.datetime.now())

            estimator = LogisticRegression(fit_intercept=True,solver='saga',max_iter=2000,n_jobs=24,tol=0.001)
            selector = RFE(estimator,step=1,n_features_to_select=n_features_to_select)
            selector = selector.fit(self.X_train_scaled, self.input_y_train_list)
            selector_support_REF=selector.support_
            print("Done select_REF: ",datetime.datetime.now())

            self.X_features=[self.X_features[i] for i,v in enumerate(selector_support_REF) if v==True]

            self.df_train_X=self.df_train_X.loc[:,self.X_features]

            print("df_train_X.shape",self.df_train_X.shape)
            self.X_train_scaled=scale(self.df_train_X)
            print("X_train_scaled.shape",self.X_train_scaled.shape)
        
        #4
        def forwards_feature_elimination_based_on_p_and_vif(self,niter=50,method="lbfgs",p_tol=0.1,vif_tol=5):
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

            
        # 5
        def run_sm_logR_model(self):
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

        # 6
        def select_test_df_from_mysql(self):
            cols_in_X=self.summary_table_output.iloc[:,0].values.tolist()
            cols_in_X.remove("const")
            self.X_features=cols_in_X
            
            table_name_t0=self.dict_cols_in_table['t0_test']['table_name']
            col_list_t0=self.dict_cols_in_table['t0_test']['cols']
            
            table_name_t1=self.dict_cols_in_table['t1']['table_name']
            col_list_t1=self.dict_cols_in_table['t1']['cols']
            
            table_name_t2_1=self.dict_cols_in_table['t2_1']['table_name']
            col_list_t2_1=self.dict_cols_in_table['t2_1']['cols']
            
            table_name_t2_2=self.dict_cols_in_table['t2_2']['table_name']
            col_list_t2_2=self.dict_cols_in_table['t2_2']['cols']
            
            col_list_t1=[x for x in col_list_t1 if x in cols_in_X]
            col_list_t2_1=[x for x in col_list_t2_1 if x in cols_in_X]
            col_list_t2_2=[x for x in col_list_t2_2 if x in cols_in_X]
            
            
            sql_str_cols_df0_test=str(["t0."+x for x in col_list_t0]).replace("'","")[1:-1]
            list_query_col_list=[sql_str_cols_df0_test]
            list_tables=["t0"]
            col_list_dv=[x for x in self.df_train_Y.columns.tolist()]
            sql_str_cols_dv=str(["t1."+x for x in col_list_dv]).replace("'","")[1:-1]
            list_query_col_list.append(sql_str_cols_dv)
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
            query_full=" ".join([select_clause]+list_of_join_clause+[where_clause]).strip() 
            print(query_full)
            df=pd.read_sql(query_full,con=self.sql_engine)
            if "nearest_BL_dist" in df.columns.tolist():
                df=df[pd.notnull(df['nearest_BL_dist'])]
            for col in df.columns.tolist():
                df_nan=df[pd.isnull(df[col])]
                if df_nan.shape[0]>0:
                    raise ValueError("%s in the selected test df is null"%col)
                    
            cols_Y=[x for x in df.columns.tolist() if "cumulative" in x]
            self.df_test_Y=df[cols_Y]
            for col in cols_Y:
                del df[col]
            self.list_ids_y_test=df['customer_id_hashed'].values.tolist()
            del df['customer_id_hashed']
            self.input_y_test_list=self.df_test_Y["DV_cumulative_week_updated_%i"%self.n_week_DV].values.tolist()     
            self.df_test_X=df
            self.X_teest_scaled=scale(self.df_test_X)
        # 7
        def run_updating_df_count(self):
            df_test_X_count=pd.DataFrame({"records":self.df_test_X.shape[0],"IVs":self.df_test_X.shape[1]},index=["X_test"])
            self.db_row_counts=self.db_row_counts.append(df_test_X_count)

        # 8
        def generate_DV_distribution(self):
            df_y_train_count=pd.DataFrame()
            for col in self.df_train_Y.columns.tolist():
                count_1=self.df_train_Y[self.df_train_Y[col]==1].shape[0]
                count_0=self.df_train_Y[self.df_train_Y[col]==0].shape[0]

                df=pd.DataFrame({"0":count_0,"1":count_1},index=[col])
                df_y_train_count=df_y_train_count.append(df)
            df_y_train_count.insert(0,"set","y_train")


            df_y_test_count=pd.DataFrame()
            for col in self.df_test_Y.columns.tolist():
                count_1=self.df_test_Y[self.df_test_Y[col]==1].shape[0]
                count_0=self.df_test_Y[self.df_test_Y[col]==0].shape[0]

                df=pd.DataFrame({"0":count_0,"1":count_1},index=[col])
                df_y_test_count=df_y_test_count.append(df)
            df_y_test_count.insert(0,"set","y_test")

            self.df_y_train_count=df_y_train_count
            self.df_y_test_count=df_y_test_count
            self.pctg=(sum(self.input_y_train_list)+sum(self.input_y_test_list))/(len(self.input_y_train_list)+len(self.input_y_test_list))
            
        # 9
        def pred_test_Y(self):
            self.list_test_pred=self.res_of_model.predict(sm.add_constant(self.df_test_X)).tolist()
            
        # 10
        def generate_step_table_of_test_SM(self,):
            if self.pctg>=0.05:
                threshold_list = [(x+1)/100 for x in range(0,100)] 
            else:
                start_prob_pctg=max(0.001,int(np.floor((self.pctg-0.02)*100))/100)
                end_prob_pctg=int(np.floor((self.pctg+0.02)*100))/100
                threshold_list = [(x+1)/1000 for x in range(int(start_prob_pctg*1000),int(end_prob_pctg*1000))]
                
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
            
        # 11
        def select_best_scored_pred_prob(self):
            
            if self.pctg>=0.05:
                self.df_step_table['self_defined_score']=self.df_step_table.apply(lambda df: scoring_above_5pctg_thresh(df['true_positive'],df['true_negative'],df['false_positive'],df['false_negative'],self.pctg),axis=1)
            else:
                self.df_step_table['self_defined_score']=self.df_step_table.apply(lambda df: scoring_below_5pctg_thresh(df['true_positive'],df['true_negative'],df['false_positive'],df['false_negative'],self.pctg),axis=1)
                
            threshold_max_selfdefinedscore=self.df_step_table[self.df_step_table['self_defined_score']==self.df_step_table['self_defined_score'].max()].index[0]
            self.threshold_max_selfdefinedscore=threshold_max_selfdefinedscore
            print("threshold_max_selfdefinedscore",threshold_max_selfdefinedscore)
            self.df_step_table=self.df_step_table.reset_index()
            self.df_confusion_table=self.df_step_table.loc[self.df_step_table['index']==threshold_max_selfdefinedscore,:]

        # 12
        def generate_gain_chart(self):
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
            
        # 13
        def check_shopper_type(self):
            recent_4_week_sign_up_end_dt=self.df_date_range.iloc[0,1]
            recent_4_week_sign_up_start_dt=recent_4_week_sign_up_end_dt-datetime.timedelta(days=27)
            str_start_sign_up="'"+str(recent_4_week_sign_up_start_dt)+"'"
            str_end_sign_up="'"+str(recent_4_week_sign_up_end_dt)+"'"
            print("new sign up date range below: \n",recent_4_week_sign_up_start_dt,recent_4_week_sign_up_end_dt)

            df_recent_4_week_new_sings=pd.read_sql("select customer_id_hashed from BL_Rewards_Master where sign_up_date between %s and %s"%(str_start_sign_up,str_end_sign_up),con=BL_engine)
            df_recent_4_week_new_sings=df_recent_4_week_new_sings.drop_duplicates()
            df_recent_4_week_new_sings['sign_up_label']="new_signs"
            # 
            df_train_ids_labeled=pd.DataFrame({"y_hat":self.list_train_pred},index=self.list_ids_y_train).reset_index().rename(columns={"index":"customer_id_hashed"})
            df_train_ids_labeled['selection_label']=np.where(df_train_ids_labeled['y_hat']>=self.threshold_max_selfdefinedscore,"target","nonselect")
            df_train_ids_labeled=pd.merge(df_train_ids_labeled,df_recent_4_week_new_sings,on="customer_id_hashed",how="left")
            df_train_ids_labeled['sign_up_label']=df_train_ids_labeled['sign_up_label'].fillna("existing")
            df_train_ids_labeled['actual_shopping_label']=self.input_y_train_list
            df_train_ids_labeled['actual_shopping_label']=df_train_ids_labeled['actual_shopping_label'].replace(0,"no").replace(1,"shopper")
            
            self.df_train_ids_labeled_summary=df_train_ids_labeled.groupby(['selection_label','sign_up_label','actual_shopping_label'])['customer_id_hashed'].nunique().to_frame().reset_index()
            self.df_train_ids_labeled=df_train_ids_labeled
            
            # 
            df_test_ids_labeled=pd.DataFrame({"y_hat":self.list_test_pred},index=self.list_ids_y_test).reset_index().rename(columns={"index":"customer_id_hashed"})
            df_test_ids_labeled['selection_label']=np.where(df_test_ids_labeled['y_hat']>=self.threshold_max_selfdefinedscore,"target","nonselect")
            df_test_ids_labeled=pd.merge(df_test_ids_labeled,df_recent_4_week_new_sings,on="customer_id_hashed",how="left")
            df_test_ids_labeled['sign_up_label']=df_test_ids_labeled['sign_up_label'].fillna("existing")
            df_test_ids_labeled['actual_shopping_label']=self.input_y_test_list
            df_test_ids_labeled['actual_shopping_label']=df_test_ids_labeled['actual_shopping_label'].replace(0,"no").replace(1,"shopper")
            
            self.df_test_ids_labeled_summary=df_test_ids_labeled.groupby(['selection_label','sign_up_label','actual_shopping_label'])['customer_id_hashed'].nunique().to_frame().reset_index()
            self.df_test_ids_labeled=df_test_ids_labeled        
            
            
        
        
        # 14
        def save_outputs(self):
            
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
            str_high_date=str(self.df_date_range.iloc[0,1])
            str_dv_type="DV%i_%s"%(self.n_week_DV,self.key_df_type)
            self.df_train_ids_labeled.to_csv(self.output_folder+"df_train_ids_labeled_%s_%s.csv"%(str_high_date,str_dv_type),index=False)
            self.df_test_ids_labeled.to_csv(self.output_folder+"df_test_ids_labeled_%s_%s.csv"%(str_high_date,str_dv_type),index=False)
            
            

            
            


    # In[22]:


    n_week_DV=3
    key_df_type="trans_1_only"
    print("%s DV %i start: "%(key_df_type,n_week_DV),datetime.datetime.now())
    SM_Logistic_Model_dvN.__init__(self=SM_Logistic_Model_dvN,
                                   n_week_DV=n_week_DV,
                                   key_df_type=key_df_type,
                                   df_date_range=df_date_range,
                                   sql_engine=BL_engine,
                                   dict_cols_in_table=dict_cols_in_table
                                  )
    SM_Logistic_Model_dvN.select_from_model_n_features(SM_Logistic_Model_dvN,N_feature_select_from_models=min(60,int(SM_Logistic_Model_dvN.df_train_X.shape[1]*0.7)))
    SM_Logistic_Model_dvN.select_REF(SM_Logistic_Model_dvN,n_features_to_select=40)
    SM_Logistic_Model_dvN.forwards_feature_elimination_based_on_p_and_vif(SM_Logistic_Model_dvN)
    SM_Logistic_Model_dvN.run_sm_logR_model(SM_Logistic_Model_dvN)
    SM_Logistic_Model_dvN.select_test_df_from_mysql(SM_Logistic_Model_dvN)
    SM_Logistic_Model_dvN.run_updating_df_count(SM_Logistic_Model_dvN)
    SM_Logistic_Model_dvN.generate_DV_distribution(SM_Logistic_Model_dvN)
    SM_Logistic_Model_dvN.pred_test_Y(SM_Logistic_Model_dvN)

    SM_Logistic_Model_dvN.generate_step_table_of_test_SM(SM_Logistic_Model_dvN)
    SM_Logistic_Model_dvN.select_best_scored_pred_prob(SM_Logistic_Model_dvN)
    SM_Logistic_Model_dvN.generate_gain_chart(SM_Logistic_Model_dvN)
    SM_Logistic_Model_dvN.check_shopper_type(SM_Logistic_Model_dvN)

    SM_Logistic_Model_dvN.save_outputs(SM_Logistic_Model_dvN)
    print("%s DV %i done: "%(key_df_type,n_week_DV),datetime.datetime.now())


    # In[23]:


    n_week_DV=2
    key_df_type="trans_2_plus"
    print("%s DV %i start: "%(key_df_type,n_week_DV),datetime.datetime.now())
    SM_Logistic_Model_dvN.__init__(self=SM_Logistic_Model_dvN,
                                   n_week_DV=n_week_DV,
                                   key_df_type=key_df_type,
                                   df_date_range=df_date_range,
                                   sql_engine=BL_engine,
                                   dict_cols_in_table=dict_cols_in_table
                                  )
    SM_Logistic_Model_dvN.select_from_model_n_features(SM_Logistic_Model_dvN,N_feature_select_from_models=min(60,int(SM_Logistic_Model_dvN.df_train_X.shape[1]*0.7)))
    SM_Logistic_Model_dvN.select_REF(SM_Logistic_Model_dvN,n_features_to_select=40)
    SM_Logistic_Model_dvN.forwards_feature_elimination_based_on_p_and_vif(SM_Logistic_Model_dvN)
    SM_Logistic_Model_dvN.run_sm_logR_model(SM_Logistic_Model_dvN)
    SM_Logistic_Model_dvN.select_test_df_from_mysql(SM_Logistic_Model_dvN)
    SM_Logistic_Model_dvN.run_updating_df_count(SM_Logistic_Model_dvN)
    SM_Logistic_Model_dvN.generate_DV_distribution(SM_Logistic_Model_dvN)
    SM_Logistic_Model_dvN.pred_test_Y(SM_Logistic_Model_dvN)

    SM_Logistic_Model_dvN.generate_step_table_of_test_SM(SM_Logistic_Model_dvN)
    SM_Logistic_Model_dvN.select_best_scored_pred_prob(SM_Logistic_Model_dvN)
    SM_Logistic_Model_dvN.generate_gain_chart(SM_Logistic_Model_dvN)
    SM_Logistic_Model_dvN.check_shopper_type(SM_Logistic_Model_dvN)

    SM_Logistic_Model_dvN.save_outputs(SM_Logistic_Model_dvN)
    print("%s DV %i done: "%(key_df_type,n_week_DV),datetime.datetime.now())


    # In[24]:


    print("Job done: model building", datetime.datetime.now())

