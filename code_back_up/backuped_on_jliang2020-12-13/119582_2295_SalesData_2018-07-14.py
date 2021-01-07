
# coding: utf-8

# In[15]:

import pandas as pd
import numpy as np
import os
import datetime
import math
import glob
folderpath = '/home/jian/BiglotsCode/outputs/'
lastweeksdate = '2018-07-07'
this_week_date= "2018_07_14"


# In[16]:

newsalespath=glob.glob("/home/jian/BigLots/MediaStorm_"+this_week_date+"/"+"MediaStormSalesWeekly"+"*.txt")[0]
newtrafficpath=glob.glob("/home/jian/BigLots/MediaStorm_"+this_week_date+"/"+"MediaStormTrafficWeekly"+"*.txt")[0]
newinventorypath=glob.glob("/home/jian/BigLots/MediaStorm_"+this_week_date+"/"+"MediaStormInventoryWeekly"+"*.txt")[0]


# In[21]:

closed_onlinestorelist = ['6990','145']


# In[22]:

df_tradearea_all = pd.read_csv('/home/jian/BiglotsCode/OtherInput/New_TA_info.csv',dtype = 'str')
df_tradearea_all['trade_area_code']=df_tradearea_all['Ta_Info'].apply(lambda x: x.split(" | ")[0])
df_tradearea_all=df_tradearea_all[['location_id','trade_area_code']]


# In[23]:

dfsales = pd.read_csv(folderpath + 'combinedsales'+ lastweeksdate + '.csv',sep = '|',dtype = 'str')
df = pd.read_csv(newsalespath,sep = '|',dtype = 'str')
a = df.columns
print("new sales data column header matches:")
print(a == ['location_id', 'week_end_dt', 'fiscal_week_nbr', 'gross_sales_amt',
       'gross_transaction_cnt', 'class_code_id', 'subclass_id', 'subclass_gross_sales_amt'])

'''print(a == ['location_id', 'week_end_dt', 'fiscal_week_nbr', 'gross_sales_amt',
       'gross_transaction_cnt', 'class_code_id', 'class_gross_sales_amt'])'''
df['subclass_gross_sales_amt']=df['subclass_gross_sales_amt'].astype(float)
df=df.groupby(['location_id','week_end_dt','fiscal_week_nbr','gross_sales_amt','gross_transaction_cnt','class_code_id'])['subclass_gross_sales_amt'].sum().to_frame().reset_index()
df=df.rename(columns={"subclass_gross_sales_amt":"class_gross_sales_amt"})



dfsales = dfsales.append(df,ignore_index = True)
a = (len(dfsales.index))
dfsales = dfsales.drop_duplicates(['location_id', 'week_end_dt', 'fiscal_week_nbr', 
       'class_code_id'])
b = (len(dfsales.index))
if a==b:
    print("")
else:
    print("last week traffic data duplication deduped")


# In[24]:

recentweek = (max(dfsales['week_end_dt']))
recentweek


# In[25]:

dfsales.to_csv(folderpath + 'combinedsales'+ recentweek + '.csv',index = False,sep = '|')


# In[26]:

dfsales = dfsales[~dfsales['location_id'].isin(closed_onlinestorelist)]


# In[27]:

outputpath = folderpath +'Output_' + recentweek +'/'
try:
    os.stat(outputpath)
except:
    os.mkdir(outputpath)


# In[28]:

dfnodata = dfsales[(dfsales['class_gross_sales_amt'] == '?')&                   (dfsales['week_end_dt'] == recentweek)]
# dfnodata.to_csv(outputpath + 'sales_nodata.csv',index = False)
print("stores with ? sales/transaction: " + str(len(dfnodata.index)))


# In[29]:

dfsales['week_end_dt'] = pd.to_datetime(dfsales['week_end_dt'])
dfsales = dfsales[dfsales['class_gross_sales_amt']!='?']
dfsales = dfsales.reset_index(drop = True)

dfsales['gross_sales_amt'] = dfsales['gross_sales_amt'].astype('float')
dfsales['gross_transaction_cnt'] = dfsales['gross_transaction_cnt'].astype('float')
dfsales['class_gross_sales_amt'] = dfsales['class_gross_sales_amt'].astype('float')


# In[30]:

dfweeklist = dfsales[['week_end_dt','fiscal_week_nbr']].drop_duplicates()
dfweeklist = dfweeklist.sort_values('week_end_dt',ascending = False)
dfweeklist.reset_index(drop = True,inplace = True)
dfweeklist.reset_index(inplace = True)

dfweeklist_wow = dfweeklist.copy()
dfweeklist_wow['index'] = dfweeklist_wow['index'] - 1
dfweeklist_wow = dfweeklist_wow[['index','week_end_dt']]
dfweeklist_wow.columns = ['index','weeklastweek']

dfweeklist = dfweeklist[dfweeklist['index']<104]
dfweeklist.reset_index(drop = True,inplace = True)
dfweeklist['year'] = np.ceil((dfweeklist['index'] + 1)/52)

dfweeklist1 = dfweeklist[dfweeklist['year'] == 1]
dfweeklist1 = dfweeklist1[['index', 'week_end_dt', 'fiscal_week_nbr']]
dfweeklist2 = dfweeklist[dfweeklist['year'] == 2]
dfweeklist2 = dfweeklist2[['week_end_dt', 'fiscal_week_nbr']]
dfweeklist2.columns = ['weeklastyear', 'fiscal_week_nbr']
dfweeklist1['rank']=dfweeklist1['week_end_dt'].rank(ascending=True)
dfweeklist2['rank']=dfweeklist2['weeklastyear'].rank(ascending=True)

del dfweeklist2['fiscal_week_nbr']

dfweeklist = pd.merge(dfweeklist1,dfweeklist2,on ='rank' )
dfweeklist = pd.merge(dfweeklist,dfweeklist_wow,on ='index')
del dfweeklist1,dfweeklist2,dfweeklist_wow

dfweeklist.to_csv(outputpath + 'weeklist.csv',index = False)


# In[31]:

recentweek_date = (max(dfsales['week_end_dt']))


# In[32]:

dfcheck = dfsales[dfsales['week_end_dt'] == recentweek_date]


# In[33]:

dfcheck_total1 = dfcheck[['location_id', 'week_end_dt', 'fiscal_week_nbr',
                         'gross_sales_amt','gross_transaction_cnt']].drop_duplicates()
a = (len(dfcheck_total1.index))
dfcheck_total1 = dfcheck_total1.drop_duplicates(['location_id', 'week_end_dt', 'fiscal_week_nbr'])
b = (len(dfcheck_total1.index))
if a==b:
    print("")
else:
    print("last week sales multiple gross sales/trasaction in the same store")

dfcheck_total2 = dfcheck[['location_id', 'week_end_dt', 'fiscal_week_nbr',
                         'class_gross_sales_amt']].groupby(['location_id', 'week_end_dt', 'fiscal_week_nbr']).sum()
dfcheck_total2.reset_index(inplace = True)

dfcheck_total = pd.merge(dfcheck_total1,dfcheck_total2,
                        on = ['location_id', 'week_end_dt', 'fiscal_week_nbr'] ,
                        how = 'outer')

del dfcheck_total1,dfcheck_total2

dfcheck_zero = dfcheck_total[(dfcheck_total['class_gross_sales_amt']<=0)|                            (dfcheck_total['gross_transaction_cnt']<=0) ]

dfcheck_zero.to_csv(outputpath + 'zerosales.csv',index = False)
print("stores with zero sales/transaction: " + str(len(dfcheck_zero.index)))
del dfcheck_zero

dfcheck_total['TotalDiff'] = dfcheck_total['gross_sales_amt']-dfcheck_total['class_gross_sales_amt']
dfcheck_total['TotalDiff'] = dfcheck_total['TotalDiff'].round()
dfcheck_totalnonmatch = dfcheck_total[dfcheck_total['TotalDiff']!=0]
print("stores gross sales can not match sum of class sales: " + str(len(dfcheck_totalnonmatch.index)))

dfcheck_totalnonmatch.to_csv(outputpath + 'totalnonmatch.csv',index = False)
del dfcheck_totalnonmatch

dfcheck_zeroclass = dfcheck[(dfcheck['class_gross_sales_amt']==0)]
dfcheck_zeroclass.to_csv(outputpath + 'zeroclasssales.csv',index = False)
print("stores with zero class sales: " + str(len(dfcheck_zeroclass.index)))
del dfcheck_zeroclass
del dfcheck


# In[34]:

dfsales_total1 = dfsales[['location_id', 'week_end_dt', 'fiscal_week_nbr',
                         'gross_sales_amt','gross_transaction_cnt']].drop_duplicates()
dfsales_total1 = dfsales_total1.drop_duplicates(['location_id', 'week_end_dt', 'fiscal_week_nbr'])

dfsales_total2 = dfsales[['location_id', 'week_end_dt', 'fiscal_week_nbr',
                         'class_gross_sales_amt']].groupby(['location_id', 'week_end_dt', 'fiscal_week_nbr']).sum()
dfsales_total2.reset_index(inplace = True)

dfsales_total = pd.merge(dfsales_total1,dfsales_total2,
                        on = ['location_id', 'week_end_dt', 'fiscal_week_nbr'] ,
                        how = 'outer')
del dfsales_total1,dfsales_total2


# In[35]:

dfstore = pd.read_table('/home/jian/BigLots/static_files/MediaStormStoreList_Nov15.txt',
                        sep = '|',dtype = 'str')
dfstore['open_dt'] = pd.to_datetime(dfstore['open_dt'])
dfstore['open_dtwd'] = dfstore['open_dt'].dt.dayofweek
dfstore['open_wk'] = np.where(dfstore['open_dtwd']<=5,
                       dfstore['open_dt'].apply(lambda x:x+datetime.timedelta(days=(5-x.weekday()))),
                       dfstore['open_dt'].apply(lambda x:x+datetime.timedelta(days=(12-x.weekday()))))

dma = pd.read_csv('/home/jian/BiglotsCode/OtherInput/zipdmamapping.csv',dtype = 'str')
dfstore_exc = dfstore
dfstore_exc['zip_cd'] = dfstore_exc['zip_cd'].str[0:5]
dfstore_exc = pd.merge(dfstore_exc,dma,on = 'zip_cd',how = 'left')


# In[36]:

dfstorematch = dfsales_total[['location_id']].drop_duplicates()
dfstorematch = pd.merge(dfstorematch,dfstore[['location_id','address_line_1']],
                        on = 'location_id',how = 'left')
dfstorematch['address_line_1'].fillna('empty',inplace = True)
dfstorematch = dfstorematch[dfstorematch['address_line_1']=='empty']
print("stores w/o detailed info: ")
print(dfstorematch['location_id'].unique())


# In[37]:

len(dfstorematch['location_id'].unique())


# In[38]:

last_week_closed_stores=['61','290','455','1084','1230','1422','1550','1750','4479','5098','5177','824','5133','4099',
                         '4113','4165','4280','4362','1913','1967','1148','1182','280','388','507','5363','4675','5364','4677']
# 5363 2018-04-28
# 4675 2018-06-02 not updated about TA info
# 5364 2018-06-19 not updated about TA info
# 4677 2018-06-26 not updated about TA info
sorted(dfstorematch['location_id'].unique().tolist())==sorted(last_week_closed_stores)


# In[39]:

[x for x in dfstorematch['location_id'].unique().tolist() if x not in last_week_closed_stores]


# In[ ]:




# In[40]:

len(dfstorematch['location_id'].unique())
del dfstorematch


# In[41]:

dfsales_total = pd.merge(dfsales_total,dfstore[['location_id','open_wk']],
                        on = 'location_id',how = 'left')
dfsales_total['open_wk'].fillna(datetime.datetime.strptime(str(20200101), '%Y%m%d').date(),inplace = True)


# In[42]:

dftraffic = pd.read_csv(folderpath + 'combinedtraffic'+ lastweeksdate + '.csv',
               sep = '|',dtype = 'str')

df = pd.read_csv(newtrafficpath,sep = '|',dtype = 'str')
a = df.columns
print("new traffic data column header matches:")
print(a == ['location_id', 'week_end_dt', 'fiscal_week_nbr', 'traffic_day_1',
       'traffic_day_2', 'traffic_day_3', 'traffic_day_4', 'traffic_day_5',
       'traffic_day_6', 'traffic_day_7'])
dftraffic = dftraffic.append(df,ignore_index = True)
a = (len(dftraffic.index))
dftraffic = dftraffic.drop_duplicates(['location_id', 'week_end_dt', 'fiscal_week_nbr'])
b = (len(dftraffic.index))
if a==b:
    print("")
else:
    print("last week traffic data duplication deduped")
dftraffic.to_csv(folderpath + 'combinedtraffic'+ recentweek + '.csv',index = False,sep = '|')

dftraffic['traffic_week'] = 0 
for i in ['traffic_day_1','traffic_day_2', 'traffic_day_3', 'traffic_day_4',
          'traffic_day_5', 'traffic_day_6', 'traffic_day_7']:
    dftraffic[i] = dftraffic[i].astype('float')
    dftraffic['traffic_week'] = dftraffic['traffic_week'] +dftraffic[i]
dftraffic['week_end_dt'] = pd.to_datetime(dftraffic['week_end_dt'])


# In[43]:

dfinventory = pd.read_csv(folderpath + 'combinedinventory'+ lastweeksdate + '.csv',
               sep = '|',dtype = 'str')

df = pd.read_csv(newinventorypath,sep = '|',dtype = 'str')
a = df.columns
print("new inventory data column header matches:")
print(a == ['location_id', 'week_end_dt', 'fiscal_week_nbr', 'class_code_id','on_hand'])
df.columns = ['location_id', 'week_end_dt', 'fiscal_week_nbr', 'class_code_id','on_hand']
dfinventory = dfinventory.append(df,ignore_index = True)
a = (len(dfinventory.index))
dfinventory = dfinventory.drop_duplicates(['location_id', 'week_end_dt', 'fiscal_week_nbr', 'class_code_id'])
b = (len(dfinventory.index))
if a==b:
    print("")
else:
    print("last week inventory data duplication deduped")


# In[44]:

dfinventory.to_csv(folderpath + 'combinedinventory'+ recentweek + '.csv',index = False,sep = '|')


# In[45]:

dfinventory['week_end_dt'] = pd.to_datetime(dfinventory['week_end_dt'])
dfinventory['on_hand'] = dfinventory['on_hand'].astype('float')
dfinventory_total = dfinventory.groupby(['location_id', 'week_end_dt', 'fiscal_week_nbr']).sum()
dfinventory_total.reset_index(inplace = True)


# In[46]:

dfsales_total_recent = pd.merge(dfsales_total,
                                dfweeklist[['week_end_dt', 'fiscal_week_nbr','weeklastyear','weeklastweek']],
                                on= ['week_end_dt', 'fiscal_week_nbr'])

dfsales_total_lastyear = pd.merge(dfsales_total,
                                 dfweeklist[['weeklastyear']],
                                 left_on= 'week_end_dt',right_on = 'weeklastyear')

dfsales_total_lastyear = dfsales_total_lastyear[['location_id','gross_transaction_cnt', 'class_gross_sales_amt','weeklastyear']]
dfsales_total_lastyear.columns = ['location_id','gross_transaction_cnt_ly', 'class_gross_sales_amt_ly','weeklastyear']

dfsales_total_recent = pd.merge(dfsales_total_recent,dfsales_total_lastyear,
                                on = ['location_id','weeklastyear'],how = 'outer')
dfsales_total_recent.fillna(0,inplace = True)

dfsales_total_recent['Store_Category'] = np.where(dfsales_total_recent['open_wk']>=dfsales_total_recent['weeklastyear'],'New',
                                         np.where((dfsales_total_recent['gross_transaction_cnt_ly']==0)&(dfsales_total_recent['class_gross_sales_amt_ly']==0),
                                         'Converted',
                                         np.where((dfsales_total_recent['gross_transaction_cnt']==0)&(dfsales_total_recent['class_gross_sales_amt']==0),
                                        'Converted','Complete')))

dfsales_total_lastweek = pd.merge(dfsales_total,
                                 dfweeklist[['weeklastweek']],
                                 left_on= 'week_end_dt',right_on = 'weeklastweek')

dfsales_total_lastweek = dfsales_total_lastweek[['location_id','gross_transaction_cnt', 'class_gross_sales_amt','weeklastweek']]
dfsales_total_lastweek.columns = ['location_id','gross_transaction_cnt_lw', 'class_gross_sales_amt_lw','weeklastweek']
dfsales_total_recent = pd.merge(dfsales_total_recent,dfsales_total_lastweek,
                                on = ['location_id','weeklastweek'],how = 'left')
dfsales_total_recent.fillna(0,inplace = True)

dfsales_total_recent['week_end_dt'] = np.where(dfsales_total_recent['week_end_dt']=='1970-01-01',
                                       dfsales_total_recent['weeklastyear'] + pd.DateOffset(364),
                                       dfsales_total_recent['week_end_dt'])
dfsales_total_recent['weeklastyear'] = np.where(dfsales_total_recent['weeklastyear']=='1970-01-01',
                                       dfsales_total_recent['week_end_dt'] + pd.DateOffset(-364),
                                       dfsales_total_recent['weeklastyear'])


# In[47]:

dfallstorelist = dfstore[~dfstore['location_id'].isin(closed_onlinestorelist)]
dfallstorelist.reset_index(drop = True, inplace = True)
dfweeklist2 = dfweeklist.copy()
dfallstorelist['concat'] = 1
dfweeklist2['concat'] = 1
dfallstorelist = pd.merge(dfallstorelist,dfweeklist2,on='concat')
dfallstorelist = dfallstorelist[['location_id','week_end_dt']]
dfallstorelist = pd.merge(dfallstorelist,dfsales_total_recent,on=['location_id','week_end_dt'],
                         how = 'left')

dfallstorelist.fillna(0,inplace = True)
dfallstorelist = dfallstorelist[(dfallstorelist['gross_sales_amt']==0)&                               (dfallstorelist['gross_transaction_cnt']==0)&                               (dfallstorelist['class_gross_sales_amt']==0)&                               (dfallstorelist['gross_transaction_cnt_ly']==0)&                               (dfallstorelist['class_gross_sales_amt_ly']==0)]
dfallstorelist = dfallstorelist.sort_values(['week_end_dt','location_id'],ascending = [0,1])
dfallstorelist = dfallstorelist[['location_id','week_end_dt']]

x_temp=dfsales_total_recent[['week_end_dt','fiscal_week_nbr']]
y_temp=x_temp.drop_duplicates()
y_temp=y_temp[y_temp['fiscal_week_nbr']!="0"]
y_temp=y_temp[y_temp['fiscal_week_nbr']!=0]
dfallstorelist = pd.merge(dfallstorelist,y_temp,on="week_end_dt",how="left")
dfallstorelist = pd.merge(dfallstorelist,dfstore_exc,on="location_id",how='left')
dfallstorelist = pd.merge(dfallstorelist,df_tradearea_all,on="location_id",how='left')
del dfallstorelist['open_dtwd']
del dfallstorelist['open_wk']

# dfallstorelist.to_csv(outputpath + 'nobothyeardatastores.csv',index = False)
print("stores with no 2017&2016 sales and transaction data: " + str(len(dfallstorelist.index)))
test = dfallstorelist[dfallstorelist['week_end_dt']==recentweek_date]
print("Last Week: stores with no 2017&2016 sales and transaction data: " + str(len(test.index)))
# del test,dfweeklist2,dfallstorelist


# For later use to add index

Recent_52_Week_nbr=dfweeklist[['week_end_dt','index']]
Recent_52_Week_nbr['52_Weeks_nbr']=52-Recent_52_Week_nbr['index']
del Recent_52_Week_nbr['index']

# Recent_52_Week_nbr


# In[48]:

dfsales_total_recent = pd.merge(dfsales_total_recent,dftraffic,
                                on=['location_id', 'week_end_dt', 'fiscal_week_nbr'],how = 'left')

dftraffic2 = dftraffic[['location_id', 'week_end_dt','traffic_day_1',
       'traffic_day_2', 'traffic_day_3', 'traffic_day_4', 'traffic_day_5',
       'traffic_day_6', 'traffic_day_7','traffic_week']]
dftraffic2.columns = ['location_id', 'weeklastyear','traffic_day_1_ly',
       'traffic_day_2_ly', 'traffic_day_3_ly', 'traffic_day_4_ly', 'traffic_day_5_ly',
       'traffic_day_6_ly', 'traffic_day_7_ly','traffic_week_ly']

dfsales_total_recent = pd.merge(dfsales_total_recent,dftraffic2,
                                on=['location_id', 'weeklastyear'],how = 'left')
del dftraffic2


# In[49]:

dfsales_total_recent = pd.merge(dfsales_total_recent,dfinventory_total,
                                on=['location_id', 'week_end_dt', 'fiscal_week_nbr'],how = 'left')

dfinventory_total2 = dfinventory_total[['location_id', 'week_end_dt','on_hand']]
dfinventory_total2.columns = ['location_id', 'weeklastyear','on_hand_ly']

dfsales_total_recent = pd.merge(dfsales_total_recent,dfinventory_total2,
                                on=['location_id', 'weeklastyear'],how = 'left')
del dfinventory_total2


# In[50]:

recentweek_last=datetime.datetime.strptime(recentweek, '%Y-%m-%d').date()
recentweek_last=recentweek_last+datetime.timedelta(days=(-84))


# In[51]:

dfsales_total_recent['yoysales'] = dfsales_total_recent['class_gross_sales_amt']/dfsales_total_recent['class_gross_sales_amt_ly'] - 1
dfsales_total_recent['yoytrans'] = dfsales_total_recent['gross_transaction_cnt']/dfsales_total_recent['gross_transaction_cnt_ly'] - 1
dfsales_total_recent['wowsales'] = dfsales_total_recent['class_gross_sales_amt']/dfsales_total_recent['class_gross_sales_amt_lw'] - 1
dfsales_total_recent['wowtrans'] = dfsales_total_recent['gross_transaction_cnt']/dfsales_total_recent['gross_transaction_cnt_lw'] - 1


# In[52]:

dfsales_total_recent_delete = dfsales_total_recent[(dfsales_total_recent['Store_Category']=='Complete')&                                                   ((abs(dfsales_total_recent['yoysales'])>0.2)&                                                   (abs(dfsales_total_recent['yoytrans'])>0.2))]#|\
                                                   #(abs(dfsales_total_recent['wowsales'])>0.2)|\
                                                   #(abs(dfsales_total_recent['wowtrans'])>0.2))]
dfsales_total_recent_delete = dfsales_total_recent_delete.sort_values(['week_end_dt','location_id'],
                                                                     ascending = [0,1])

dfsales_total_recent_delete=pd.merge(dfsales_total_recent_delete,dfstore_exc,on="location_id",how='left')
dfsales_total_recent_delete = pd.merge(dfsales_total_recent_delete,df_tradearea_all,on='location_id',how='left')

del dfsales_total_recent_delete['open_dtwd']
del dfsales_total_recent_delete['open_wk_y']
dfsales_total_recent_delete.rename(index=str, columns={"open_wk_x": "open_wk"})

dfsales_total_recent_delete = pd.merge(dfsales_total_recent_delete,Recent_52_Week_nbr,on="week_end_dt",how='left')
# dfsales_total_recent_delete.to_csv(outputpath + 'highyoy_wowchangestores.csv',index = False)
print("stores with high yoy change: " + str(len(dfsales_total_recent_delete.index)))
test = dfsales_total_recent_delete[dfsales_total_recent_delete['week_end_dt']==recentweek_date]
print("Last Week: stores with high yoy change: " + str(len(test.index)))
del test


# In[53]:

#dfsales_total_recent =  dfsales_total_recent[(dfsales_total_recent['gross_transaction_cnt']!=0)|\
#                                             (dfsales_total_recent['class_gross_sales_amt']!=0)]
dfsales_total_recent = dfsales_total_recent[(dfsales_total_recent['Store_Category']!='Complete')|                                            ((dfsales_total_recent['Store_Category']=='Complete')&                                            (abs(dfsales_total_recent['yoysales'])<=0.2)|                                            (abs(dfsales_total_recent['yoytrans'])<=0.2))]#&\
                                            #(abs(dfsales_total_recent['wowsales'])<=0.2)&\
                                            #(abs(dfsales_total_recent['wowtrans'])<=0.2))]


# In[54]:

dfweeklist2 = dfweeklist[['week_end_dt']]
dfweeklist2['week_end_dt_8w'] = dfweeklist2['week_end_dt']+pd.DateOffset(-84)
# Name "week_end_dt_8w" reflects 12 weeks, not 8


# In[55]:

dfweeklist_12plus = dfsales[['week_end_dt']].drop_duplicates()
dfweeklist_12plus = dfweeklist_12plus.sort_values('week_end_dt',ascending = False)
dfweeklist_12plus.reset_index(drop = True,inplace = True)
dfweeklist_12plus.reset_index(inplace = True)
dfweeklist_12plus = dfweeklist_12plus[dfweeklist_12plus['index']<64]
dfweeklist_12plus['weeklastyear'] = dfweeklist_12plus['week_end_dt'] + pd.DateOffset(-364)


# In[56]:

dfsales_12plus = pd.merge(dfsales_total,dfweeklist_12plus,on= ['week_end_dt'])

dfsales_12plus_lastyear = pd.merge(dfsales_total,
                                 dfweeklist_12plus[['weeklastyear']],
                                 left_on= 'week_end_dt',right_on = 'weeklastyear')

dfsales_12plus_lastyear = dfsales_12plus_lastyear[['location_id','gross_transaction_cnt', 'class_gross_sales_amt','weeklastyear']]
dfsales_12plus_lastyear.columns = ['location_id','gross_transaction_cnt_ly', 'class_gross_sales_amt_ly','weeklastyear']

dfsales_12plus = pd.merge(dfsales_12plus,dfsales_12plus_lastyear,
                                on = ['location_id','weeklastyear'],how = 'left')
dfsales_12plus.fillna(0,inplace = True)

dfsales_12plus['Store_Category'] = np.where(dfsales_12plus['open_wk']>=dfsales_12plus['weeklastyear'],'New',
                                         np.where((dfsales_12plus['gross_transaction_cnt_ly']==0)&(dfsales_12plus['class_gross_sales_amt_ly']==0),
                                         'Converted',
                                         np.where((dfsales_12plus['gross_transaction_cnt']==0)&(dfsales_12plus['class_gross_sales_amt']==0),
                                        'Converted','Complete')))
dfsales_12plus['yoysales'] = dfsales_12plus['class_gross_sales_amt']/dfsales_12plus['class_gross_sales_amt_ly'] - 1
dfsales_12plus['yoytrans'] = dfsales_12plus['gross_transaction_cnt']/dfsales_12plus['gross_transaction_cnt_ly'] - 1
dfsales_12plus = dfsales_12plus[(dfsales_12plus['Store_Category']!='Complete')|                                            ((dfsales_12plus['Store_Category']=='Complete')&                                            (abs(dfsales_12plus['yoysales'])<=0.2)|                                            (abs(dfsales_12plus['yoytrans'])<=0.2))]#&\
dfsales_12plus = dfsales_12plus[['location_id','week_end_dt']]


# In[57]:

# Finishing reading here
dfsales_rankingall = pd.DataFrame()
for i in range(52):
    cweekdate = dfweeklist2['week_end_dt'][i]
    recentweek_last = dfweeklist2['week_end_dt_8w'][i]
    dfsales_ranking = dfsales_total[dfsales_total['week_end_dt']>recentweek_last]
    dfsales_ranking = dfsales_ranking[dfsales_ranking['week_end_dt']<=cweekdate]
    dfsales_ranking = pd.merge(dfsales_ranking,dfsales_12plus,
                           on = ['location_id', 'week_end_dt'])
    dfsales_ranking = pd.merge(dfsales_ranking,
                               dftraffic[['location_id', 'week_end_dt', 'fiscal_week_nbr','traffic_week']],
                               on =['location_id', 'week_end_dt', 'fiscal_week_nbr'],how = 'left')
    dfsales_ranking.fillna(0,inplace = True)
    dfsales_ranking = dfsales_ranking[['location_id','class_gross_sales_amt','traffic_week']].groupby('location_id').sum()
    dfsales_ranking['Rev/Traffic'] = dfsales_ranking['class_gross_sales_amt']/dfsales_ranking['traffic_week']
    dfsales_ranking.reset_index(inplace = True)
    dfsales_ranking = dfsales_ranking.sort_values('class_gross_sales_amt',ascending = False)
    dfsales_ranking.reset_index(drop = True,inplace = True)
    dfsales_ranking.reset_index(inplace = True)
    dfsales_ranking = dfsales_ranking.rename(columns = {'index':'rev_index'})
    dfsales_ranking = dfsales_ranking.replace(np.inf, 0)
    
    dfsales_ranking = dfsales_ranking.sort_values('Rev/Traffic',ascending = False)
    dfsales_ranking.reset_index(drop = True,inplace = True)
    dfsales_ranking.reset_index(inplace = True)
    dfsales_ranking = dfsales_ranking.rename(columns = {'index':'traffi_index'})
    
    cols = len(dfsales_ranking.index)
    dfsales_ranking['Store_Revenue_Rank'] = np.where(dfsales_ranking['rev_index']/cols <=0.2,'H',
                                                    np.where(dfsales_ranking['rev_index']/cols <=0.8,'M','L'))
    dfsales_ranking['Store_Revenue/Traffic_Rank'] = np.where(dfsales_ranking['traffi_index']/cols <=0.2,'H',
                                                    np.where(dfsales_ranking['traffi_index']/cols <=0.8,'M','L'))
    dfsales_ranking['week_end_dt'] = cweekdate
    dfsales_rankingall = dfsales_rankingall.append(dfsales_ranking,ignore_index = False)


# In[58]:

dfsales_total_recent.fillna(0,inplace = True)
dfsales_total_recent.reset_index(drop = True,inplace = True)
dfsales_total_recent = pd.merge(dfsales_total_recent,
                                dfsales_rankingall[['location_id','week_end_dt','Store_Revenue_Rank','Store_Revenue/Traffic_Rank']],
                                on = ['location_id','week_end_dt'],how = 'left')
dfsales_total_recent['Store_Revenue_Rank'].fillna('NA',inplace = True)
dfsales_total_recent['Store_Revenue/Traffic_Rank'].fillna('NA',inplace = True)


# In[59]:

dfsales_total_recent['AOV'] = dfsales_total_recent['class_gross_sales_amt']/dfsales_total_recent['gross_transaction_cnt']
dfsales_total_recent['AOV_ly'] = dfsales_total_recent['class_gross_sales_amt_ly']/dfsales_total_recent['gross_transaction_cnt_ly']
dfsales_total_recent['Trans/Traffic'] = dfsales_total_recent['gross_transaction_cnt']/dfsales_total_recent['traffic_week']
dfsales_total_recent['Trans/Traffic_ly'] = dfsales_total_recent['gross_transaction_cnt_ly']/dfsales_total_recent['traffic_week_ly']


# In[60]:

dfsales_total_recent['Store_Category'].unique()


# In[61]:

df_complete = dfsales_total_recent[dfsales_total_recent['Store_Category']=='Complete']
df_complete.reset_index(drop = True, inplace = True)
metricslist = ['class_gross_sales_amt','gross_transaction_cnt','AOV','traffic_week',
              'Trans/Traffic', 'traffic_day_1', 'traffic_day_2', 'traffic_day_3',
              'traffic_day_4', 'traffic_day_5', 'traffic_day_6', 'traffic_day_7','on_hand']
columnheader = ['location_id', 'week_end_dt', 'fiscal_week_nbr', 'Store_Revenue_Rank', 'Store_Revenue/Traffic_Rank']
for i in metricslist:
    a = i+'_ly'
    b = i+ 'YoYDiff'
    columnheader = columnheader + [i,a,b]
    df_complete[b] = df_complete[i]/df_complete[a] - 1


# In[62]:

del dfstore['open_dtwd']
del dfstore['open_wk']


# In[63]:

dma = pd.read_csv('/home/jian/BiglotsCode/OtherInput/zipdmamapping.csv',dtype = 'str')
dfstore['zip_cd'] = dfstore['zip_cd'].str[0:5]
dfstore = pd.merge(dfstore,dma,on = 'zip_cd',how = 'left')


# In[64]:

df_complete = df_complete[columnheader]
df_complete = pd.merge(df_complete,dfstore,on='location_id',how = 'left')
df_complete = df_complete.sort_values(['location_id','week_end_dt'],ascending = [1,0])


# In[65]:

df_new = dfsales_total_recent[dfsales_total_recent['Store_Category']=='New']
df_new = df_new[['location_id', 'week_end_dt', 'fiscal_week_nbr',
                 'Store_Revenue_Rank', 'Store_Revenue/Traffic_Rank',
                 'class_gross_sales_amt','gross_transaction_cnt','AOV','traffic_week',
                 'Trans/Traffic','on_hand','Store_Category']]
df_new = pd.merge(df_new,dfstore_exc,on='location_id',how = 'left')
df_new = df_new.sort_values(['location_id','week_end_dt'],ascending = [1,0])
df_new = pd.merge(df_new,df_tradearea_all,on='location_id',how='left')
del df_new['open_wk']
del df_new['open_dtwd']
del df_new['Store_Category']
df_new = pd.merge(df_new,Recent_52_Week_nbr,on="week_end_dt",how='left')

# df_new.to_csv(outputpath + 'output2_new.csv',index = False)


# In[66]:

dfsales_total_recent['Store_Category'].unique()


# In[67]:

dfnodata=pd.merge(dfnodata,dfstore_exc,on="location_id",how='left')
dfnodata = pd.merge(dfnodata,df_tradearea_all,on='location_id',how='left')
dfnodata = pd.merge(dfnodata,Recent_52_Week_nbr,on="week_end_dt",how='left')

# dfnodata.to_csv(outputpath + 'sales_nodata.csv',index = False)



df_converted = dfsales_total_recent[dfsales_total_recent['Store_Category']=='Converted']
del df_converted['fiscal_week_nbr']
df_converted['week_end_dt'] = np.where(df_converted['week_end_dt']=='1970-01-01',
                                       df_converted['weeklastyear'] + pd.DateOffset(364),
                                       df_converted['week_end_dt'])
df_converted['weeklastyear'] = np.where(df_converted['weeklastyear']=='1970-01-01',
                                       df_converted['week_end_dt'] + pd.DateOffset(-364),
                                       df_converted['weeklastyear'])

df_converted = pd.merge(df_converted,dfweeklist[['week_end_dt','fiscal_week_nbr']],
                       on = 'week_end_dt',how='left')
df_converted = df_converted[['location_id', 'week_end_dt', 'fiscal_week_nbr',
                 'Store_Revenue_Rank', 'Store_Revenue/Traffic_Rank',
                 'class_gross_sales_amt','gross_transaction_cnt','AOV','traffic_week',
                 'Trans/Traffic','on_hand','Store_Category',
                 'weeklastyear','gross_transaction_cnt_ly','class_gross_sales_amt_ly']]
df_converted = pd.merge(df_converted,dfstore_exc,on='location_id',how = 'left')
df_converted = df_converted.sort_values(['location_id','week_end_dt'],ascending = [1,0])

df_converted = pd.merge(df_converted,Recent_52_Week_nbr,on="week_end_dt",how='left')


del df_converted['Store_Category']
df_converted = pd.merge(df_converted,df_tradearea_all,on='location_id',how='left')

del df_converted['open_wk']
del df_converted['open_dtwd']


# df_converted.to_csv(outputpath + 'output3_converted.csv',index = False)


# In[68]:

TA_Information = pd.read_csv("/home/jian/BiglotsCode/OtherInput/New_TA_info.csv")
TA_Information['trade_area_code']=TA_Information['Ta_Info'].apply(lambda x: x.split(" | ")[0])
TA_Information=TA_Information[['trade_area_code','Ta_Info']]
TA_Information=TA_Information.drop_duplicates()
df_ta_info_na=pd.DataFrame(data = {'trade_area_code':['N0'], 'Ta_Info':['NA']})
TA_Information=TA_Information.append(df_ta_info_na,ignore_index=True)


# In[69]:

df_complete = pd.merge(df_complete,df_tradearea_all,on ='location_id',how = 'left')
df_complete['trade_area_code'].fillna('N0',inplace = True)

df_complete = pd.merge(df_complete,Recent_52_Week_nbr,on='week_end_dt',how='left')


df_complete = pd.merge(df_complete,TA_Information,on='trade_area_code',how='left')
df_complete_new_list = df_complete.columns.tolist()
df_complete_new_list = df_complete_new_list[-1:]+df_complete_new_list[:-1]
df_complete = df_complete[df_complete_new_list]

df_complete['Store_Info'] = df_complete['location_id'].map(str)+" | "+df_complete['city_nm']+                        " | "+df_complete['state_nm']
df_complete_new_list = df_complete.columns.tolist()
df_complete_new_list = df_complete_new_list[-1:]+df_complete_new_list[:-1]
df_complete = df_complete[df_complete_new_list]


# df_complete['revenue_trigger'] = np.where(abs(df_complete['class_gross_sales_amtYoYDiff'])>0.1,1,0)
# df_complete['transaction_trigger'] = np.where(abs(df_complete['gross_transaction_cntYoYDiff'])>0.1,1,0)
# df_complete['conversion_trigger'] = np.where(abs(df_complete['Trans/TrafficYoYDiff'])>0.1,1,0)

# df_complete.to_csv(outputpath + 'output1.csv',index = False)


# In[70]:

df_complete.head(2)


# In[71]:

# Rewrite for adding Store Key and Trade Area Key

Write_df_list = [df_converted,dfnodata,df_new,dfsales_total_recent_delete,dfallstorelist]
for df in Write_df_list:

    df['trade_area_code'].fillna("N0",inplace = True)
    df['Store_Numeric']=df['location_id'].astype('str')
    df['TA_Numeric']=df['trade_area_code'].apply(lambda x: x[1:len(x)])
                                                 
    df['Store_Numeric']= df['Store_Numeric'].apply(lambda x: x.zfill(4))
    df['Store_Key_1']=df['Store_Numeric'].str[0:2]
    df['Store_Key_2']=df['Store_Numeric'].str[2:4]
    df['Store_Key_1']=df['Store_Key_1'].astype('int')
    df['Store_Key_2']=df['Store_Key_2'].astype('int')
    
    
    df['TA_Numeric']= df['TA_Numeric'].apply(lambda x: x.zfill(4))
    df['TA_Key_1']=df['TA_Numeric'].str[0:2]
    df['TA_Key_2']=df['TA_Numeric'].str[2:4]
    df['TA_Key_1']=df['TA_Key_1'].astype('int')
    df['TA_Key_2']=df['TA_Key_2'].astype('int')

    del df['TA_Numeric']
    del df['Store_Numeric']

    
Write_df_list2 = [df_converted,dfnodata,df_new,dfsales_total_recent_delete,dfallstorelist]
# TA_Information['trade_area_code']=TA_Information['trade_area_code'].astype('int')
Write_df_list3 = []
for df in Write_df_list2:
    df['trade_area_code'].fillna("N0",inplace = True)
    # df['trade_area_code']=df['trade_area_code'].astype('int')
    
    df = pd.merge(df,TA_Information,on='trade_area_code',how='left')
    
    df_complete_new_list = df.columns.tolist()
    df_complete_new_list = df_complete_new_list[-1:]+df_complete_new_list[:-1]
    df = df[df_complete_new_list]

    df['Store_Info'] = df['location_id'].map(str)+" | "+df['city_nm'].map(str)+                        " | "+df['state_nm'].map(str)
    df_complete_new_list = df.columns.tolist()
    df_complete_new_list = df_complete_new_list[-1:]+df_complete_new_list[:-1]
    df = df[df_complete_new_list]    
    Write_df_list3.append(df)
    

df_converted=Write_df_list3[0]
dfnodata=Write_df_list3[1]
df_new=Write_df_list3[2]
dfsales_total_recent_delete=Write_df_list3[3]
dfallstorelist=Write_df_list3[4]
dfallstorelist=dfallstorelist[dfallstorelist['fiscal_week_nbr']!=0]

df_complete.to_csv(outputpath + 'output1.csv',index = False)
df_converted.to_csv(outputpath + 'output3_converted'+' '+recentweek+'.csv',index = False)
dfnodata.to_csv(outputpath + 'sales_nodata'+' '+recentweek+'.csv',index = False)
df_new.to_csv(outputpath + 'output2_new'+' '+recentweek+'.csv',index = False)
dfsales_total_recent_delete.to_csv(outputpath + 'highyoy_wowchangestores'+' '+recentweek+'.csv',index = False)
dfallstorelist.to_csv(outputpath + 'nobothyeardatastores'+' '+recentweek+'.csv',index = False)



# In[72]:

'''
# Wide to long

df_output1_17=df_complete[['location_id','week_end_dt', 'fiscal_week_nbr', 'Store_Revenue_Rank',
                           'Store_Revenue/Traffic_Rank', 'class_gross_sales_amt',
                           'gross_transaction_cnt','traffic_week','on_hand','location_desc', 'open_dt',
                           'address_line_1', 'address_line_2', 'city_nm', 'state_nm', 'zip_cd',
                           'longitude_meas', 'latitude_meas', 'DMA', 'trade_area_code']]
# df_output1_17=pd.merge(df_output1_17,Recent_52_Week_nbr,on="week_end_dt",how="left")



df_output1_16=df_complete[['location_id','week_end_dt', 'fiscal_week_nbr', 'Store_Revenue_Rank',
                           'Store_Revenue/Traffic_Rank', 'class_gross_sales_amt_ly',
                           'gross_transaction_cnt_ly','traffic_week_ly','on_hand_ly','location_desc', 'open_dt',
                           'address_line_1', 'address_line_2', 'city_nm', 'state_nm', 'zip_cd',
                           'longitude_meas', 'latitude_meas', 'DMA', 'trade_area_code']]
# df_output1_16=pd.merge(df_output1_16,Recent_52_Week_nbr,on="week_end_dt",how="left")

df_output1_16['week_end_dt']=df_output1_16['week_end_dt']+pd.DateOffset(-364)

df_output1_16.columns=['location_id','week_end_dt', 'fiscal_week_nbr', 'class_gross_sales_amt',
                           'gross_transaction_cnt','traffic_week','on_hand','location_desc', 'open_dt',
                           'address_line_1', 'address_line_2', 'city_nm', 'state_nm', 'zip_cd',
                           'longitude_meas', 'latitude_meas', 'DMA', 'trade_area_code']

df_output1_datorama=pd.concat([df_output1_17,df_output1_16])
df_output1_datorama=df_output1_datorama.reindex_axis(df_output1_17.columns,axis=1)
df_output1_datorama.to_csv(outputpath + 'output1_datorama.csv',index = False)
'''

# del df_output1_16
# del df_output1_17


# In[73]:

df_tadata = df_complete.groupby(['week_end_dt', 'fiscal_week_nbr','trade_area_code']).sum()
df_tadata.reset_index(inplace = True)

df_tadata['AOV'] = df_tadata['class_gross_sales_amt']/df_tadata['gross_transaction_cnt']
df_tadata['AOV_ly'] = df_tadata['class_gross_sales_amt_ly']/df_tadata['gross_transaction_cnt_ly']
df_tadata['Trans/Traffic'] = df_tadata['gross_transaction_cnt']/df_tadata['traffic_week']
df_tadata['Trans/Traffic_ly'] = df_tadata['gross_transaction_cnt_ly']/df_tadata['traffic_week_ly']

metricslist = ['class_gross_sales_amt','gross_transaction_cnt','AOV','traffic_week',
              'Trans/Traffic', 'traffic_day_1', 'traffic_day_2', 'traffic_day_3',
              'traffic_day_4', 'traffic_day_5', 'traffic_day_6', 'traffic_day_7','on_hand']
for i in metricslist:
    a = i+'_ly'
    b = i+ 'YoYDiff'
    df_tadata[b] = df_tadata[i]/df_tadata[a] - 1


# In[74]:

df_taclass1 = df_complete[['location_id','trade_area_code','week_end_dt','Store_Revenue_Rank',
                           'Store_Revenue/Traffic_Rank']]
df_taclass1.reset_index(drop = True, inplace = True)
df_taclass1['Number_of_HMStores'] = np.where(df_taclass1['Store_Revenue_Rank']!='L',1,0)
df_taclass1['Number_of_HMStores_RevTrafRank'] = np.where(df_taclass1['Store_Revenue/Traffic_Rank']!='L',1,0)
df_taclass1['Number of Stores'] = 1
df_taclass1 = df_taclass1.groupby(['trade_area_code','week_end_dt']).sum()
df_taclass1.reset_index(inplace = True)

df_taclass2 = df_complete[['trade_area_code','week_end_dt','zip_cd']].drop_duplicates()
df_taclass2 = df_taclass2.groupby(['trade_area_code','week_end_dt']).count()
df_taclass2.columns = ['NumberofZipcodes']
df_taclass2.reset_index(inplace = True)

df_taclass3 = df_complete[['trade_area_code','state_nm']].drop_duplicates(['trade_area_code'])

df_tadata = pd.merge(df_tadata,df_taclass1,on =['trade_area_code','week_end_dt'])
df_tadata = pd.merge(df_tadata,df_taclass2,on =['trade_area_code','week_end_dt'])
df_tadata = pd.merge(df_tadata,df_taclass3,on =['trade_area_code'])

df_tadata = df_tadata.sort_values(['trade_area_code','week_end_dt'],ascending = [1,0])
df_tadata.to_csv(outputpath + 'output4.csv',index = False)


# In[75]:

df_dmadata = df_complete.groupby(['week_end_dt', 'fiscal_week_nbr','DMA']).sum()
df_dmadata.reset_index(inplace = True)

df_dmadata['AOV'] = df_dmadata['class_gross_sales_amt']/df_dmadata['gross_transaction_cnt']
df_dmadata['AOV_ly'] = df_dmadata['class_gross_sales_amt_ly']/df_dmadata['gross_transaction_cnt_ly']
df_dmadata['Trans/Traffic'] = df_dmadata['gross_transaction_cnt']/df_dmadata['traffic_week']
df_dmadata['Trans/Traffic_ly'] = df_dmadata['gross_transaction_cnt_ly']/df_dmadata['traffic_week_ly']

metricslist = ['class_gross_sales_amt','gross_transaction_cnt','AOV','traffic_week',
              'Trans/Traffic', 'traffic_day_1', 'traffic_day_2', 'traffic_day_3',
              'traffic_day_4', 'traffic_day_5', 'traffic_day_6', 'traffic_day_7','on_hand']
for i in metricslist:
    a = i+'_ly'
    b = i+ 'YoYDiff'
    df_dmadata[b] = df_dmadata[i]/df_dmadata[a] - 1


# In[76]:

df_dmadata1 = df_complete[['location_id','DMA','week_end_dt','Store_Revenue_Rank',
                           'Store_Revenue/Traffic_Rank']]
df_dmadata1.reset_index(drop = True, inplace = True)
df_dmadata1['Number_of_HMStores'] = np.where(df_dmadata1['Store_Revenue_Rank']!='L',1,0)
df_dmadata1['Number_of_HMStores_RevTrafRank'] = np.where(df_dmadata1['Store_Revenue/Traffic_Rank']!='L',1,0)
df_dmadata1['Number of Stores'] = 1
df_dmadata1 = df_dmadata1.groupby(['DMA','week_end_dt']).sum()
df_dmadata1.reset_index(inplace = True)

df_dmadata2 = df_complete[['DMA','week_end_dt','zip_cd']].drop_duplicates()
df_dmadata2 = df_dmadata2.groupby(['DMA','week_end_dt']).count()
df_dmadata2.columns = ['NumberofZipcodes']
df_dmadata2.reset_index(inplace = True)

df_dmadata4 = df_complete[['DMA','week_end_dt','trade_area_code']].drop_duplicates()
df_dmadata4 = df_dmadata4.groupby(['DMA','week_end_dt']).count()
df_dmadata4.columns = ['NumberofTAs']
df_dmadata4.reset_index(inplace = True)

df_dmadata3 = df_complete[['DMA','state_nm']].drop_duplicates(['DMA'])

df_dmadata = pd.merge(df_dmadata,df_dmadata1,on =['DMA','week_end_dt'])
df_dmadata = pd.merge(df_dmadata,df_dmadata2,on =['DMA','week_end_dt'])
df_dmadata = pd.merge(df_dmadata,df_dmadata4,on =['DMA','week_end_dt'])
df_dmadata = pd.merge(df_dmadata,df_dmadata3,on =['DMA'])

df_dmadata = df_dmadata.sort_values(['DMA','week_end_dt'],ascending = [1,0])
df_dmadata.to_csv(outputpath + 'output5.csv',index = False)
del df_dmadata1,df_dmadata2,df_dmadata3,df_dmadata4


# In[77]:

df_storeweeklist = pd.merge(df_complete[['location_id', 'week_end_dt']],
                            dfweeklist[['week_end_dt','weeklastyear']],on ='week_end_dt')


# In[ ]:




# In[ ]:

dfsales_class1 = pd.merge(dfsales[['location_id', 'week_end_dt','class_code_id','class_gross_sales_amt']],
                          df_storeweeklist,
                          on = ['location_id', 'week_end_dt'])
dfsales_class2 = pd.merge(dfsales[['location_id', 'week_end_dt', 'class_code_id','class_gross_sales_amt']],
                          df_storeweeklist,
                          left_on = ['location_id', 'week_end_dt'],
                          right_on = ['location_id', 'weeklastyear'])

del dfsales_class2['week_end_dt_x']
dfsales_class2 = dfsales_class2.rename(columns = {'week_end_dt_y':'week_end_dt'})

dfsales_class1 = pd.merge(dfsales_class1,dfsales_class2,
                          on =['location_id','week_end_dt','class_code_id'],how='outer')
dfsales_class1.fillna(0,inplace = True)
del dfsales_class2
del dfsales_class1['weeklastyear_x']
del dfsales_class1['weeklastyear_y']
dfsales_class1 = dfsales_class1.rename(columns = {'class_gross_sales_amt_x':'class_gross_sales_amt'})
dfsales_class1 = dfsales_class1.rename(columns = {'class_gross_sales_amt_y':'class_gross_sales_amt_ly'})
dfsales_class1['class_gross_sales_amt_yoy'] = dfsales_class1['class_gross_sales_amt']/dfsales_class1['class_gross_sales_amt_ly'] -1


# In[ ]:

dfinventory_class1 = pd.merge(dfinventory[['location_id', 'week_end_dt', 'class_code_id','on_hand']], 
                              df_storeweeklist,
                          on = ['location_id', 'week_end_dt'])
dfinventory_class2 = pd.merge(dfinventory[['location_id', 'week_end_dt', 'class_code_id','on_hand']],
                          df_storeweeklist,
                          left_on = ['location_id', 'week_end_dt'],
                          right_on = ['location_id', 'weeklastyear'])

del dfinventory_class2['week_end_dt_x']
dfinventory_class2 = dfinventory_class2.rename(columns = {'week_end_dt_y':'week_end_dt'})

dfinventory_class1 = pd.merge(dfinventory_class1,dfinventory_class2,
                          on =['location_id','week_end_dt','class_code_id'],how='outer')
dfinventory_class1.fillna(0,inplace = True)
del dfinventory_class2
del dfinventory_class1['weeklastyear_x']
del dfinventory_class1['weeklastyear_y']
dfinventory_class1 = dfinventory_class1.rename(columns = {'on_hand_x':'on_hand'})
dfinventory_class1 = dfinventory_class1.rename(columns = {'on_hand_y':'on_hand_ly'})
dfinventory_class1['on_hand_yoy'] = dfinventory_class1['on_hand']/dfinventory_class1['on_hand_ly'] -1


# In[ ]:

df_tadetail = pd.merge(dfsales_class1,dfinventory_class1,
                      on=['location_id','week_end_dt','class_code_id'],how='outer')
df_tadetail = pd.merge(df_tadetail,df_tradearea_all,on ='location_id',how = 'left')
df_tadetail['trade_area_code'].fillna('NA',inplace = True)
df_tadetail.fillna(0,inplace = True)

df_tadetail = df_tadetail.groupby(['trade_area_code','week_end_dt','class_code_id']).sum()
df_tadetail['class_gross_sales_amt_yoy'] = df_tadetail['class_gross_sales_amt']/df_tadetail['class_gross_sales_amt_ly'] -1
df_tadetail['on_hand_yoy'] = df_tadetail['on_hand']/df_tadetail['on_hand_ly'] -1
df_tadetail.reset_index(inplace = True)

df_tadetail = pd.merge(df_tadetail,df_taclass1,on =['trade_area_code','week_end_dt'])
df_tadetail = pd.merge(df_tadetail,df_taclass2,on =['trade_area_code','week_end_dt'])
df_tadetail = pd.merge(df_tadetail,df_taclass3,on =['trade_area_code'])
df_tadetail = pd.merge(dfweeklist[['week_end_dt','fiscal_week_nbr']],df_tadetail,
                      on ='week_end_dt')
df_tadetail.to_csv(outputpath + 'output4_classdetail.csv',index = False)


# In[ ]:

writer = pd.ExcelWriter(outputpath+'BigLots_Weekly_Data_'+recentweek+'.xlsx',
                            #engine='xlsxwriter',
                            datetime_format='yyyy-mm-dd',
                            date_format='yyyy-mm-dd')
test = dfsales_total[['location_id', 'week_end_dt', 'fiscal_week_nbr', 'gross_transaction_cnt']]
test = test.sort_values(['location_id', 'week_end_dt'])
test.to_excel(writer,'Transactions', index=False)
test = dfsales_total[['location_id', 'week_end_dt', 'fiscal_week_nbr', 'class_gross_sales_amt']]
test = test.sort_values(['location_id', 'week_end_dt'])
test.to_excel(writer,'Revenue', index=False)
dftraffic = dftraffic[~dftraffic['location_id'].isin(closed_onlinestorelist)]
dftraffic = dftraffic.sort_values(['location_id', 'week_end_dt'])
dfinventory_total = dfinventory_total[~dfinventory_total['location_id'].isin(closed_onlinestorelist)]
dfinventory_total = dfinventory_total.sort_values(['location_id', 'week_end_dt'])
dftraffic.to_excel(writer,'Traffic', index=False)
dfinventory_total.to_excel(writer,'Inventory', index=False)


# In[ ]:

writer.save()


# In[ ]:

df_complete_week = df_complete[df_complete['week_end_dt']==recentweek_date]
df_complete_week.to_csv(outputpath + 'output1_' + recentweek + '.csv',index = False)

df_highvariance_week = dfsales_total_recent_delete[dfsales_total_recent_delete['week_end_dt']==recentweek_date]
df_highvariance_week.to_csv(outputpath + 'highyoy_wowchangestores_' + recentweek + '.csv',index = False)

df_tadetail_week = df_tadetail[df_tadetail['week_end_dt']==recentweek_date]
df_tadetail_week.to_csv(outputpath + 'output4_classdetail_' + recentweek + '.csv',index = False)

df_dmadata_week = df_dmadata[df_dmadata['week_end_dt']==recentweek_date]
df_dmadata_week.to_csv(outputpath + 'output5_' + recentweek + '.csv',index = False)

df_tadata_week = df_tadata[df_tadata['week_end_dt']==recentweek_date]
df_tadata_week.to_csv(outputpath + 'output4_' + recentweek + '.csv',index = False)

df_converted_week = df_converted[df_converted['week_end_dt']==recentweek_date]
df_converted_week.to_csv(outputpath + 'output3_converted_' + recentweek + '.csv',index = False)

df_new_week = df_new[df_new['week_end_dt']==recentweek_date]
df_new_week.to_csv(outputpath + 'output2_new_' + recentweek + '.csv',index = False)


# In[ ]:

df_complete_week = df_complete_week[['location_id', 'week_end_dt', 'fiscal_week_nbr', 
                                     'location_desc', 'open_dt',
                   'address_line_1', 'address_line_2', 'city_nm', 'state_nm', 'zip_cd',
                   'longitude_meas', 'latitude_meas', 'DMA', 'trade_area_code',
                   'class_gross_sales_amt',
                   'class_gross_sales_amt_ly', 'class_gross_sales_amtYoYDiff',
                   'gross_transaction_cnt', 'gross_transaction_cnt_ly','gross_transaction_cntYoYDiff',
                   'Trans/Traffic', 'Trans/Traffic_ly', 'Trans/TrafficYoYDiff',
                   'traffic_week', 'traffic_week_ly', 'traffic_weekYoYDiff']]


# In[ ]:

df_complete_week1 = df_complete_week.sort_values(['gross_transaction_cntYoYDiff'],ascending = True)
df_complete_week1 = df_complete_week1[df_complete_week1['gross_transaction_cntYoYDiff']<=-0.1]

df_complete_week2 = df_complete_week.sort_values(['Trans/TrafficYoYDiff'],ascending = True)
df_complete_week2 = df_complete_week2[df_complete_week2['Trans/TrafficYoYDiff']<=-0.1]

df_complete_week3 = df_complete_week.sort_values(['traffic_weekYoYDiff'],ascending = True)
df_complete_week3 = df_complete_week3[df_complete_week3['traffic_weekYoYDiff']<=-0.1]


# In[ ]:

import xlsxwriter

writer = pd.ExcelWriter(outputpath+'Output1Tracker_'+recentweek+'.xlsx',
                            engine='xlsxwriter',
                            datetime_format='yyyy-mm-dd',
                            date_format='yyyy-mm-dd')

workbook  = writer.book

format1 = workbook.add_format({'num_format': '#,##0'})
format2 = workbook.add_format({'text_wrap' : True})
format3 = workbook.add_format({'num_format': '0.0%'})
format4 = workbook.add_format({'num_format': '0.0%',
                               'bg_color': 'FF9999'})

df_complete_week1.to_excel(writer,'TransactionTracker', index=False)
worksheet = writer.sheets['TransactionTracker']
worksheet.set_row(0,None, format2)
worksheet.set_column('B:B', 12, None)
worksheet.set_column('E:E', 12, None)
worksheet.set_column('F:F', 16, None)
worksheet.set_column('O:Y', None, format1)
worksheet.set_column('Q:Q', None, format3)
worksheet.set_column('T:T', None, format4)
worksheet.set_column('W:W', None, format3)
worksheet.set_column('Z:Z', None, format3)
worksheet.set_column('U:V', None, format3)

df_complete_week2.to_excel(writer,'ConversionTracker', index=False)
worksheet = writer.sheets['ConversionTracker']
worksheet.set_column('B:B', 12, None)
worksheet.set_column('E:E', 12, None)
worksheet.set_column('F:F', 16, None)
worksheet.set_column('O:Y', None, format1)
worksheet.set_column('Q:Q', None, format3)
worksheet.set_column('T:T', None, format3)
worksheet.set_column('W:W', None, format4)
worksheet.set_column('Z:Z', None, format3)
worksheet.set_column('U:V', None, format3)

df_complete_week3.to_excel(writer,'TrafficTracker', index=False)
worksheet = writer.sheets['TrafficTracker']
worksheet.set_column('B:B', 12, None)
worksheet.set_column('E:E', 12, None)
worksheet.set_column('F:F', 16, None)
worksheet.set_column('O:Y', None, format1)
worksheet.set_column('Q:Q', None, format3)
worksheet.set_column('T:T', None, format3)
worksheet.set_column('W:W', None, format3)
worksheet.set_column('Z:Z', None, format4)
worksheet.set_column('U:V', None, format3)

writer.save()


# In[ ]:

df_complete_week = df_tadata_week[['trade_area_code', 'week_end_dt', 'fiscal_week_nbr', 
                                     'Number_of_HMStores', 'Number_of_HMStores_RevTrafRank',
                   'Number of Stores', 'NumberofZipcodes', 'state_nm',
                   'class_gross_sales_amt',
                   'class_gross_sales_amt_ly', 'class_gross_sales_amtYoYDiff',
                   'gross_transaction_cnt', 'gross_transaction_cnt_ly','gross_transaction_cntYoYDiff',
                   'Trans/Traffic', 'Trans/Traffic_ly', 'Trans/TrafficYoYDiff',
                   'traffic_week', 'traffic_week_ly', 'traffic_weekYoYDiff']]

df_complete_week1 = df_complete_week.sort_values(['gross_transaction_cntYoYDiff'],ascending = True)
df_complete_week1 = df_complete_week1[df_complete_week1['gross_transaction_cntYoYDiff']<=-0.1]

df_complete_week2 = df_complete_week.sort_values(['Trans/TrafficYoYDiff'],ascending = True)
df_complete_week2 = df_complete_week2[df_complete_week2['Trans/TrafficYoYDiff']<=-0.1]

df_complete_week3 = df_complete_week.sort_values(['traffic_weekYoYDiff'],ascending = True)
df_complete_week3 = df_complete_week3[df_complete_week3['traffic_weekYoYDiff']<=-0.1]


# In[ ]:

import xlsxwriter

writer = pd.ExcelWriter(outputpath+'Output4Tracker_'+recentweek+'.xlsx',
                            engine='xlsxwriter',
                            datetime_format='yyyy-mm-dd',
                            date_format='yyyy-mm-dd')

workbook  = writer.book

format1 = workbook.add_format({'num_format': '#,##0'})
format2 = workbook.add_format({'text_wrap' : True})
format3 = workbook.add_format({'num_format': '0.0%'})
format4 = workbook.add_format({'num_format': '0.0%',
                               'bg_color': 'FF9999'})

df_complete_week1.to_excel(writer,'TransactionTracker', index=False)
worksheet = writer.sheets['TransactionTracker']
worksheet.set_column('B:B', 12, None)
worksheet.set_column('I:S', None, format1)
worksheet.set_column('K:K', None, format3)
worksheet.set_column('N:N', None, format4)
worksheet.set_column('Q:Q', None, format3)
worksheet.set_column('T:T', None, format3)
worksheet.set_column('O:P', None, format3)

df_complete_week2.to_excel(writer,'ConversionTracker', index=False)
worksheet = writer.sheets['ConversionTracker']
worksheet.set_column('B:B', 12, None)
worksheet.set_column('I:S', None, format1)
worksheet.set_column('K:K', None, format3)
worksheet.set_column('N:N', None, format3)
worksheet.set_column('Q:Q', None, format4)
worksheet.set_column('T:T', None, format3)
worksheet.set_column('O:P', None, format3)

df_complete_week3.to_excel(writer,'TrafficTracker', index=False)
worksheet = writer.sheets['TrafficTracker']
worksheet.set_column('B:B', 12, None)
worksheet.set_column('I:S', None, format1)
worksheet.set_column('K:K', None, format3)
worksheet.set_column('N:N', None, format3)
worksheet.set_column('Q:Q', None, format3)
worksheet.set_column('T:T', None, format4)
worksheet.set_column('O:P', None, format3)

writer.save()


# In[ ]:

df_complete_week = df_dmadata_week[['DMA', 'week_end_dt', 'fiscal_week_nbr', 
                                     'Number_of_HMStores',
                     'Number_of_HMStores_RevTrafRank', 'Number of Stores',
                     'NumberofZipcodes', 'NumberofTAs', 'state_nm',
                   'class_gross_sales_amt',
                   'class_gross_sales_amt_ly', 'class_gross_sales_amtYoYDiff',
                   'gross_transaction_cnt', 'gross_transaction_cnt_ly','gross_transaction_cntYoYDiff',
                   'Trans/Traffic', 'Trans/Traffic_ly', 'Trans/TrafficYoYDiff',
                   'traffic_week', 'traffic_week_ly', 'traffic_weekYoYDiff']]

df_complete_week1 = df_complete_week.sort_values(['gross_transaction_cntYoYDiff'],ascending = True)
df_complete_week1 = df_complete_week1[df_complete_week1['gross_transaction_cntYoYDiff']<=-0.1]

df_complete_week2 = df_complete_week.sort_values(['Trans/TrafficYoYDiff'],ascending = True)
df_complete_week2 = df_complete_week2[df_complete_week2['Trans/TrafficYoYDiff']<=-0.1]

df_complete_week3 = df_complete_week.sort_values(['traffic_weekYoYDiff'],ascending = True)
df_complete_week3 = df_complete_week3[df_complete_week3['traffic_weekYoYDiff']<=-0.1]


# In[ ]:

writer = pd.ExcelWriter(outputpath+'Output5Tracker_'+recentweek+'.xlsx',
                            engine='xlsxwriter',
                            datetime_format='yyyy-mm-dd',
                            date_format='yyyy-mm-dd')

workbook  = writer.book

format1 = workbook.add_format({'num_format': '#,##0'})
format2 = workbook.add_format({'text_wrap' : True})
format3 = workbook.add_format({'num_format': '0.0%'})
format4 = workbook.add_format({'num_format': '0.0%',
                               'bg_color': 'FF9999'})

df_complete_week1.to_excel(writer,'TransactionTracker', index=False)
worksheet = writer.sheets['TransactionTracker']
worksheet.set_column('B:B', 12, None)
worksheet.set_column('J:T', None, format1)
worksheet.set_column('L:L', None, format3)
worksheet.set_column('O:O', None, format4)
worksheet.set_column('R:R', None, format3)
worksheet.set_column('U:U', None, format3)
worksheet.set_column('P:Q', None, format3)

df_complete_week2.to_excel(writer,'ConversionTracker', index=False)
worksheet = writer.sheets['ConversionTracker']
worksheet.set_column('B:B', 12, None)
worksheet.set_column('J:T', None, format1)
worksheet.set_column('L:L', None, format3)
worksheet.set_column('O:O', None, format3)
worksheet.set_column('R:R', None, format4)
worksheet.set_column('U:U', None, format3)
worksheet.set_column('P:Q', None, format3)

df_complete_week3.to_excel(writer,'TrafficTracker', index=False)
worksheet = writer.sheets['TrafficTracker']
worksheet.set_column('B:B', 12, None)
worksheet.set_column('J:T', None, format1)
worksheet.set_column('L:L', None, format3)
worksheet.set_column('O:O', None, format3)
worksheet.set_column('R:R', None, format3)
worksheet.set_column('U:U', None, format4)
worksheet.set_column('P:Q', None, format3)

writer.save()


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# # Write all historical data into long and wide format by store

# In[ ]:

from datetime import datetime


# In[ ]:

sales_long_df=pd.read_csv("/home/jian/BiglotsCode/outputs/combined_sales_long_" +lastweeksdate+".csv")
del sales_long_df['week_indicator']
sales_long_df['week_end_date']=sales_long_df['week_end_date'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d'))
start_record_date=min(sales_long_df['week_end_date'])


# In[ ]:

new_sales_df = pd.read_csv(newsalespath,sep = '|',dtype = 'str')
new_sales_df['subclass_gross_sales_amt']=new_sales_df['subclass_gross_sales_amt'].astype(float)
new_sales_df=new_sales_df.groupby(['location_id','week_end_dt','fiscal_week_nbr','gross_sales_amt','gross_transaction_cnt','class_code_id'])['subclass_gross_sales_amt'].sum().to_frame().reset_index()
new_sales_df=new_sales_df.rename(columns={"subclass_gross_sales_amt":"class_gross_sales_amt"})

new_traffic_df = pd.read_csv(newtrafficpath,sep = '|',dtype = 'str')
new_sales_df=new_sales_df[~new_sales_df['location_id'].isin(["145","6990"])]
new_traffic_df=new_traffic_df[~new_traffic_df['location_id'].isin(["145","6990"])]

new_sales_df['location_id']=new_sales_df['location_id'].astype(int)
new_sales_df['week_end_dt']=new_sales_df['week_end_dt'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d'))
new_sales_df['class_gross_sales_amt']=new_sales_df['class_gross_sales_amt'].astype(float)
new_sales_df['gross_transaction_cnt']=new_sales_df['gross_transaction_cnt'].astype(int)

new_traffic_df['location_id']=new_traffic_df['location_id'].astype(int)
new_traffic_df['week_end_dt']=new_traffic_df['week_end_dt'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d'))
new_traffic_df['traffic_day_1']=new_traffic_df['traffic_day_1'].astype(int)
new_traffic_df['traffic_day_2']=new_traffic_df['traffic_day_2'].astype(int)
new_traffic_df['traffic_day_3']=new_traffic_df['traffic_day_3'].astype(int)
new_traffic_df['traffic_day_4']=new_traffic_df['traffic_day_4'].astype(int)
new_traffic_df['traffic_day_5']=new_traffic_df['traffic_day_5'].astype(int)
new_traffic_df['traffic_day_6']=new_traffic_df['traffic_day_6'].astype(int)
new_traffic_df['traffic_day_7']=new_traffic_df['traffic_day_7'].astype(int)
new_traffic_df['traffic']=new_traffic_df[['traffic_day_1','traffic_day_2','traffic_day_3','traffic_day_4',
                                         'traffic_day_5','traffic_day_6','traffic_day_7']].sum(axis=1)
new_traffic_df=new_traffic_df[['location_id','week_end_dt','traffic']]
new_traffic_df=new_traffic_df.drop_duplicates()
new_traffic_df.reset_index(inplace=True)
del new_traffic_df['index']


# In[ ]:

new_sales_df_app=new_sales_df[['location_id','week_end_dt','class_gross_sales_amt']].drop_duplicates()
new_sales_df_app=new_sales_df.groupby(['location_id','week_end_dt'])['class_gross_sales_amt'].sum().to_frame()
new_sales_df_app.reset_index(inplace=True)


# In[ ]:

new_transaction_df_app=new_sales_df[['location_id','week_end_dt','gross_transaction_cnt']]
new_transaction_df_app=new_transaction_df_app.drop_duplicates().reset_index()
del new_transaction_df_app['index']


# In[ ]:

recentweek = str(max(new_sales_df_app['week_end_dt']).date())


# In[ ]:

append_to_long=pd.merge(new_sales_df_app,new_transaction_df_app,on=['location_id','week_end_dt'],how='outer')
append_to_long=pd.merge(append_to_long,new_traffic_df,on=['location_id','week_end_dt'],how='outer')
append_to_long.columns=['location_id','week_end_date','sales','transactions','traffics']
append_to_long=append_to_long.fillna(0)
append_to_long['traffics']=append_to_long['traffics'].astype(int)
sales_long_df=sales_long_df.append(append_to_long)
sales_long_df['week_indicator']=sales_long_df['week_end_date'].apply(lambda x: int((x-start_record_date).days/7+1))
sales_long_df=sales_long_df.sort_values(['location_id','week_end_date'])
sales_long_df['week_indicator']=sales_long_df['week_indicator'].apply(lambda x: x% 52)
sales_long_df.to_csv("/home/jian/BiglotsCode/outputs/combined_sales_long_" +recentweek+".csv",index=False)


# In[ ]:

writer = pd.ExcelWriter("/home/jian/BiglotsCode/outputs/Output_"+recentweek+"/wide_sales_date"+recentweek+".xlsx", engine='xlsxwriter')
sales_long_df['week_end_date']=sales_long_df['week_end_date'].apply(lambda x: str(x.date()))
sales_wide_df_sales=sales_long_df[['location_id','week_end_date','sales']].pivot(index='location_id',columns='week_end_date',values='sales')
sales_wide_df_sales=sales_wide_df_sales.fillna(0)
sales_wide_df_sales.to_excel(writer, sheet_name='sales')
trans_wide_df_sales=sales_long_df[['location_id','week_end_date','transactions']].pivot(index='location_id',columns='week_end_date',values='transactions')
trans_wide_df_sales=trans_wide_df_sales.fillna(0)
trans_wide_df_sales.to_excel(writer, sheet_name='transactions')
traffics_wide_df_sales=sales_long_df[['location_id','week_end_date','traffics']].pivot(index='location_id',columns='week_end_date',values='traffics')
traffics_wide_df_sales=traffics_wide_df_sales.fillna(0)
traffics_wide_df_sales.to_excel(writer, sheet_name='traffics')

# summary=pd.DataFrame(columns=['location_id'])


# In[ ]:

T_sales_wide_df_sales=sales_wide_df_sales.T
T_sales_wide_df_sales['sales']=T_sales_wide_df_sales[T_sales_wide_df_sales.columns.tolist()].sum(axis=1)
T_sales_wide_df_sales=T_sales_wide_df_sales['sales'].to_frame().T

T_trans_wide_df_sales=trans_wide_df_sales.T
T_trans_wide_df_sales['trans']=T_trans_wide_df_sales[T_trans_wide_df_sales.columns.tolist()].sum(axis=1)
T_trans_wide_df_sales=T_trans_wide_df_sales['trans'].to_frame().T

T_traffics_wide_df_sales=traffics_wide_df_sales.T
T_traffics_wide_df_sales['traffics']=T_traffics_wide_df_sales[T_traffics_wide_df_sales.columns.tolist()].sum(axis=1)
T_traffics_wide_df_sales=T_traffics_wide_df_sales['traffics'].to_frame().T

count_wide_df_sales=sales_wide_df_sales.copy()

for col in count_wide_df_sales.columns.tolist():
    count_wide_df_sales[col]=np.where(count_wide_df_sales[col]>0,1,0)

T_count_wide_df_sales=count_wide_df_sales.T
T_count_wide_df_sales['counts']=T_count_wide_df_sales[T_count_wide_df_sales.columns.tolist()].sum(axis=1)
T_count_wide_df_sales=T_count_wide_df_sales['counts'].to_frame().T

T_avg_sales_wide_df_sales=T_sales_wide_df_sales.copy()
T_avg_sales_wide_df_sales.index=['avg_sales']
for col in T_avg_sales_wide_df_sales.columns.tolist():
    T_avg_sales_wide_df_sales[col]['avg_sales']=T_sales_wide_df_sales[col]['sales']/T_count_wide_df_sales[col]['counts']


summary=T_sales_wide_df_sales.append(T_trans_wide_df_sales).append(T_traffics_wide_df_sales).append(T_count_wide_df_sales).append(T_avg_sales_wide_df_sales)

summary.to_excel(writer,sheet_name="summary")


# In[ ]:

workbook  = writer.book
worksheet = writer.sheets['summary']


# In[ ]:

# Create a sales line chart
chartloc = len(summary.index) + 4

chart_sales = workbook.add_chart({'type': 'line'})
chart_sales.add_series({
        'name':       '2018',
        'categories': ['summary', 0, summary.shape[1]-11, 0, summary.shape[1]],
        'values':     ['summary', 1, summary.shape[1]-11, 1, summary.shape[1]]})

chart_sales.add_series({
        'name':       '2017',
        'categories': ['summary', 0, summary.shape[1]-11-52, 0, summary.shape[1]-52],
        'values':     ['summary', 1, summary.shape[1]-11-52, 1, summary.shape[1]-52]})

chart_sales.add_series({
        'name':       '2016',
        'categories': ['summary', 0, summary.shape[1]-11-52-52, 0, summary.shape[1]-52-52],
        'values':     ['summary', 1, summary.shape[1]-11-52-52, 1, summary.shape[1]-52-52]})

chart_sales.set_x_axis({'name': 'Date'})
chart_sales.set_y_axis({'name': 'Revenue', 'major_gridlines': {'visible': True}})

chart_sales.set_size({'width': 960, 'height': 432})
chart_sales.set_title({'name': 'Recent 12 Weeks Sales'})
chart_sales.set_legend({'position': 'bottom'})
worksheet.insert_chart('B'+str(chartloc), chart_sales) 


# In[ ]:

# Create a transaction line chart
chartloc = len(summary.index) + 4

chart_trans = workbook.add_chart({'type': 'line'})
chart_trans.add_series({
        'name':       '2018',
        'categories': ['summary', 0, summary.shape[1]-11, 0, summary.shape[1]],
        'values':     ['summary', 2, summary.shape[1]-11, 2, summary.shape[1]]})

chart_trans.add_series({
        'name':       '2017',
        'categories': ['summary', 0, summary.shape[1]-11-52, 0, summary.shape[1]-52],
        'values':     ['summary', 2, summary.shape[1]-11-52, 2, summary.shape[1]-52]})

chart_trans.add_series({
        'name':       '2016',
        'categories': ['summary', 0, summary.shape[1]-11-52-52, 0, summary.shape[1]-52-52],
        'values':     ['summary', 2, summary.shape[1]-11-52-52, 2, summary.shape[1]-52-52]})

chart_trans.set_x_axis({'name': 'Date'})
chart_trans.set_y_axis({'name': 'Transaction', 'major_gridlines': {'visible': True}})

chart_trans.set_size({'width': 960, 'height': 432})
chart_trans.set_title({'name': 'Recent 12 Weeks Transactions'})
chart_trans.set_legend({'position': 'bottom'})
worksheet.insert_chart('Q'+str(chartloc), chart_trans) 


# In[ ]:

# Create a traffics line chart
chartloc = len(summary.index) + 4

chart_traffics = workbook.add_chart({'type': 'line'})
chart_traffics.add_series({
        'name':       '2018',
        'categories': ['summary', 0, summary.shape[1]-11, 0, summary.shape[1]],
        'values':     ['summary', 3, summary.shape[1]-11, 3, summary.shape[1]]})

chart_traffics.add_series({
        'name':       '2017',
        'categories': ['summary', 0, summary.shape[1]-11-52, 0, summary.shape[1]-52],
        'values':     ['summary', 3, summary.shape[1]-11-52, 3, summary.shape[1]-52]})

chart_traffics.add_series({
        'name':       '2016',
        'categories': ['summary', 0, summary.shape[1]-11-52-52, 0, summary.shape[1]-52-52],
        'values':     ['summary', 3, summary.shape[1]-11-52-52, 3, summary.shape[1]-52-52]})

chart_traffics.set_x_axis({'name': 'Date'})
chart_traffics.set_y_axis({'name': 'Revenue', 
                       'major_gridlines': {'visible': True}})




chart_traffics.set_size({'width': 960, 'height': 432})
chart_traffics.set_title({'name': 'Recent 12 weeks traffics'})
chart_traffics.set_legend({'position': 'bottom'})
worksheet.insert_chart('AF'+str(chartloc), chart_traffics) 


# In[ ]:

# Create a average sales line chart
chartloc = len(summary.index) + 4+25

chart_avg_sales = workbook.add_chart({'type': 'line'})
chart_avg_sales.add_series({
        'name':       '2018',
        'categories': ['summary', 0, summary.shape[1]-11, 0, summary.shape[1]],
        'values':     ['summary', 5, summary.shape[1]-11, 5, summary.shape[1]]})

chart_avg_sales.add_series({
        'name':       '2017',
        'categories': ['summary', 0, summary.shape[1]-11-52, 0, summary.shape[1]-52],
        'values':     ['summary', 5, summary.shape[1]-11-52, 5, summary.shape[1]-52]})

chart_avg_sales.add_series({
        'name':       '2016',
        'categories': ['summary', 0, summary.shape[1]-11-52-52, 0, summary.shape[1]-52-52],
        'values':     ['summary', 5, summary.shape[1]-11-52-52, 5, summary.shape[1]-52-52]})

chart_avg_sales.set_x_axis({'name': 'Date'})
chart_avg_sales.set_y_axis({'name': 'Avg_Revenue', 
                       'major_gridlines': {'visible': True}})




chart_avg_sales.set_size({'width': 960, 'height': 432})
chart_avg_sales.set_title({'name': 'Recent 12 weeks sales per open store'})
chart_avg_sales.set_legend({'position': 'bottom'})
worksheet.insert_chart('B'+str(chartloc), chart_avg_sales) 


# In[ ]:

sales_long_df_positive_sales=sales_long_df[sales_long_df['sales']>0]
store_counts=sales_long_df_positive_sales.groupby(['week_end_date'])['location_id'].count().to_frame()
store_counts.to_excel(writer,"store_counts")


# In[ ]:

all_store_dma=pd.read_excel("/home/jian/BigLots/static_files/all_store_DMA_20180602.xlsx")
all_store_dma=all_store_dma[['location_id','DMA']]
store_counts_DMA=pd.merge(sales_long_df_positive_sales,all_store_dma,on='location_id',how='left')
store_counts_DMA=store_counts_DMA.groupby(['week_end_date','DMA'])['location_id'].count().to_frame()
store_counts_DMA.reset_index(inplace=True)
store_counts_DMA.to_excel(writer,"store_counts_DMA")


# In[ ]:

writer.save()


# In[ ]:




# In[ ]:




# In[ ]:



