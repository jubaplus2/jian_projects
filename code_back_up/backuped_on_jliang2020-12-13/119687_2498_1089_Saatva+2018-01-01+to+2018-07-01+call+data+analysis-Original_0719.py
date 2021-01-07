
# coding: utf-8

# In[1]:

import pandas as pd
import datetime
import numpy as np
import glob
import string
import re
import gc
import os
gc.collect()
import logging
today_str=str(datetime.datetime.now().date())
logging.basicConfig(filename='Saatva_Call_Matrix_'+today_str+'_files.log', level=logging.INFO)


# In[2]:

# Before
# 2018-01-15 (2nd week) to 2018-05-15 (both dates included)
# 85.7% phone orders found in the PressOne inbound calls
# Use phoen call start time to determin because it can be happened during a call (before the end of the call)


# 15273/32591 46.86% of the total buyer are found with a call in the PressOne dataset
# Jan 15 to Jul 1


# In[3]:


writer_folder="/home/jian/Projects/Saatva/Call_Analysis/output/"+today_str+"/"
try:
    os.stat(writer_folder)
except:
    os.mkdir(writer_folder)


# # Transaction Data

# In[4]:

klipfolio_data=pd.read_excel("/home/jian/Projects/Saatva/Call_Analysis/data/Transaction_with_phone_2018_to_May.xlsx",sheetname="Sales_By_Brand",dtype=str)
klipfolio_data=klipfolio_data.rename(columns={"Zip_Code":"zip_cd"})
klipfolio_data['Date']=klipfolio_data['Date'].apply(lambda x: datetime.datetime.strptime(x.split(" ")[0],"%Y-%m-%d").date())
klipfolio_data['Sales']=klipfolio_data['Sales'].astype(float)
klipfolio_data['Phone_1']=klipfolio_data['Phone_1'].apply(lambda x: x.replace("-",""))
klipfolio_data['Phone_2']=klipfolio_data['Phone_2'].apply(lambda x: x.replace("-",""))
klipfolio_data['Phone_1']=klipfolio_data['Phone_1'].apply(lambda x: re.sub('['+string.punctuation+']', '', x))
klipfolio_data['Phone_2']=klipfolio_data['Phone_2'].apply(lambda x: re.sub('['+string.punctuation+']', '', x))
klipfolio_data['Phone_1']=klipfolio_data['Phone_1'].apply(lambda x: x.replace(" ",""))
klipfolio_data['Phone_2']=klipfolio_data['Phone_2'].apply(lambda x: x.replace(" ",""))

del klipfolio_data['CSR']
del klipfolio_data['PressOne_Rep_Name']


# In[5]:

klipfolio_data_2_folder="/home/jian/Projects/Saatva/Transaction_Data/2018_weekly_klipfolio_with_Phone/"
klipfolio_data_2_file_list=glob.glob(klipfolio_data_2_folder+"*.csv")
for file in klipfolio_data_2_file_list:

    df=pd.read_csv(file,dtype=str)
    df=df[['Order ID','Date','Time','Shipping Zip','Product Revenue','Order Type',
                                   'Phone 1','Phone 2','PressOne Rep Name','Brand']]

    df=df.rename(columns={"Order ID":"Order_id","Shipping Zip":"zip_cd","Product Revenue":"Sales",
                                                     "Order Type":"Source","Phone 1":"Phone_1","Phone 2":"Phone_2",
                                                     "PressOne Rep Name":"PressOne_Rep_Name"})
    
    df=df.fillna("nan")
    df=df[df['Order_id']!="nan"]
    df['Date']=df['Date'].apply(lambda x: x.replace("-April-","-Apr-").replace("-March-","-Mar-").replace("-June-","Jun"))
    df['Date']=np.where(len(df['Date'])<df['Date'].apply(lambda x: len(x)).max(),"0"+df['Date'],df['Date'])
    df['Date']=df['Date'].apply(lambda x: datetime.datetime.strptime(x,"%d-%b-%Y").date())
    df['Sales']=df['Sales'].astype(float)
    df['Phone_1']=df['Phone_1'].apply(lambda x: x.replace("-",""))
    df['Phone_2']=df['Phone_2'].apply(lambda x: x.replace("-",""))
    df['Phone_1']=df['Phone_1'].apply(lambda x: re.sub('['+string.punctuation+']', '', x))
    df['Phone_2']=df['Phone_2'].apply(lambda x: re.sub('['+string.punctuation+']', '', x))
    df['Phone_1']=df['Phone_1'].apply(lambda x: x.replace(" ",""))
    df['Phone_2']=df['Phone_2'].apply(lambda x: x.replace(" ",""))
    

    klipfolio_data=klipfolio_data[klipfolio_data['Date']<=(df['Date'].min()+datetime.timedelta(days=1))]
    klipfolio_data=klipfolio_data.append(df)
    klipfolio_data=klipfolio_data.drop_duplicates()
    
transaction_data=klipfolio_data[klipfolio_data['Brand']=="Saatva"].reset_index()
del transaction_data['index']
del transaction_data['PressOne_Rep_Name']
transaction_data['Time']=transaction_data['Time'].apply(lambda x: x.zfill(8))
transaction_data=transaction_data.drop_duplicates()
transaction_data=transaction_data.reset_index()
del transaction_data['index']


# In[6]:

transaction_data_both=transaction_data.copy()
transaction_data_both['Date_Str']=transaction_data_both['Date'].astype(str)
transaction_data_both=transaction_data_both.drop_duplicates()
transaction_data_both['Date_Time_Str']=transaction_data_both['Date_Str']+" "+transaction_data_both['Time']
transaction_data_both['Date_Time']=transaction_data_both['Date_Time_Str'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d %H:%M:%S"))
del transaction_data_both['Date_Str']
transaction_data_both=transaction_data_both[(transaction_data_both['Date_Time']<datetime.datetime(2018,7,2)) & (transaction_data_both['Date_Time']>=datetime.datetime(2018,1,1))]
data_both_1=transaction_data_both[['Order_id','Date_Time','Phone_1']]
data_both_1=data_both_1.rename(columns={"Phone_1":"Phone"})

data_both_2=transaction_data_both[['Order_id','Date_Time','Phone_2']]
data_both_2=data_both_2.rename(columns={"Phone_2":"Phone"})

transaction_both_data=data_both_1.append(data_both_2)
transaction_both_data=transaction_both_data.sort_values('Date_Time')


# In[7]:

transaction_both_data['defined_buyer_id']=np.nan
id_number=1
df_buyer=pd.DataFrame(columns={'Order_id','Date_Time','Phone'})
for order_id,group in transaction_both_data.groupby(['Order_id']):
    df=group[group['Phone']!='nan']
    common_list=list(set(df['Phone'].tolist()).intersection(df_buyer['Phone'].tolist()))
    if len(common_list)>0:
        buyer_id=df_buyer[df_buyer['Phone'].isin(df['Phone'])]['defined_buyer_id'].unique().tolist()
        if len(buyer_id)>1:
            print("Multi_Buyer_id_Error:",order_id)
            df_buyer_re_allocate=df_buyer[df_buyer['defined_buyer_id'].isin(buyer_id)]
            df_buyer=df_buyer[~df_buyer['defined_buyer_id'].isin(buyer_id)]
            df=df_buyer_re_allocate.append(group)
            df=df[df['Phone']!='nan']
            df['defined_buyer_id']=buyer_id[0]
            
        else:    
            df['defined_buyer_id']=buyer_id[0]
    
    else:
        df['defined_buyer_id']="buyer_"+str(id_number)
        id_number=id_number+1
        
    
    df_buyer=df_buyer.append(df)
    
    if id_number% 3000==30:
        print(id_number,datetime.datetime.now())
        


# In[8]:

# Check
test=df_buyer[(df_buyer['Phone']=="2109122041")| (df_buyer['Phone']=="2103945454")]
test


# In[ ]:




# In[9]:

df_buyer=df_buyer.sort_values(['Date_Time','defined_buyer_id'],ascending=[True,True])
df_buyer=df_buyer.reset_index()
del df_buyer['index']

df_order_buyer=df_buyer[['Date_Time','Order_id','defined_buyer_id']].drop_duplicates().reset_index()
del df_order_buyer['index']


# In[10]:

df_order_buyer_seq=pd.DataFrame()
for defined_buyer_id,group in df_order_buyer.groupby(['defined_buyer_id']):
    group['Buy_Seq']=[x for x in range(1,(len(group)+1))]
    df_order_buyer_seq=df_order_buyer_seq.append(group)
print(datetime.datetime.now())


# In[33]:

transaction_data=pd.merge(transaction_data_both,df_order_buyer_seq,on=['Order_id','Date_Time'],how="outer")
transaction_data=transaction_data.sort_values(['Date_Time','Order_id'])
transaction_data=transaction_data.drop_duplicates(['Date_Time','Order_id'])
transaction_data=transaction_data.reset_index()
del transaction_data['index']


# In[12]:

# For multiple purchases, the Phone_2 issue not cleaned yet!!! (2 records for the same order, 1 with 2 phones and 1 with 1 phone)
transaction_data_1st_Only=transaction_data[transaction_data['Buy_Seq']==1]
transaction_data_1st_Only=transaction_data_1st_Only.sort_values(['Date_Time','Order_id','defined_buyer_id','Phone_1','Phone_2'])
transaction_data_1st_Only=transaction_data_1st_Only.drop_duplicates(['Date_Time','Order_id','defined_buyer_id','Phone_1'])
transaction_data_phone_1st_Only=transaction_data_1st_Only[transaction_data_1st_Only['Source']=="phone"]
transaction_data_online_1st_Only=transaction_data_1st_Only[transaction_data_1st_Only['Source']=="online"]


# In[13]:

transaction_data_1st_Only.shape[0]-len(transaction_data_phone_1st_Only['defined_buyer_id'].unique())-len(transaction_data_online_1st_Only['defined_buyer_id'].unique())



# In[14]:

data_both_1=transaction_data[['Phone_1','defined_buyer_id']]
data_both_1=data_both_1.rename(columns={"Phone_1":"Phone"})

data_both_2=transaction_data[['Phone_2','defined_buyer_id']]
data_both_2=data_both_2.rename(columns={"Phone_2":"Phone"})

match_buyer_phone=data_both_1.append(data_both_2).drop_duplicates()
match_buyer_phone=match_buyer_phone[match_buyer_phone['Phone']!='nan']


# In[15]:

match_buyer_phone_list=match_buyer_phone.groupby(['defined_buyer_id'])['Phone'].apply(list).to_frame().reset_index()
match_buyer_phone_list.tail(2)


# In[ ]:




# In[ ]:




# In[ ]:




# In[16]:

transacted_match_data_writer=pd.ExcelWriter(writer_folder+"transaction_match_data"+today_str+".xlsx",engine="xlsxwriter")
transaction_data.to_excel(transacted_match_data_writer,"transactation_data",index=False)
transaction_data_1st_Only.to_excel(transacted_match_data_writer,"trans_data_1st_purchase",index=False)
match_buyer_phone_list.to_excel(transacted_match_data_writer,"buyer_phone",index=False)
transacted_match_data_writer.save()



# In[17]:

'''
Multiple_Buyers=transaction_data[transaction_data['defined_buyer_id'].isin(transaction_data[transaction_data['Buy_Seq']>1]['defined_buyer_id'])]
len(Multiple_Buyers['defined_buyer_id'].unique()) #660
'''



# In total 4 unique orders (also unique buyers) contain the free code, and 2 of the 4 have another phone (4-2 only with the 866 and 888)
# 8665963368,9072541011
# 4157389811,8772727241
# 8662266454
# 8886390639
'''
copy=transaction_data.copy()
copy['AC_1']=copy['Phone_1'].apply(lambda x: str(x)[0:3])
copy['AC_2']=copy['Phone_2'].apply(lambda x: str(x)[0:3])

copy[(copy['AC_1'].isin(['800','888','877','866','855','844'])) | (copy['AC_2'].isin(['800','888','877','866','855','844']))]
'''
'''
digit_11=match_buyer_phone[match_buyer_phone['Phone'].apply(lambda x: len(x))==11]
digit_11['1st_digit']=digit_11['Phone'].apply(lambda x: x[0])
digit_11['last_10']=digit_11['Phone'].apply(lambda x: x[1:])
digit_11.groupby(['1st_digit'])['defined_buyer_id'].count().to_frame().reset_index()
# 24 out of the 94 are found if remove the leading 1 for the 11-digits phones
# No need to clean the leading 1, because of low counts, low pctg fount, and unsure of the country code
'''
'''
transaction_data_online=transaction_data[transaction_data['Source']=="online"]
transaction_data_online['Date_Str']=transaction_data_online['Date'].astype(str)
transaction_data_online['Date_Time']=transaction_data_online['Date_Str']+" "+transaction_data_online['Time']
transaction_data_online['Time']=transaction_data_online['Date_Time'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d %H:%M:%S"))
del transaction_data_online['Date_Str']
transaction_data_online=transaction_data_online[(transaction_data_online['Time']<=datetime.datetime(2018,6,10)) & (transaction_data_phone['Time']>=datetime.datetime(2018,1,15))]
data_online_1=transaction_data_online[['Order_id','Date_Time','Phone_1']]
data_online_2=transaction_data_online[['Order_id','Date_Time','Phone_2']]
'''
'''
transaction_data_phone=transaction_data[transaction_data['Source']=="phone"]
transaction_data_phone['Date_Str']=transaction_data_phone['Date'].astype(str)
transaction_data_phone['Date_Time']=transaction_data_phone['Date_Str']+" "+transaction_data_phone['Time']
transaction_data_phone['Time']=transaction_data_phone['Date_Time'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d %H:%M:%S"))
del transaction_data_phone['Date_Str']
transaction_data_phone=transaction_data_phone[(transaction_data_phone['Time']<=datetime.datetime(2018,6,10)) & (transaction_data_phone['Time']>=datetime.datetime(2018,1,15))]
data_phone_1=transaction_data_phone[['Order_id','Date_Time','Phone_1']]
data_phone_1=data_phone_1.rename(columns={"Phone_1":"Phone"})
data_phone_2=transaction_data_phone[['Order_id','Date_Time','Phone_2']]
data_phone_2=data_phone_2.rename(columns={"Phone_2":"Phone"})

transaction_data_phone=data_phone_1.append(data_phone_2)
'''


# # Buyer_DMA

# In[18]:

buyer_zip=transaction_data[['defined_buyer_id','zip_cd']].drop_duplicates()
zip_DMA=pd.read_excel("/home/jian/Docs/Geo_mapping/Zips by DMA by County16-17 nielsen.xlsx",dtype=str,skiprows=1)
zip_DMA=zip_DMA.iloc[:,[0,2]]
zip_DMA.columns=['zip_cd',"DMA"]
zip_DMA=zip_DMA.drop_duplicates(['zip_cd'])
buyer_zip_DMA=pd.merge(buyer_zip,zip_DMA,on='zip_cd',how="left")
buyer_zip_DMA=buyer_zip_DMA.fillna("non_US")
# 4 zips in 5 digits are not in DMA 80847,02098,49165,22512


# In[ ]:




# # Phone Data

# In[19]:

Press_One_folder="/home/jian/Projects/Saatva/Call_Analysis/data/2018_Call_Data_with_Dup/"
call_file_list=glob.glob(Press_One_folder+"*.csv")
call_data=pd.DataFrame()
for file in call_file_list:
    df=pd.read_csv(file,dtype=str)
    call_data=call_data.append(df)
call_data=call_data.drop_duplicates()

call_data.reset_index(inplace=True)
del call_data['index']
call_data=call_data[['S','Started (Dist) (Display)','Talk Time (Display)','Talk Time (Seconds Value)','Ring (Dist) (Seconds Value)','Dev (Clg)','TelNo (Clg)','Ans','Type']]
call_data.columns=['Status','Start_Time_Str','Talk_Time_Str','Talk_Time_Sec','Ring_Time_Sec','Dev_Clg','TelNo_Clg','Ans','Type']
call_data=call_data[~pd.isnull(call_data['Status'])]
call_data=call_data.reset_index()
del call_data['index']
gc.collect()


# In[20]:

call_data=call_data[call_data['Type']=="Ext/In"] # inbound calls only

call_data['Call_in_Phone']=np.where(call_data['Dev_Clg']=="[Trunk]",
                                   call_data['TelNo_Clg'],call_data['Dev_Clg'])
call_data=call_data[(call_data['Call_in_Phone']!="[No CallerID]") & (call_data['Call_in_Phone']!="[Conference]") & (call_data['Call_in_Phone']!="Anonymous") & (call_data['Call_in_Phone']!="0000000000")]
call_data['Call_in_Phone']=call_data['Call_in_Phone'].apply(lambda x:x.split("x")[0])

call_data['Start_Time']=call_data['Start_Time_Str'].apply(lambda x: datetime.datetime.strptime(x,'%m/%d/%Y %H:%M:%S'))
call_data['Talk_Time_Delta']=call_data['Talk_Time_Sec'].astype(int).apply(lambda x:datetime.timedelta(seconds=x))
call_data['Ring_Time_Delta']=call_data['Ring_Time_Sec'].astype(int).apply(lambda x:datetime.timedelta(seconds=x))

call_data['End_Time']=call_data['Start_Time']+call_data['Talk_Time_Delta']+call_data['Ring_Time_Delta']
call_data['Area_Code']=call_data['Call_in_Phone'].apply(lambda x: x[0:3])
call_data=call_data.sort_values(['Call_in_Phone','Start_Time','End_Time'])


free_numbers_area=['800','888','877','866','855','844']
call_data=call_data[~call_data['Area_Code'].isin(free_numbers_area)]
call_data=call_data.reset_index()
call_data['Phone']=call_data['Call_in_Phone']

call_data=call_data[(call_data['Start_Time']<datetime.datetime(2018,7,2)) & (call_data['Start_Time']>=datetime.datetime(2018,1,1))]
call_data=call_data.sort_values(['Call_in_Phone','Start_Time'])
call_data=call_data.reset_index()
del call_data['index']


# In[21]:

call_data=pd.merge(call_data,match_buyer_phone,on="Phone",how="left")
call_data=call_data.sort_values(['Call_in_Phone','Start_Time'],ascending=[True,True])

call_data_transacted=call_data[~pd.isnull(call_data['defined_buyer_id'])]
call_data_transacted=call_data_transacted.sort_values(['Call_in_Phone','Start_Time'],ascending=[True,True])

call_data_no_buy=call_data[pd.isnull(call_data['defined_buyer_id'])]
call_data_no_buy=call_data_no_buy.sort_values(['Call_in_Phone','Start_Time'],ascending=[True,True])


# In[22]:

call_data.shape


# In[23]:

call_data_transacted.shape


# In[24]:

call_data_no_buy.shape


# In[25]:

len(call_data_transacted['defined_buyer_id'].unique())/len(transaction_data['defined_buyer_id'].unique()) 
# Percentage of the matched calls for both online and phone order: 


# In[ ]:




# In[ ]:




# In[26]:

len(call_data_transacted['defined_buyer_id'].unique())/len(transaction_data_phone_1st_Only['defined_buyer_id'].unique()) 
# Percentage of the matched calls for 1st phone order only: 


# In[27]:

# len(set(digit_11[digit_11['1st_digit']=="1"]['last_10'].unique().tolist()).intersection(call_data['Phone'].unique().tolist()))
gc.collect()


# # Non-Buyers

# In[26]:

# Reason to use 30 seconds: e.g. June, Phone "2025094332"
# Part 1: non-transacted phone calls
gc.collect()
call_data_group_only_no_buy=call_data_no_buy[['Call_in_Phone','Start_Time','End_Time','Talk_Time_Delta','Ring_Time_Delta']]
non_transacted_phone_calls=pd.DataFrame()

k=1
for phone,group in call_data_group_only_no_buy.groupby(['Call_in_Phone']):

    df_drop_transfer=group.reset_index()
    del df_drop_transfer['index']
    df_drop_transfer=df_drop_transfer.sort_values(['Start_Time']) # Order!
    df_drop_transfer=df_drop_transfer.reset_index()
    del df_drop_transfer['index']
    df_drop_transfer['call_seq']=np.nan
    df_drop_transfer['call_seq'][0]=1
    seq=1

    for i in range(1,len(df_drop_transfer)):
        if (df_drop_transfer['Start_Time'][i]-df_drop_transfer['End_Time'][i-1]).seconds<=30:
            df_drop_transfer['call_seq'][i]=seq
        else:
            seq=seq+1
            df_drop_transfer['call_seq'][i]=seq

    non_transacted_phone_calls=non_transacted_phone_calls.append(df_drop_transfer)
    k=k+1
    if k%2000==100: # <10 minutes of running per 10,000 loops
        print(k,datetime.datetime.now())
        gc.collect()


# In[27]:

gc.collect()
i=0
unique_non_transacted_phone_calls=pd.DataFrame()
for (phone,seq),group in non_transacted_phone_calls.groupby(['Call_in_Phone','call_seq']):
    df_unique_phone=group.reset_index()
    del df_unique_phone['index']
    
    len_df=len(df_unique_phone)
    start_time=df_unique_phone['Start_Time'][0]
    end_time=df_unique_phone['End_Time'][len_df-1]
    duration_sec=(end_time-start_time).seconds
    talk_sec=df_unique_phone['Talk_Time_Delta'].sum()
    ring_sec=df_unique_phone['Ring_Time_Delta'].sum()
    
    df=pd.DataFrame({"Phone_Num":phone,"Call_Seq":seq,"Start_time":start_time,"End_time":end_time,"Call_duration_Sec":duration_sec,
                    "Talk_length_Sec":talk_sec,"Ring_length_Sec":ring_sec},index=[i])
    unique_non_transacted_phone_calls=unique_non_transacted_phone_calls.append(df)
    i=i+1
    if i%10000==100:
        print(i,datetime.datetime.now())


# In[28]:

unique_non_transacted_phone_calls_Exclusion=unique_non_transacted_phone_calls[unique_non_transacted_phone_calls['Phone_Num'].isin(unique_non_transacted_phone_calls[unique_non_transacted_phone_calls['Call_Seq']>10]['Phone_Num'])]
unique_non_transacted_phone_calls=unique_non_transacted_phone_calls[~unique_non_transacted_phone_calls['Phone_Num'].isin(unique_non_transacted_phone_calls_Exclusion['Phone_Num'])]




# # Buyers

# In[29]:

transaction_time_1st_Only=transaction_data_1st_Only[['defined_buyer_id','Date_Time','Source']]
transaction_time_1st_Only=transaction_time_1st_Only.rename(columns={"Date_Time":"Trans_1st_Time"})
transaction_time_1st_Only=transaction_time_1st_Only.reset_index()
del transaction_time_1st_Only['index']
transaction_time_1st_Only['Trans_1st_Time'][0]
transaction_time_1st_Only['Trans_1st_Time_plust_1_hour']=transaction_time_1st_Only['Trans_1st_Time'].apply(lambda x: x+datetime.timedelta(hours=1))


# In[30]:

# Part 2: Transacted phone calls
gc.collect()
call_data_group_buyers=call_data_transacted[['defined_buyer_id','Start_Time','End_Time','Talk_Time_Delta','Ring_Time_Delta']]
Transacted_phone_calls=pd.DataFrame()


# In[32]:

k=1
for buyer_id,group in call_data_group_buyers.groupby(['defined_buyer_id']):

    df_drop_transfer=group.reset_index()
    del df_drop_transfer['index']
    df_drop_transfer=df_drop_transfer.sort_values(['Start_Time']) #Order
    df_drop_transfer=df_drop_transfer.reset_index()
    del df_drop_transfer['index']
    df_drop_transfer['call_seq']=np.nan
    df_drop_transfer['call_seq'][0]=1
    seq=1

    if len(df_drop_transfer)>1:
        for i in range(1,len(df_drop_transfer)):
            if (df_drop_transfer['Start_Time'][i]-df_drop_transfer['End_Time'][i-1]).seconds<=30:
                df_drop_transfer['call_seq'][i]=seq
            else:
                seq=seq+1
                df_drop_transfer['call_seq'][i]=seq

    Transacted_phone_calls=Transacted_phone_calls.append(df_drop_transfer)
    k=k+1
    if k%1500==100:
        print(k,datetime.datetime.now())


# In[33]:

gc.collect()
i=0
unique_Transacted_phone_calls=pd.DataFrame()
for (buyer_id,seq),group in Transacted_phone_calls.groupby(['defined_buyer_id','call_seq']):
    df_unique_phone=group.reset_index()
    del df_unique_phone['index']
    len_df=len(df_unique_phone)
    start_time=df_unique_phone['Start_Time'][0]
    end_time=df_unique_phone['End_Time'][len_df-1]
    duration_sec=(end_time-start_time).seconds
    talk_sec=df_unique_phone['Talk_Time_Delta'].sum()
    ring_sec=df_unique_phone['Ring_Time_Delta'].sum()
    
    df=pd.DataFrame({"defined_buyer_id":buyer_id,"Call_Seq":seq,"Start_time":start_time,"End_time":end_time,"Call_duration_Sec":duration_sec,
                    "Talk_length_Sec":talk_sec,"Ring_length_Sec":ring_sec},index=[i])
    unique_Transacted_phone_calls=unique_Transacted_phone_calls.append(df)
    i=i+1
    if i%3700==100:
        print(i,datetime.datetime.now())


# In[35]:

logging.info("Unique buyers count who have ever called with a transaction (both before and after):")
logging.info(len(unique_Transacted_phone_calls['defined_buyer_id'].unique()))


# In[36]:

unique_Transacted_phone_calls_Before=pd.merge(unique_Transacted_phone_calls,transaction_time_1st_Only,on="defined_buyer_id",how="left")

# Online_Order: call Start time only before Transaction time
# Phone_Order: call Start time only before Transaction time + 1 Hour



unique_Transacted_calls_Before_phone=unique_Transacted_phone_calls_Before[unique_Transacted_phone_calls_Before['Source']=="phone"]
unique_Transacted_calls_Before_phone=unique_Transacted_calls_Before_phone[unique_Transacted_calls_Before_phone['Start_time']<=unique_Transacted_calls_Before_phone['Trans_1st_Time_plust_1_hour']]

unique_Transacted_calls_Before_online=unique_Transacted_phone_calls_Before[unique_Transacted_phone_calls_Before['Source']=="online"]
unique_Transacted_calls_Before_online=unique_Transacted_calls_Before_online[unique_Transacted_calls_Before_online['Start_time']<=unique_Transacted_calls_Before_online['Trans_1st_Time']]

unique_Transacted_phone_calls_matched=unique_Transacted_calls_Before_phone.append(unique_Transacted_calls_Before_online)
unique_Transacted_phone_calls_matched=unique_Transacted_phone_calls_matched.sort_values(['Start_time'])


# In[66]:

logging.info("len of unique_Transacted_phone_calls_matched:")
logging.info(len(unique_Transacted_phone_calls_matched))


# In[37]:

unique_Transacted_phone_calls_matched['Source'].unique()


# In[39]:

unique_Transacted_phone_calls_matched['Start_time'].min()


# In[40]:

logging.info("Unique buyers count who called before a transaction: unique_Transacted_phone_calls_matched['defined_buyer_id'].unique()")
logging.info(len(unique_Transacted_phone_calls_matched['defined_buyer_id'].unique()))


# In[64]:

len(unique_Transacted_phone_calls_matched[unique_Transacted_phone_calls_matched['Source']=='online']['defined_buyer_id'].unique())


# In[42]:

writer=pd.ExcelWriter(writer_folder+"transacted_and_non_transacted_call_data"+today_str+".xlsx",engine="xlsxwriter")
call_data.to_excel(writer,"call_data",index=False)
non_transacted_phone_calls.to_excel(writer,'non_transcted_call_all',index=False)
unique_non_transacted_phone_calls.to_excel(writer,'non_transcted_unique',index=False)
Transacted_phone_calls.to_excel(writer,'Transcted_call_all',index=False)
unique_Transacted_phone_calls.to_excel(writer,'Transcted_unique',index=False)
unique_Transacted_phone_calls_matched.to_excel(writer,'unique_Trans_Call_Before_Only',index=False) #Including 10+
writer.save()


# In[43]:

Transacted_Call_Freq_by_buyers=unique_Transacted_phone_calls_matched.groupby(['defined_buyer_id'])['Start_time'].count().to_frame().reset_index()
Transacted_Call_Freq_by_buyers=Transacted_Call_Freq_by_buyers.rename(columns={"Start_time":"Call_Times"})

Transacted_Call_Freq_by_buyers_1_to_4=Transacted_Call_Freq_by_buyers[Transacted_Call_Freq_by_buyers['Call_Times']<=4]
Transacted_Call_Freq_by_buyers_5_plus=Transacted_Call_Freq_by_buyers[(Transacted_Call_Freq_by_buyers['Call_Times']>4) & (Transacted_Call_Freq_by_buyers['Call_Times']<=10)]
# Transacted_Call_Freq_by_buyers_5_plus changed to 5 to 10

real_Transacted_Call_Freq_by_buyers_5_plus=Transacted_Call_Freq_by_buyers[Transacted_Call_Freq_by_buyers['Call_Times']>4]

unique_Transacted_phone_calls_matched=unique_Transacted_phone_calls_matched[unique_Transacted_phone_calls_matched['defined_buyer_id'].isin(Transacted_Call_Freq_by_buyers[Transacted_Call_Freq_by_buyers['Call_Times']<=10]['defined_buyer_id'])]


# In[68]:

buyers_called=call_data_transacted['defined_buyer_id'].unique().tolist()
match_buyer_phone_list_called=match_buyer_phone_list[match_buyer_phone_list['defined_buyer_id'].isin(buyers_called)]
match_buyer_phone_list_called['len_phone']=match_buyer_phone_list_called['Phone'].apply(lambda x:len(x))
Ratio_Phone_to_Buyer_Called=match_buyer_phone_list_called['len_phone'].mean()
Ratio_Phone_to_Buyer_Called


# In[69]:

buyers_called=unique_Transacted_phone_calls_matched['defined_buyer_id'].unique().tolist()
match_buyer_phone_list_called=match_buyer_phone_list[match_buyer_phone_list['defined_buyer_id'].isin(buyers_called)]
match_buyer_phone_list_called['len_phone']=match_buyer_phone_list_called['Phone'].apply(lambda x:len(x))
Ratio_Phone_to_Buyer_Called=match_buyer_phone_list_called['len_phone'].mean()
Ratio_Phone_to_Buyer_Called


# In[67]:

call_buyers_with_phone=call_data[~pd.isnull(call_data['defined_buyer_id'])]
Ratio_Phone_to_Buyer_Called=len(call_buyers_with_phone['Phone'].unique())/len(call_buyers_with_phone['defined_buyer_id'].unique())
Ratio_Phone_to_Buyer_Called


# In[71]:

call_buyers_with_phone=call_data[~pd.isnull(call_data['defined_buyer_id'])]
buyers_called=unique_Transacted_phone_calls_matched['defined_buyer_id'].unique().tolist()
call_buyers_with_phone=call_buyers_with_phone[call_buyers_with_phone['defined_buyer_id'].isin(buyers_called)]
Ratio_Phone_to_Buyer_Called=len(call_buyers_with_phone['Phone'].unique())/len(call_buyers_with_phone['defined_buyer_id'].unique())
Ratio_Phone_to_Buyer_Called


# In[ ]:

# Ratio of 1.2009839146536951 for buyer credit card phones / buyers !! Selected
# Ratio of 1.0399093471892 for buyers who actually give a call
# 18091 unique buyers give a call
# unique buyers only called before they ordered (Online: Call start time < trans time; Call: Call end time < trans time +1 H)

# Transaction data: 38293 unique buyers (online 28340 + phone 9953 as 1st time purchage source)


# In[ ]:

# Apply the ratio later in excel


# # Df_1 Call Summary

# In[72]:

def agg_by_freq_seg(df,df_5_plus):
    df_1=df.groupby(['Call_Times'])['defined_buyer_id'].apply(list).to_frame().reset_index()
    df_1=df_1.rename(columns={"defined_buyer_id":"buyer_list"})

    df_2=df.groupby(['Call_Times'])['defined_buyer_id'].count().to_frame().reset_index()
    df_2=df_2.rename(columns={"defined_buyer_id":"buyer_count"})

    df_result=pd.merge(df_1,df_2,on="Call_Times",how="outer")
    
    df_5_plus_app=pd.DataFrame({"Call_Times":"5-10","buyer_list":[df_5_plus['defined_buyer_id'].unique().tolist()],"buyer_count":len(df_5_plus['defined_buyer_id'].unique().tolist())},index=[4])
    df_result=df_result.append(df_5_plus_app)
    
    return df_result

base_df_transacted=agg_by_freq_seg(Transacted_Call_Freq_by_buyers_1_to_4,Transacted_Call_Freq_by_buyers_5_plus)


# In[73]:


# 1. Transacted
Transaced_df_summary=base_df_transacted.copy()
Transaced_df_summary['Total_Buyer']=np.nan
Transaced_df_summary['Phone_Buyer']=np.nan
Transaced_df_summary['Online_Buyer']=np.nan

Transaced_df_summary['Total_Trans']=np.nan
Transaced_df_summary['Phone_Trans']=np.nan
Transaced_df_summary['Online_Trans']=np.nan

Transaced_df_summary['Total_Transacted_Calls']=np.nan

# Use 1st Time Transaction only : transaction_data_1st_Only
# Because use the 1st time transaction, the total equals to the sum of both sources
for i in range(5):
    buyer_list=Transaced_df_summary['buyer_list'][i]
    df_trans=transaction_data[transaction_data['defined_buyer_id'].isin(buyer_list)]
    type_of_order_order=df_trans.groupby(['Source'])['Order_id'].count().to_frame().reset_index()
    
    phone_trans_order=type_of_order_order[type_of_order_order['Source']=="phone"]['Order_id'].unique()[0]
    online_trans_order=type_of_order_order[type_of_order_order['Source']=="online"]['Order_id'].unique()[0]
    both_trans_order=len(df_trans['Order_id'].unique())
    
    df=transaction_data_1st_Only[transaction_data_1st_Only['defined_buyer_id'].isin(buyer_list)]
    type_of_order_buyer=df.groupby(['Source'])['defined_buyer_id'].apply(lambda x: len(x.unique())).to_frame().reset_index()
    phone_trans_buyer=type_of_order_buyer[type_of_order_buyer['Source']=="phone"]['defined_buyer_id'].unique()[0]
    online_trans_buyer=type_of_order_buyer[type_of_order_buyer['Source']=="online"]['defined_buyer_id'].unique()[0]
    both_trans_buyer=len(df['Order_id'].unique())

    
    Transaced_df_summary['Phone_Trans'][i]=phone_trans_order
    Transaced_df_summary['Online_Trans'][i]=online_trans_order
    Transaced_df_summary['Total_Trans'][i]=both_trans_order
    
    Transaced_df_summary['Phone_Buyer'][i]=phone_trans_buyer
    Transaced_df_summary['Online_Buyer'][i]=online_trans_buyer
    Transaced_df_summary['Total_Buyer'][i]=both_trans_buyer
    
    df_total_call=unique_Transacted_phone_calls_matched[unique_Transacted_phone_calls_matched['defined_buyer_id'].isin(buyer_list)]
    total_calls=len(df_total_call)
    Transaced_df_summary['Total_Transacted_Calls'][i]=total_calls
    
Transaced_df_summary=Transaced_df_summary[['Call_Times','buyer_count','buyer_list','Total_Transacted_Calls','Phone_Trans','Online_Trans','Total_Trans',
                                          'Phone_Buyer','Online_Buyer','Total_Buyer']]


# In[74]:

#2. Non Buyers
unique_non_transacted_phone_calls.head(2)
Non__Transacted_df_summary_count=unique_non_transacted_phone_calls.groupby(['Phone_Num'])['Start_time'].count().to_frame().reset_index()
# exclusions_non_transactions=Non__Transacted_df_summary_count[Non__Transacted_df_summary_count['Start_time']>=100]
# Non__Transacted_df_summary_count=Non__Transacted_df_summary_count[Non__Transacted_df_summary_count['Start_time']<100]


Non__Transacted_df_summary_list=Non__Transacted_df_summary_count.groupby(['Start_time'])['Phone_Num'].apply(list).to_frame().reset_index()
Non__Transacted_df_summary_list=Non__Transacted_df_summary_list.rename(columns={"Start_time":"Call_Times","Phone_Num":"Non_Transacted_Phones"})

Non__Transacted_df_summary_list_1_4=Non__Transacted_df_summary_list[Non__Transacted_df_summary_list['Call_Times']<=4]

Non__Transacted_df_summary_list_5_Plus_list_Phone=Non__Transacted_df_summary_count[Non__Transacted_df_summary_count['Start_time']>=5]['Phone_Num'].unique().tolist()
Non__Transacted_df_summary=Non__Transacted_df_summary_list_1_4.append(pd.DataFrame({"Call_Times":"5-10","Non_Transacted_Phones":[Non__Transacted_df_summary_list_5_Plus_list_Phone]}))
Non__Transacted_df_summary['Non_Transacted_Calls']=np.nan

Non__Transacted_df_summary=Non__Transacted_df_summary.reset_index()
del Non__Transacted_df_summary['index']
for i in range(len(Non__Transacted_df_summary)):
    phone_list=Non__Transacted_df_summary['Non_Transacted_Phones'][i]
    df=unique_non_transacted_phone_calls[unique_non_transacted_phone_calls['Phone_Num'].isin(phone_list)]
    counts=len(df)
    Non__Transacted_df_summary['Non_Transacted_Calls'][i]=counts
Non__Transacted_df_summary['Unique_Non_Trancted_Call_Count']=Non__Transacted_df_summary['Non_Transacted_Phones'].apply(len)
    


# In[75]:

df_1_output=pd.merge(Transaced_df_summary,Non__Transacted_df_summary,on='Call_Times',how="left")
df_1_output['Total_Overall_Calls']=df_1_output['Non_Transacted_Calls']+df_1_output['Total_Transacted_Calls']
df_1_writer=pd.ExcelWriter(writer_folder+"Output_1_Summary_"+today_str+".xlsx",engine="xlsxwriter")
df_1_output.to_excel(df_1_writer,"Output_1_Summary.csv",index=False)
df_1_writer.save()


# In[76]:

df_1_output['buyer_count'].sum()


# # Df_0 Ratio

# In[77]:

Output_Ratio_writer=pd.ExcelWriter(writer_folder+"Output_0_Ratio_by_Group_"+today_str+".xlsx",engine="xlsxwriter")
Output_Ratio=base_df_transacted.copy()
Output_Ratio['Ratio']=np.nan
Output_Ratio['Buyers_Count']=np.nan
Output_Ratio['Phones_Count']=np.nan
for i in range(5):
    buyers_called=Output_Ratio['buyer_list'][i]
    match_buyer_phone_list_called=match_buyer_phone_list[match_buyer_phone_list['defined_buyer_id'].isin(buyers_called)]
    match_buyer_phone_list_called['len_phone']=match_buyer_phone_list_called['Phone'].apply(lambda x:len(x))
    Ratio_Phone_to_Buyer_Called=match_buyer_phone_list_called['len_phone'].mean()
    Output_Ratio['Ratio'][i]=Ratio_Phone_to_Buyer_Called
    Output_Ratio['Buyers_Count'][i]=len(match_buyer_phone_list_called['defined_buyer_id'].unique().tolist())
    Output_Ratio['Phones_Count'][i]=match_buyer_phone_list_called['len_phone'].sum()
    
Output_Ratio.to_excel(Output_Ratio_writer,"Ratio",index=False)
Output_Ratio_writer.save()


# # Df_2 From Call to Trans Time length

# In[78]:

# Transacted data only
call_diff_time_writer=pd.ExcelWriter(writer_folder+"Output_2_Five_Group_Call_to_Transactions"+today_str+".xlsx",engine="xlsxwriter")
Call_to_Trans_Length=base_df_transacted.copy()
Call_to_Trans_Length['Day_Length']=np.nan

Call_to_Trans_Length_call=pd.DataFrame()
Call_to_Trans_Length_online=pd.DataFrame()
for i in range(len(Call_to_Trans_Length)):
    buyer_list=Call_to_Trans_Length['buyer_list'][i]
    df_trans=transaction_data_1st_Only[transaction_data_1st_Only['defined_buyer_id'].isin(buyer_list)]
    df_trans['Trans_1st_Time_plust_1_hour']=df_trans['Date_Time'].apply(lambda x: x+datetime.timedelta(hours=1))
    
    df_trans_time=df_trans[['defined_buyer_id','Trans_1st_Time_plust_1_hour','Source']]
    df_trans_time=df_trans_time.rename(columns={"Trans_1st_Time_plust_1_hour":"Trans_Time_plus_1"})
    
    df_call=unique_Transacted_phone_calls_matched[unique_Transacted_phone_calls_matched['defined_buyer_id'].isin(buyer_list)]
    df_call=df_call[df_call['Call_Seq']==1]
    df_call_time=df_call[['defined_buyer_id','Start_time']]
    df_call_time=df_call_time.rename(columns={"Start_time":"Call_First_Time"})
    if len(df_trans_time)!=len(df_trans_time):
        print(i)
    df_diff=pd.merge(df_call_time,df_trans_time,on="defined_buyer_id",how='outer')
    df_diff['Buyer_Group']='Call_Times_'+str(Call_to_Trans_Length['Call_Times'][i])
    
    
    
    df_diff_online=df_diff[df_diff['Source']=="online"]
    df_diff_phone=df_diff[df_diff['Source']=="phone"]

    df_diff_online['Diff_Time']=df_diff_online['Trans_Time_plus_1']-df_diff_online['Call_First_Time']
    df_diff_online['Day_Length']=df_diff_online['Diff_Time'].apply(lambda x: x.days) #Use +1 Hour
    df_diff_online['Source']='online'
    
    df_diff_phone['Diff_Time']=df_diff_phone['Trans_Time_plus_1']-df_diff_phone['Call_First_Time']
    df_diff_phone['Day_Length']=df_diff_phone['Diff_Time'].apply(lambda x: x.days) #Use +1 Hour
    df_diff_phone['Source']='phone'
    
    df_diff=df_diff_online.append(df_diff_phone)
    df_diff.to_excel(call_diff_time_writer,"group_freq_"+str(i+1),index=False)
    
    Call_to_Trans_Length['Day_Length'][i]=df_diff['Day_Length'].mean()
    
    buyer_list_call=df_diff_phone['defined_buyer_id'].unique().tolist()
    buyer_list_online=df_diff_online['defined_buyer_id'].unique().tolist()
    
    call_app=pd.DataFrame({"Call_Times":str(i+1),"buyer_count":len(buyer_list_call),                          "buyer_list":[buyer_list_call],"Day_Length":df_diff_phone['Day_Length'].mean()},index=[i])
    online_app=pd.DataFrame({"Call_Times":str(i+1),"buyer_count":len(buyer_list_online),                          "buyer_list":[buyer_list_online],"Day_Length":df_diff_online['Day_Length'].mean()},index=[i])
    Call_to_Trans_Length_call=Call_to_Trans_Length_call.append(call_app)
    Call_to_Trans_Length_online=Call_to_Trans_Length_online.append(online_app)
    
    
Call_to_Trans_Length.to_excel(call_diff_time_writer,"avg_length_overall",index=False)

Call_to_Trans_Length_call['Call_Times']=Call_to_Trans_Length_call['Call_Times'].apply(lambda x: x.replace("5","5-10"))
Call_to_Trans_Length_online['Call_Times']=Call_to_Trans_Length_online['Call_Times'].apply(lambda x: x.replace("5","5-10"))

Call_to_Trans_Length_call.to_excel(call_diff_time_writer,"avg_length_call",index=False)
Call_to_Trans_Length_online.to_excel(call_diff_time_writer,"avg_length_online",index=False)

call_diff_time_writer.save()


# # Df_3 Weekday Call Volumes

# In[79]:

weekday_call_df_non_trans=unique_non_transacted_phone_calls[['Phone_Num','Start_time','Call_Seq']]
weekday_call_df_non_trans.columns=["Phone|Buyer","Start_time_Call",'Call_Seq']
weekday_call_df_non_trans['weekday']=weekday_call_df_non_trans['Start_time_Call'].apply(lambda x: str(x.weekday()))
weekday_call_df_non_trans['Day_of_Week']=weekday_call_df_non_trans['weekday'].apply(lambda x: x.replace("0","Monday").                                                                                    replace("1","Tuesday").replace("2","Wednesday").                                                                                   replace("3","Thursday").replace("4","Friday").                                                                                   replace("5","Saturday").replace("6","Sunday"))
                                                                   
                                                                   
weekday_call_df_transacted=unique_Transacted_phone_calls_matched[['defined_buyer_id','Start_time','Call_Seq']]
weekday_call_df_transacted.columns=["Phone|Buyer","Start_time_Call",'Call_Seq']
weekday_call_df_transacted['weekday']=weekday_call_df_transacted['Start_time_Call'].apply(lambda x: str(x.weekday()))
weekday_call_df_transacted['Day_of_Week']=weekday_call_df_transacted['weekday'].apply(lambda x: x.replace("0","Monday").                                                                                    replace("1","Tuesday").replace("2","Wednesday").                                                                                   replace("3","Thursday").replace("4","Friday").                                                                                   replace("5","Saturday").replace("6","Sunday"))

weekday_call_df_both=weekday_call_df_non_trans.append(weekday_call_df_transacted)


output_3_both=weekday_call_df_both.groupby(['weekday','Day_of_Week'])['Phone|Buyer'].count().to_frame().reset_index()
output_3_both=output_3_both.rename(columns={"Phone|Buyer":"Call_From_Both"})
output_3_transacted=weekday_call_df_transacted.groupby(['weekday','Day_of_Week'])['Phone|Buyer'].count().to_frame().reset_index()
output_3_transacted=output_3_transacted.rename(columns={"Phone|Buyer":"Call_From_Buyers"})
output_3_non_trans=weekday_call_df_non_trans.groupby(['weekday','Day_of_Week'])['Phone|Buyer'].count().to_frame().reset_index()
output_3_non_trans=output_3_non_trans.rename(columns={"Phone|Buyer":"Call_From_Non_Buyers"})

output_3_weekday_overll=pd.merge(output_3_transacted,output_3_non_trans,on=['weekday',"Day_of_Week"])
output_3_weekday_overll=pd.merge(output_3_weekday_overll,output_3_both,on=['weekday',"Day_of_Week"])



# In[80]:

# 3.1 Buyer 
weekday_group_buyer=pd.DataFrame(columns={'Buyer_List',"Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"},index=['1','2','3','4','5-10'])
weekday_group_buyer=weekday_group_buyer.reset_index()
weekday_group_buyer=weekday_group_buyer.rename(columns={"index":"Call_Times"})
weekday_group_buyer=weekday_group_buyer[['Call_Times','Buyer_List']+["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]]

for i in range(len(Transaced_df_summary)):
    Buyer_List=Transaced_df_summary['buyer_list'][i]
    df_call=unique_Transacted_phone_calls_matched[unique_Transacted_phone_calls_matched['defined_buyer_id'].isin(Buyer_List)][['defined_buyer_id','Start_time','Call_Seq']]
    df_call.columns=["Phone|Buyer","Start_time_Call",'Call_Seq']
    df_call['weekday']=df_call['Start_time_Call'].apply(lambda x: str(x.weekday()))
    df_call['Day_of_Week']=df_call['weekday'].apply(lambda x: x.replace("0","Monday").                                                                                    replace("1","Tuesday").replace("2","Wednesday").                                                                                   replace("3","Thursday").replace("4","Friday").                                                                                   replace("5","Saturday").replace("6","Sunday"))
    df_group_weekday=df_call.groupby(['Day_of_Week'])['Phone|Buyer'].count().to_frame().reset_index()
    for weekday in ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]:
        weekday_group_buyer[weekday][i]=df_group_weekday[df_group_weekday['Day_of_Week']==weekday]['Phone|Buyer'].unique()[0]
    weekday_group_buyer['Buyer_List'][i]=Buyer_List
    
weekday_group_buyer['Buyer_Count']=weekday_group_buyer['Buyer_List'].apply(lambda x: len(x))
    


# In[81]:

# 3.2 Non-Buyer 

weekday_group_non_buyer=pd.DataFrame(columns={'Phone_List',"Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"},index=['1','2','3','4','5-10'])
weekday_group_non_buyer=weekday_group_non_buyer.reset_index()
weekday_group_non_buyer=weekday_group_non_buyer.rename(columns={"index":"Call_Times"})
weekday_group_non_buyer=weekday_group_non_buyer[['Call_Times','Phone_List']+["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]]

for i in range(len(Non__Transacted_df_summary)):
    Phone_List=Non__Transacted_df_summary['Non_Transacted_Phones'][i]
    df_call=unique_non_transacted_phone_calls[unique_non_transacted_phone_calls['Phone_Num'].isin(Phone_List)][['Phone_Num','Start_time','Call_Seq']]
    df_call.columns=["Phone|Buyer","Start_time_Call",'Call_Seq']
    df_call['weekday']=df_call['Start_time_Call'].apply(lambda x: str(x.weekday()))
    df_call['Day_of_Week']=df_call['weekday'].apply(lambda x: x.replace("0","Monday").                                                                                    replace("1","Tuesday").replace("2","Wednesday").                                                                                   replace("3","Thursday").replace("4","Friday").                                                                                   replace("5","Saturday").replace("6","Sunday"))
    df_group_weekday=df_call.groupby(['Day_of_Week'])['Phone|Buyer'].count().to_frame().reset_index()
    for weekday in ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]:
        weekday_group_non_buyer[weekday][i]=df_group_weekday[df_group_weekday['Day_of_Week']==weekday]['Phone|Buyer'].unique()[0]
    weekday_group_non_buyer['Phone_List'][i]=Phone_List
    
weekday_group_non_buyer['Phone_Count']=weekday_group_non_buyer['Phone_List'].apply(lambda x: len(x))


# In[82]:

weekday_group_both=weekday_group_non_buyer[['Call_Times','Phone_List']]
weekday_group_both['Buyer_List']=weekday_group_buyer['Buyer_List']

for col in ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]:
    weekday_group_both[col]=np.nan
    for i in range(len(weekday_group_both)):
        weekday_group_both[col][i]=weekday_group_buyer[col][i]+weekday_group_non_buyer[col][i]
weekday_group_both['id_count']=np.nan
for i in range(len(weekday_group_both)):
    weekday_group_both['id_count'][i]=weekday_group_buyer['Buyer_Count'][i]+weekday_group_non_buyer['Phone_Count'][i]


# In[83]:

output_3_weekday_writer=pd.ExcelWriter(writer_folder+"Output_3_Weekday_of_Calls_"+today_str+".xlsx",engine="xlsxwriter")
output_3_weekday_overll.to_excel(output_3_weekday_writer,'weekday_overall_brief',index=False)
weekday_group_buyer.to_excel(output_3_weekday_writer,'weekday_group_buyer',index=False)
weekday_group_non_buyer.to_excel(output_3_weekday_writer,'weekday_group_non_buyer',index=False)
weekday_group_both.to_excel(output_3_weekday_writer,'weekday_group_both',index=False)
output_3_weekday_writer.save()


# # Df_4 Day_Hour Call Volumes

# In[84]:

hour_call_df_non_trans=unique_non_transacted_phone_calls[['Phone_Num','Start_time','Call_Seq']]
hour_call_df_non_trans.columns=["Phone|Buyer","Start_time_Call",'Call_Seq']
hour_call_df_non_trans['Hour']=hour_call_df_non_trans['Start_time_Call'].apply(lambda x: str(x.hour))
                                                                  
hour_call_df_transacted=unique_Transacted_phone_calls_matched[['defined_buyer_id','Start_time','Call_Seq']]
hour_call_df_transacted.columns=["Phone|Buyer","Start_time_Call",'Call_Seq']
hour_call_df_transacted['Hour']=hour_call_df_transacted['Start_time_Call'].apply(lambda x: str(x.hour))

hour_call_df_both=hour_call_df_non_trans.append(hour_call_df_transacted)


output_4_both=hour_call_df_both.groupby(['Hour'])['Phone|Buyer'].count().to_frame().reset_index()
output_4_both=output_4_both.rename(columns={"Phone|Buyer":"Call_From_Both"})
output_4_transacted=hour_call_df_transacted.groupby(['Hour'])['Phone|Buyer'].count().to_frame().reset_index()
output_4_transacted=output_4_transacted.rename(columns={"Phone|Buyer":"Call_From_Buyers"})
output_4_non_trans=hour_call_df_non_trans.groupby(['Hour'])['Phone|Buyer'].count().to_frame().reset_index()
output_4_non_trans=output_4_non_trans.rename(columns={"Phone|Buyer":"Call_From_Non_Buyers"})

output_4_hour_overll=pd.merge(output_4_transacted,output_4_non_trans,on=['Hour'])
output_4_hour_overll=pd.merge(output_4_hour_overll,output_4_both,on=['Hour'])
output_4_hour_overll['Hour']=output_4_hour_overll['Hour'].astype(int)
output_4_hour_overll=output_4_hour_overll.sort_values("Hour")
output_4_hour_overll=output_4_hour_overll.reset_index()
del output_4_hour_overll['index']


# In[85]:

# 4.1 Buyer 
hour_group_buyer=pd.DataFrame(columns=set(['Buyer_List']+[str(x) for x in range(24)]),index=['1','2','3','4','5-10'])
hour_group_buyer=hour_group_buyer.reset_index()
hour_group_buyer=hour_group_buyer.rename(columns={"index":"Call_Times"})
hour_group_buyer=hour_group_buyer[['Call_Times','Buyer_List']+[str(x) for x in range(24)]]

for i in range(len(Transaced_df_summary)):
    Buyer_List=Transaced_df_summary['buyer_list'][i]
    df_call=unique_Transacted_phone_calls_matched[unique_Transacted_phone_calls_matched['defined_buyer_id'].isin(Buyer_List)][['defined_buyer_id','Start_time','Call_Seq']]
    df_call.columns=["Phone|Buyer","Start_time_Call",'Call_Seq']
    df_call['Hour']=df_call['Start_time_Call'].apply(lambda x: str(x.hour))
    df_group_hour=df_call.groupby(['Hour'])['Phone|Buyer'].count().to_frame().reset_index()
    
    for hour in [str(x) for x in range(24)]:
        try:
            hour_group_calls=df_group_hour[df_group_hour['Hour']==hour]['Phone|Buyer'].unique()[0]
        
        except:
            hour_group_calls=0
        
        hour_group_buyer[hour][i]=hour_group_calls
    hour_group_buyer['Buyer_List'][i]=Buyer_List
    
hour_group_buyer['Buyer_Count']=hour_group_buyer['Buyer_List'].apply(lambda x: len(x))


# In[86]:

# 4.2 Non-Buyer 

hour_group_non_buyer=pd.DataFrame(columns=set(['Phone_List']+[str(x) for x in range(24)]),index=['1','2','3','4','5-10'])
hour_group_non_buyer=hour_group_non_buyer.reset_index()
hour_group_non_buyer=hour_group_non_buyer.rename(columns={"index":"Call_Times"})
hour_group_non_buyer=hour_group_non_buyer[['Call_Times','Phone_List']+[str(x) for x in range(24)]]

for i in range(len(Non__Transacted_df_summary)):
    Phone_List=Non__Transacted_df_summary['Non_Transacted_Phones'][i]
    df_call=unique_non_transacted_phone_calls[unique_non_transacted_phone_calls['Phone_Num'].isin(Phone_List)][['Phone_Num','Start_time','Call_Seq']]
    df_call.columns=["Phone|Buyer","Start_time_Call",'Call_Seq']
    df_call['Hour']=df_call['Start_time_Call'].apply(lambda x: str(x.hour))
    
    df_group_hour=df_call.groupby(['Hour'])['Phone|Buyer'].count().to_frame().reset_index()
    for hour in [str(x) for x in range(24)]:
        hour_group_non_buyer[hour][i]=df_group_hour[df_group_hour['Hour']==hour]['Phone|Buyer'].unique()[0]
    hour_group_non_buyer['Phone_List'][i]=Phone_List
    
hour_group_non_buyer['Phone_Count']=hour_group_non_buyer['Phone_List'].apply(lambda x: len(x))


# In[87]:

hour_group_both=hour_group_non_buyer[['Call_Times','Phone_List']]
hour_group_both['Buyer_List']=hour_group_buyer['Buyer_List']

for col in [str(x) for x in range(24)]:
    hour_group_both[col]=np.nan
    for i in range(len(hour_group_both)):
        hour_group_both[col][i]=hour_group_buyer[col][i]+hour_group_non_buyer[col][i]
hour_group_both['id_count']=np.nan
for i in range(len(hour_group_both)):
    hour_group_both['id_count'][i]=hour_group_buyer['Buyer_Count'][i]+hour_group_non_buyer['Phone_Count'][i]


# In[88]:

output_4_hour_writer=pd.ExcelWriter(writer_folder+"Output_4_hour_of_Calls_"+today_str+".xlsx",engine="xlsxwriter")
output_4_hour_overll.to_excel(output_4_hour_writer,'hour_overall_brief',index=False)
hour_group_buyer.to_excel(output_4_hour_writer,'hour_group_buyer',index=False)
hour_group_non_buyer.to_excel(output_4_hour_writer,'hour_group_non_buyer',index=False)
hour_group_both.to_excel(output_4_hour_writer,'hour_group_both',index=False)
output_4_hour_writer.save()


# # Df_5 DMA Summary

# In[89]:

Buyer_DMA=transaction_data_1st_Only[['defined_buyer_id','zip_cd']]
transacted_phone_DMA=unique_Transacted_phone_calls_matched[['defined_buyer_id','Source','Start_time']]
transacted_phone_DMA=pd.merge(transacted_phone_DMA,Buyer_DMA,on="defined_buyer_id",how="left")
transacted_phone_DMA=pd.merge(transacted_phone_DMA,zip_DMA,on="zip_cd",how="left")
transacted_phone_DMA['DMA']=transacted_phone_DMA['DMA'].fillna("non-US")
transacted_phone_DMA_total_calls=transacted_phone_DMA.groupby(['DMA','Source'])['Start_time'].count().to_frame().reset_index()
transacted_phone_DMA_total_calls=transacted_phone_DMA_total_calls.rename(columns={"Start_time":"Total_Transacted_Call_Count"})

transacted_phone_DMA_buyer_count=transacted_phone_DMA.groupby(['DMA','Source'])['defined_buyer_id'].apply(lambda x: len(set(x))).to_frame().reset_index()
transacted_phone_DMA_buyer_count=transacted_phone_DMA_buyer_count.rename(columns={"defined_buyer_id":"Buyer_Count"})

transacted_phone_DMA_both=pd.merge(transacted_phone_DMA_total_calls,transacted_phone_DMA_buyer_count,on=["DMA",'Source'])
transacted_phone_phone=transacted_phone_DMA_both[transacted_phone_DMA_both['Source']=="phone"]
transacted_phone_online=transacted_phone_DMA_both[transacted_phone_DMA_both['Source']=="online"]
transacted_phone_DMA_agg_both=transacted_phone_DMA_both.groupby(['DMA'])['Total_Transacted_Call_Count','Buyer_Count'].sum().reset_index()



# In[90]:

every_transaction_DMA=pd.merge(transaction_data_both,zip_DMA,on='zip_cd',how="left")
every_transaction_DMA['DMA']=every_transaction_DMA['DMA'].fillna("non-US")
every_transaction_DMA_both=every_transaction_DMA.groupby(['DMA'])['Order_id'].count().to_frame().reset_index()
every_transaction_DMA_both=every_transaction_DMA_both.rename(columns={"Order_id":"Every_Single_Order_Count"})

every_transaction_DMA_Phone=every_transaction_DMA[every_transaction_DMA['Source']=='phone']
every_transaction_DMA_Phone=every_transaction_DMA_Phone.groupby(['DMA'])['Order_id'].count().to_frame().reset_index()
every_transaction_DMA_Phone=every_transaction_DMA_Phone.rename(columns={"Order_id":"Every_Phone_Order_Count"})


every_transaction_DMA_Online=every_transaction_DMA[every_transaction_DMA['Source']=='online']
every_transaction_DMA_Online=every_transaction_DMA_Online.groupby(['DMA'])['Order_id'].count().to_frame().reset_index()
every_transaction_DMA_Online=every_transaction_DMA_Online.rename(columns={"Order_id":"Every_Online_Order_Count"})



# In[91]:

output_5_DMA_writer=pd.ExcelWriter(writer_folder+"Output_5_DMA_of_Calls_"+today_str+".xlsx",engine="xlsxwriter")

transacted_phone_DMA_agg_both.to_excel(output_5_DMA_writer,'transacted_phone_DMA_agg_both',index=False)
transacted_phone_phone.to_excel(output_5_DMA_writer,'transacted_phone_phone',index=False)
transacted_phone_online.to_excel(output_5_DMA_writer,'transacted_phone_online',index=False)

every_transaction_DMA_both.to_excel(output_5_DMA_writer,'every_transaction_DMA_both',index=False)
every_transaction_DMA_Phone.to_excel(output_5_DMA_writer,'every_transaction_DMA_Phone',index=False)
every_transaction_DMA_Online.to_excel(output_5_DMA_writer,'every_transaction_DMA_Online',index=False)

output_5_DMA_writer.save()


# # Df_6 Memorial Week Comparison

# In[92]:

# Memorial Week Range = (2018,5,24) (2018,6,3)
# Prior Week Range = (2018,5,10) (2018,5,20)
start_date_prior_week=datetime.date(2018,5,10)
end_date_prior_week=datetime.date(2018,5,20)

start_date_memorial_week=datetime.date(2018,5,24)
end_date_memorial_week=datetime.date(2018,6,3)


# In[93]:

# To correct the error about more buyers than trans
# To coorect unique non transacted calls
def Time_Range_Transacted_Summary_Table(df_input,start_time,end_time):
    df_unique_matched_trans_calls=df_input[(df_input['Start_time']>=start_time) & (df_input['Start_time']<end_time+datetime.timedelta(days=1))]
    Transacted_Call_Freq_by_buyers=df_unique_matched_trans_calls.groupby(['defined_buyer_id'])['Start_time'].count().to_frame().reset_index()
    Transacted_Call_Freq_by_buyers=Transacted_Call_Freq_by_buyers.rename(columns={"Start_time":"Call_Times"})

    Transacted_Call_Freq_by_buyers_1_to_4=Transacted_Call_Freq_by_buyers[Transacted_Call_Freq_by_buyers['Call_Times']<=4]
    Transacted_Call_Freq_by_buyers_5_plus=Transacted_Call_Freq_by_buyers[Transacted_Call_Freq_by_buyers['Call_Times']>4]

    df_1=Transacted_Call_Freq_by_buyers_1_to_4.groupby(['Call_Times'])['defined_buyer_id'].apply(list).to_frame().reset_index()
    df_1=df_1.rename(columns={"defined_buyer_id":"buyer_list"})

    df_2=Transacted_Call_Freq_by_buyers_1_to_4.groupby(['Call_Times'])['defined_buyer_id'].count().to_frame().reset_index()
    df_2=df_2.rename(columns={"defined_buyer_id":"buyer_count"})

    df_buyer=pd.merge(df_1,df_2,on="Call_Times",how="outer")
    
    df_5_plus_app=pd.DataFrame({"Call_Times":"5-10","buyer_list":[Transacted_Call_Freq_by_buyers_5_plus['defined_buyer_id'].unique().tolist()],"buyer_count":len(Transacted_Call_Freq_by_buyers_5_plus['defined_buyer_id'].unique().tolist())},index=[4])
    df_buyer=df_buyer.append(df_5_plus_app)
    
    
    # 1. Transacted
    Transaced_df_summary=df_buyer.copy()
    Transaced_df_summary['Total_Buyer']=np.nan
    Transaced_df_summary['Phone_Buyer']=np.nan
    Transaced_df_summary['Online_Buyer']=np.nan

    Transaced_df_summary['Total_Trans']=np.nan
    Transaced_df_summary['Phone_Trans']=np.nan
    Transaced_df_summary['Online_Trans']=np.nan

    Transaced_df_summary['Total_Transacted_Calls']=np.nan

    # Use 1st Time Transaction only : transaction_data_1st_Only
    # Because use the 1st time transaction, the total equals to the sum of both sources
    transaction_data_range=transaction_data[(transaction_data['Date_Time']>=start_time) & (transaction_data['Date_Time']<end_time+datetime.timedelta(days=1))]
    for i in range(5):
        buyer_list=Transaced_df_summary['buyer_list'][i]
        df_trans=transaction_data_range[transaction_data_range['defined_buyer_id'].isin(buyer_list)]
        type_of_order_order=df_trans.groupby(['Source'])['Order_id'].count().to_frame().reset_index()

        try:
            phone_trans_order=type_of_order_order[type_of_order_order['Source']=="phone"]['Order_id'].unique()[0]
        except:
            phone_trans_order=0
        try:
            online_trans_order=type_of_order_order[type_of_order_order['Source']=="online"]['Order_id'].unique()[0]
        except:
            online_trans_order=0
        both_trans_order=len(df_trans['Order_id'].unique())
        
        transaction_data_1st_Only_range=transaction_data_1st_Only[(transaction_data_1st_Only['Date_Time']>=start_time) &                                                                 (transaction_data_1st_Only['Date_Time']<end_time+datetime.timedelta(days=1))]
        df=transaction_data_1st_Only_range[transaction_data_1st_Only_range['defined_buyer_id'].isin(buyer_list)]
        type_of_order_buyer=df.groupby(['Source'])['defined_buyer_id'].apply(lambda x: len(x.unique())).to_frame().reset_index()
        
        try:
            phone_trans_buyer=type_of_order_buyer[type_of_order_buyer['Source']=="phone"]['defined_buyer_id'].unique()[0]
        except:
            phone_trans_buyer=0
        try:
            online_trans_buyer=type_of_order_buyer[type_of_order_buyer['Source']=="online"]['defined_buyer_id'].unique()[0]
        except:
            online_trans_buyer=0
        both_trans_buyer=len(df['Order_id'].unique())


        Transaced_df_summary['Phone_Trans'][i]=phone_trans_order
        Transaced_df_summary['Online_Trans'][i]=online_trans_order
        Transaced_df_summary['Total_Trans'][i]=both_trans_order

        Transaced_df_summary['Phone_Buyer'][i]=phone_trans_buyer
        Transaced_df_summary['Online_Buyer'][i]=online_trans_buyer
        Transaced_df_summary['Total_Buyer'][i]=both_trans_buyer

        df_total_call=df_unique_matched_trans_calls[df_unique_matched_trans_calls['defined_buyer_id'].isin(buyer_list)]
        total_calls=len(df_total_call)
        Transaced_df_summary['Total_Transacted_Calls'][i]=total_calls

    Transaced_df_summary=Transaced_df_summary[['Call_Times','buyer_count','buyer_list','Total_Transacted_Calls','Phone_Trans','Online_Trans','Total_Trans',
                                              'Phone_Buyer','Online_Buyer','Total_Buyer']]
    return Transaced_df_summary
    


# In[94]:

#2. Non Buyers
def Time_Range_Non_Transacted_Summary_Table(df_input,start_time,end_time):
    unique_non_transacted_phone_calls_range=df_input[(df_input['Start_time']>=start_time) & (df_input['Start_time']<end_time+datetime.timedelta(days=1))]

    Non__Transacted_df_summary_count=unique_non_transacted_phone_calls_range.groupby(['Phone_Num'])['Start_time'].count().to_frame().reset_index()
    # exclusions_non_transactions=Non__Transacted_df_summary_count[Non__Transacted_df_summary_count['Start_time']>=100]
    # Non__Transacted_df_summary_count=Non__Transacted_df_summary_count[Non__Transacted_df_summary_count['Start_time']<100]


    Non__Transacted_df_summary_list=Non__Transacted_df_summary_count.groupby(['Start_time'])['Phone_Num'].apply(list).to_frame().reset_index()
    Non__Transacted_df_summary_list=Non__Transacted_df_summary_list.rename(columns={"Start_time":"Call_Times","Phone_Num":"Non_Transacted_Phones"})

    Non__Transacted_df_summary_list_1_4=Non__Transacted_df_summary_list[Non__Transacted_df_summary_list['Call_Times']<=4]

    Non__Transacted_df_summary_list_5_Plus_list_Phone=Non__Transacted_df_summary_count[Non__Transacted_df_summary_count['Start_time']>=5]['Phone_Num'].unique().tolist()
    Non__Transacted_df_summary=Non__Transacted_df_summary_list_1_4.append(pd.DataFrame({"Call_Times":"5-10","Non_Transacted_Phones":[Non__Transacted_df_summary_list_5_Plus_list_Phone]}))
    Non__Transacted_df_summary['Non_Transacted_Calls']=np.nan

    Non__Transacted_df_summary=Non__Transacted_df_summary.reset_index()
    del Non__Transacted_df_summary['index']
    for i in range(len(Non__Transacted_df_summary)):
        phone_list=Non__Transacted_df_summary['Non_Transacted_Phones'][i]
        df=unique_non_transacted_phone_calls_range[unique_non_transacted_phone_calls_range['Phone_Num'].isin(phone_list)]
        counts=len(df)
        Non__Transacted_df_summary['Non_Transacted_Calls'][i]=counts
    Non__Transacted_df_summary['Unique_Non_Trancted_Call_Count']=Non__Transacted_df_summary['Non_Transacted_Phones'].apply(len)
    return Non__Transacted_df_summary


# In[95]:

memorial_transacted=Time_Range_Transacted_Summary_Table(unique_Transacted_phone_calls_matched,start_date_memorial_week,end_date_memorial_week)
memorial_non_transacted=Time_Range_Non_Transacted_Summary_Table(unique_non_transacted_phone_calls,start_date_memorial_week,end_date_memorial_week)
memorial_week=pd.merge(memorial_transacted,memorial_non_transacted,on='Call_Times',how="left")

prior_transacted=Time_Range_Transacted_Summary_Table(unique_Transacted_phone_calls_matched,start_date_prior_week,end_date_prior_week)
prior_non_transacted=Time_Range_Non_Transacted_Summary_Table(unique_non_transacted_phone_calls,start_date_prior_week,end_date_prior_week)
prior_week=pd.merge(prior_transacted,prior_non_transacted,on='Call_Times',how="left")



# In[96]:

df_6_writer=pd.ExcelWriter(writer_folder+"Output_6_Memorial_Week_Comparison_"+today_str+".xlsx",engine='xlsxwriter')
memorial_week.to_excel(df_6_writer,"memorial_week",index=False)
prior_week.to_excel(df_6_writer,"prior_week",index=False)
df_6_writer.save()


# In[ ]:




# In[ ]:




# In[ ]:



