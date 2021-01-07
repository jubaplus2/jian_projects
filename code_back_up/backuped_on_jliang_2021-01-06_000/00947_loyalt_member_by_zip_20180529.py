import pandas as pd
import hashlib
import datetime

data_registor_2=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/Email_Zips/MediaStormCustomerTransactionTotals_2018-01-09_2018-03-31.txt",usecols=[0,2,5],dtype=str)

data_registor_1 = pd.read_table('/home/jian/Projects/Big_Lots/Loyal_members/Email_Zips/MediaStormCustTot-hashed-email.txt',
                       header=None,sep = ',',usecols=[0,2,5],skiprows = [7625478, 12278302, 12372213, 12519621, 12682600, 14718389,  19001868],dtype=str)


data_registor_1.columns=data_registor_2.columns.tolist()
data_registor_1['customer_id_hashed']=data_registor_1['customer_id_hashed'].apply(lambda x: hashlib.sha256(x.encode('UTF-8')).hexdigest())
data_registor_1['sign_up_date']=data_registor_1['sign_up_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
data_registor_2['sign_up_date']=data_registor_2['sign_up_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())

data_registor_1=data_registor_1.drop_duplicates()
data_registor_2=data_registor_2.drop_duplicates()
data_registor=data_registor_1.append(data_registor_2)
data_registor=data_registor.sort_values('sign_up_date',ascending=False)
data_registor=data_registor.drop_duplicates('customer_id_hashed')

data_registor.to_csv("/home/jian/Projects/Big_Lots/Loyal_members/Email_Zips/output_loyalty_member_by_id.csv",index=False)

