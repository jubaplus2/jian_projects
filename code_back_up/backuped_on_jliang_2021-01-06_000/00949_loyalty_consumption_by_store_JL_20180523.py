import pandas as pd
import datetime
data_1=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/sales_of_loyalty_member/MediaStormCustDtl.txt",header=None,dtype=str)
data_2=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/sales_of_loyalty_member/MediaStorm customer transaction details - 2018-01-09 - 2018-03-31.txt",dtype=str)
data_3=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/sales_of_loyalty_member/MediaStorm customer transaction details - 2018-04-01 - 2018-04-15.txt",dtype=str)

data_1.columns=['customer_id']+data_2.columns.tolist()[1:]
def clean_data(df):
    df=df.iloc[:,1:]
    del df['merch_cat']
    df['total_transaction_amt']=df['total_transaction_amt'].astype(float)
    df=df.drop_duplicates()    
    return df
data_1=clean_data(data_1)
data_2=clean_data(data_2)
data_3=clean_data(data_3)
data_all=data_1.append(data_2).append(data_3).drop_duplicates()
data_all=data_all[['transaction_date','location_id','total_transaction_amt']]

output=data_all.groupby(['location_id','transaction_date'])['total_transaction_amt'].sum().to_frame().reset_index()
output.to_csv("/home/jian/Projects/Big_Lots/Loyal_members/sales_of_loyalty_member/output_of_loyalty_consumption_by_location.csv",index=False)