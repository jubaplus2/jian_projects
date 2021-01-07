# -*- coding: utf-8 -*-
"""
Created on Sat Mar 12 16:15:36 2016

@author: Spencer
"""

import os
os.chdir("/Users/jubaplus1/Downloads")
import pandas as pd
import numpy as np

#####################################

w0=pd.read_csv("USNG_deciles_2015-11-02-2015-11-15-1887065.txt",sep=",")
w1=pd.read_csv("USNG_deciles_2015-11-16-2015-11-29-1887065.txt",sep=",")
w2=pd.read_csv("USNG_deciles_2015-11-30-2015-12-13-1887065.txt",sep=",")
w3=pd.read_csv("USNG_deciles_2015-12-14-2015-12-27-1887065.txt",sep=",")
w4=pd.read_csv("USNG_deciles_2015-12-28-2016-01-10-1887065.txt",sep=",")
w5=pd.read_csv("USNG_deciles_2016-01-11-2016-01-24-1887065.txt",sep=",")
w6=pd.read_csv("USNG_deciles_2016-01-25-2016-02-07-1887065.txt",sep=",")
w7=pd.read_csv("USNG_deciles_2016-02-08-2016-02-21-1887065.txt",sep=",")
w8=pd.read_csv("USNG_deciles_2016-02-22-2016-03-06-1887065.txt",sep=",")
w9=pd.read_csv("USNG_deciles_2016-03-07-2016-03-20-1887065.txt",sep=",")
w10=pd.read_csv("USNG_deciles_2016-03-21-2016-04-03-1887065.txt",sep=",")

w0.columns=['USNG_100','s1w0']
w1.columns=['USNG_100','s1w1']
w2.columns=['USNG_100','s1w2']
w3.columns=['USNG_100','s1w3']
w4.columns=['USNG_100','s1w4']
w5.columns=['USNG_100','s1w5']
w6.columns=['USNG_100','s1w6']
w7.columns=['USNG_100','s1w7']
w8.columns=['USNG_100','s1w8']
w9.columns=['USNG_100','s1w9']
w10.columns=['USNG_100','s1w10']

show1=pd.merge(w0,w1,how='outer',on='USNG_100')
show1=pd.merge(show1,w2,how='outer',on='USNG_100')
show1=pd.merge(show1,w3,how='outer',on='USNG_100')
show1=pd.merge(show1,w4,how='outer',on='USNG_100')
show1=pd.merge(show1,w5,how='outer',on='USNG_100')
show1=pd.merge(show1,w6,how='outer',on='USNG_100')
show1=pd.merge(show1,w7,how='outer',on='USNG_100')
show1=pd.merge(show1,w8,how='outer',on='USNG_100')
show1=pd.merge(show1,w9,how='outer',on='USNG_100')
show1=pd.merge(show1,w10,how='outer',on='USNG_100')

zip=pd.read_csv("/Users/jubaplus1/Desktop/Work/Rentrak/ZipNew0519.csv",dtype={'Zip':'object','Zip4':'object'})
zip.columns=['Zip','Zip4','USNG_100']

show1=pd.merge(show1,zip,on='USNG_100')

del w0
del w1

import gc
gc.collect()

w0=pd.read_csv("USNG_deciles_2015-11-30-2015-12-13-2172792.txt",sep=",")
w1=pd.read_csv("USNG_deciles_2015-12-14-2015-12-27-2172792.txt",sep=",")
w2=pd.read_csv("USNG_deciles_2015-12-28-2016-01-10-2172792.txt",sep=",")
w3=pd.read_csv("USNG_deciles_2016-01-11-2016-01-24-2172792.txt",sep=",")
w4=pd.read_csv("USNG_deciles_2016-01-25-2016-02-07-2172792.txt",sep=",")
w5=pd.read_csv("USNG_deciles_2016-02-08-2016-02-21-2172792.txt",sep=",")
w6=pd.read_csv("USNG_deciles_2016-02-22-2016-03-06-2172792.txt",sep=",")


w0.columns=['USNG_100','s2w0']
w1.columns=['USNG_100','s2w1']
w2.columns=['USNG_100','s2w2']
w3.columns=['USNG_100','s2w3']
w4.columns=['USNG_100','s2w4']
w5.columns=['USNG_100','s2w5']
w6.columns=['USNG_100','s2w6']

show2=pd.merge(w0,w1,how='outer',on='USNG_100')
show2=pd.merge(show2,w2,how='outer',on='USNG_100')
show2=pd.merge(show2,w3,how='outer',on='USNG_100')
show2=pd.merge(show2,w4,how='outer',on='USNG_100')
show2=pd.merge(show2,w5,how='outer',on='USNG_100')
show2=pd.merge(show2,w6,how='outer',on='USNG_100')

show2=pd.merge(show2,zip,on='USNG_100')


del w0
del w1

import gc
gc.collect()

w0=pd.read_csv("USNG_deciles_2016-03-21-2016-04-03-2021069.txt",sep=",")
w1=pd.read_csv("USNG_deciles_2015-12-28-2016-01-10-2021069.txt",sep=",")
w2=pd.read_csv("USNG_deciles_2016-01-11-2016-01-24-2021069.txt",sep=",")
w3=pd.read_csv("USNG_deciles_2016-01-25-2016-02-07-2021069.txt",sep=",")
w4=pd.read_csv("USNG_deciles_2016-02-08-2016-02-21-2021069.txt",sep=",")
w5=pd.read_csv("USNG_deciles_2016-02-22-2016-03-06-2021069.txt",sep=",")
w6=pd.read_csv("USNG_deciles_2016-03-07-2016-03-20-2021069.txt",sep=",")
w7=pd.read_csv("USNG_deciles_2016-03-21-2016-04-03-2021069.txt",sep=",")

w0.columns=['USNG_100','s3w0']
w1.columns=['USNG_100','s3w1']
w2.columns=['USNG_100','s3w2']
w3.columns=['USNG_100','s3w3']
w4.columns=['USNG_100','s3w4']
w5.columns=['USNG_100','s3w5']
w6.columns=['USNG_100','s3w6']
w7.columns=['USNG_100','s3w7']

show3=pd.merge(w0,w1,how='outer',on='USNG_100')
show3=pd.merge(show3,w2,how='outer',on='USNG_100')
show3=pd.merge(show3,w3,how='outer',on='USNG_100')
show3=pd.merge(show3,w4,how='outer',on='USNG_100')
show3=pd.merge(show3,w5,how='outer',on='USNG_100')
show3=pd.merge(show3,w6,how='outer',on='USNG_100')
show3=pd.merge(show3,w7,how='outer',on='USNG_100')

show3=pd.merge(show3,zip,on='USNG_100')

del w1

###############################

print(show1.columns)
print(show2.columns)
print(show3.columns)

gc.collect()

combined=pd.merge(show1,show2,how='outer',on=['USNG_100', 'Zip','Zip4'])
combined=pd.merge(combined,show3,how='outer',on=['USNG_100', 'Zip','Zip4'])

combined=combined.fillna(0)

##combined['Segment']=np.where(,'A',np.where(,'B',np.where(,'C',np.where(,'D','Other'))))

combined['Segment']=np.where((((combined['s1w0']<4)&(combined['s1w0']>0))&
((combined['s1w1']<4)&(combined['s1w1']>0))&
((combined['s1w2']<4)&(combined['s1w2']>0))&
((combined['s1w3']<4)&(combined['s1w3']>0))&
((combined['s1w4']<4)&(combined['s1w4']>0))&
((combined['s1w5']<4)&(combined['s1w5']>0))&
((combined['s1w6']<4)&(combined['s1w6']>0))&
((combined['s1w7']<4)&(combined['s1w7']>0))&
((combined['s1w8']<4)&(combined['s1w8']>0))&
((combined['s1w9']<4)&(combined['s1w9']>0))&
((combined['s1w10']<4)&(combined['s1w10']>0))&
((combined['s2w0']<4)&(combined['s2w0']>0))&
((combined['s2w1']<4)&(combined['s2w1']>0))&
((combined['s2w2']<4)&(combined['s2w2']>0))&
((combined['s2w3']<4)&(combined['s2w3']>0))&
((combined['s2w4']<4)&(combined['s2w4']>0))&
((combined['s2w5']<4)&(combined['s2w5']>0))&
((combined['s2w6']<4)&(combined['s2w6']>0))&
((combined['s3w0']<4)&(combined['s3w0']>0))&
((combined['s3w1']<4)&(combined['s3w1']>0))&
((combined['s3w2']<4)&(combined['s3w2']>0))&
((combined['s3w3']<4)&(combined['s3w3']>0))&
((combined['s3w4']<4)&(combined['s3w4']>0))&
((combined['s3w5']<4)&(combined['s3w5']>0))&
((combined['s3w6']<4)&(combined['s3w6']>0))&
((combined['s3w7']<4)&(combined['s3w7']>0))),'A',np.where(((((combined['s1w0']<4)&(combined['s1w0']>0))|
((combined['s1w1']<4)&(combined['s1w1']>0))|
((combined['s1w2']<4)&(combined['s1w2']>0))|
((combined['s1w3']<4)&(combined['s1w3']>0))|
((combined['s1w4']<4)&(combined['s1w4']>0))|
((combined['s1w5']<4)&(combined['s1w5']>0))|
((combined['s1w6']<4)&(combined['s1w6']>0))|
((combined['s1w7']<4)&(combined['s1w7']>0))|
((combined['s1w8']<4)&(combined['s1w8']>0))|
((combined['s1w9']<4)&(combined['s1w9']>0))|
((combined['s1w10']<4)&(combined['s1w10']>0)))&
(((combined['s2w0']<4)&(combined['s2w0']>0))|
((combined['s2w1']<4)&(combined['s2w1']>0))|
((combined['s2w2']<4)&(combined['s2w2']>0))|
((combined['s2w3']<4)&(combined['s2w3']>0))|
((combined['s2w4']<4)&(combined['s2w4']>0))|
((combined['s2w5']<4)&(combined['s2w5']>0))|
((combined['s2w6']<4)&(combined['s2w6']>0)))&
(((combined['s3w0']<4)&(combined['s3w0']>0))|
((combined['s3w1']<4)&(combined['s3w1']>0))|
((combined['s3w2']<4)&(combined['s3w2']>0))|
((combined['s3w3']<4)&(combined['s3w3']>0))|
((combined['s3w4']<4)&(combined['s3w4']>0))|
((combined['s3w5']<4)&(combined['s3w5']>0))|
((combined['s3w6']<4)&(combined['s3w6']>0))|
((combined['s3w7']<4)&(combined['s3w7']>0)))),'B',np.where(((((combined['s1w0']<4)&(combined['s1w0']>=0))&
((combined['s1w1']<4)&(combined['s1w1']>=0))&
((combined['s1w2']<4)&(combined['s1w2']>=0))&
((combined['s1w3']<4)&(combined['s1w3']>=0))&
((combined['s1w4']<4)&(combined['s1w4']>=0))&
((combined['s1w5']<4)&(combined['s1w5']>=0))&
((combined['s1w6']<4)&(combined['s1w6']>=0))&
((combined['s1w7']<4)&(combined['s1w7']>=0))&
((combined['s1w8']<4)&(combined['s1w8']>=0))&
((combined['s1w9']<4)&(combined['s1w9']>=0))&
((combined['s1w10']<4)&(combined['s1w10']>0)))|
(((combined['s2w0']<4)&(combined['s2w0']>=0))&
((combined['s2w1']<4)&(combined['s2w1']>=0))&
((combined['s2w2']<4)&(combined['s2w2']>=0))&
((combined['s2w3']<4)&(combined['s2w3']>=0))&
((combined['s2w4']<4)&(combined['s2w4']>=0))&
((combined['s2w5']<4)&(combined['s2w5']>=0))&
((combined['s2w6']<4)&(combined['s2w6']>0)))|
(((combined['s3w0']<4)&(combined['s3w0']>=0))&
((combined['s3w1']<4)&(combined['s3w1']>=0))&
((combined['s3w2']<4)&(combined['s3w2']>=0))&
((combined['s3w3']<4)&(combined['s3w3']>=0))&
((combined['s3w4']<4)&(combined['s3w4']>=0))&
((combined['s3w5']<4)&(combined['s3w5']>=0))&
((combined['s3w6']<4)&(combined['s3w6']>=0))&
((combined['s3w7']<4)&(combined['s3w7']>0)))),'C',np.where((((combined['s1w0']<4)&(combined['s1w0']>0))|
((combined['s1w1']<4)&(combined['s1w1']>0))|
((combined['s1w2']<4)&(combined['s1w2']>0))|
((combined['s1w3']<4)&(combined['s1w3']>0))|
((combined['s1w4']<4)&(combined['s1w4']>0))|
((combined['s1w5']<4)&(combined['s1w5']>0))|
((combined['s1w6']<4)&(combined['s1w6']>0))|
((combined['s1w7']<4)&(combined['s1w7']>0))|
((combined['s1w8']<4)&(combined['s1w8']>0))|
((combined['s1w9']<4)&(combined['s1w9']>0))|
((combined['s1w10']<4)&(combined['s1w10']>0))|
((combined['s2w0']<4)&(combined['s2w0']>0))|
((combined['s2w1']<4)&(combined['s2w1']>0))|
((combined['s2w2']<4)&(combined['s2w2']>0))|
((combined['s2w3']<4)&(combined['s2w3']>0))|
((combined['s2w4']<4)&(combined['s2w4']>0))|
((combined['s2w5']<4)&(combined['s2w5']>0))|
((combined['s2w6']<4)&(combined['s2w6']>0))|
((combined['s3w0']<4)&(combined['s3w0']>0))|
((combined['s3w1']<4)&(combined['s3w1']>0))|
((combined['s3w2']<4)&(combined['s3w2']>0))|
((combined['s3w3']<4)&(combined['s3w3']>0))|
((combined['s3w4']<4)&(combined['s3w4']>0))|
((combined['s3w5']<4)&(combined['s3w5']>0))|
((combined['s3w6']<4)&(combined['s3w6']>0))|
((combined['s3w7']<4)&(combined['s3w7']>0))),'D','Other'))))

combined.to_csv("Rentrakalldata0808.csv",index=False,dtype={'ZIP_CODE':'object','ZIP4':'object'})

combined.columns
combined=combined[combined.Zip4.str.contains('"')==False]
combined=combined[combined.Zip!='0.0']
combined=combined[combined.Zip4!='0']
combined['Zip4']=combined['Zip4'].str[:4]
combined=combined[combined.Zip4!='0000']
combined=combined.drop_duplicates(['Zip','Zip4'])

test=combined[['s1w1','Segment']]
print(test.groupby('Segment').count())

combined3=combined[combined.Segment!='Other']
combined3=combined3[['Zip','Zip4']]
combined3=combined3.drop_duplicates(['Zip','Zip4'])
combined3.to_csv("JIM_Rentrak_0808.csv",index=False,dtype={'Zip':'object','Zip4':'object'})

combined2=combined
combined2=combined2[['Zip','Zip4','Segment','USNG_100']]
combined2.groupby('Segment').count()
combined2.to_csv("JIM_Rentrak_0808_withsegment.csv",index=False,dtype={'Zip':'object','Zip4':'object'})