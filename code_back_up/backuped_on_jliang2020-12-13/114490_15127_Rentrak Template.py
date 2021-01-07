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

w0=pd.read_csv("USNG_deciles_2015-12-28-2016-01-10-1854478.txt",sep=",")
w1=pd.read_csv("USNG_deciles_2016-01-11-2016-01-24-1854478.txt",sep=",")
w2=pd.read_csv("USNG_deciles_2016-01-25-2016-02-07-1854478.txt",sep=",")
w3=pd.read_csv("USNG_deciles_2016-02-08-2016-02-21-1854478.txt",sep=",")
w4=pd.read_csv("USNG_deciles_2016-02-22-2016-03-06-1854478.txt",sep=",")


w0.columns=['USNG_100','s1w0']
w1.columns=['USNG_100','s1w1']
w2.columns=['USNG_100','s1w2']
w3.columns=['USNG_100','s1w3']
w4.columns=['USNG_100','s1w4']


show1=pd.merge(w0,w1,how='outer',on='USNG_100')
show1=pd.merge(show1,w2,how='outer',on='USNG_100')
show1=pd.merge(show1,w3,how='outer',on='USNG_100')
show1=pd.merge(show1,w4,how='outer',on='USNG_100')


zip=pd.read_csv("/Users/jubaplus1/Desktop/Work/Rentrak/ZipNew0519.csv",dtype={'Zip':'object','Zip4':'object'})
zip.columns=['Zip','Zip4','USNG_100']

show1=pd.merge(show1,zip,on='USNG_100')

del w0
del w1

import gc
gc.collect()

###############################

print(show1.columns)

gc.collect()

combined=show1
combined=combined.fillna(0)

##combined['Segment']=np.where(,'A',np.where(,'B',np.where(,'C',np.where(,'D','Other'))))

combined['Segment']=np.where((((combined['s1w0']<4)&(combined['s1w0']>0))&
((combined['s1w1']<4)&(combined['s1w1']>0))&
((combined['s1w2']<4)&(combined['s1w2']>0))&
((combined['s1w3']<4)&(combined['s1w3']>0))&
((combined['s1w4']<4)&(combined['s1w4']>0))),'A',np.where((((combined['s1w0']<4)&(combined['s1w0']>0))|
((combined['s1w1']<4)&(combined['s1w1']>0))|
((combined['s1w2']<4)&(combined['s1w2']>0))|
((combined['s1w3']<4)&(combined['s1w3']>0))|
((combined['s1w4']<4)&(combined['s1w4']>0))),'B','Other'))

combined.to_csv("Rentrakalldata0808ovation.csv",index=False,dtype={'ZIP_CODE':'object','ZIP4':'object'})

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
combined3.to_csv("ovation_Rentrak_0808.csv",index=False,dtype={'Zip':'object','Zip4':'object'})

combined2=combined
combined2=combined2[['Zip','Zip4','Segment','USNG_100']]
combined2.groupby('Segment').count()
combined2.to_csv("ovation_Rentrak_0808_withsegment.csv",index=False,dtype={'Zip':'object','Zip4':'object'})