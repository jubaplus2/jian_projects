# -*- coding: utf-8 -*-
"""
Created on Sat Mar 12 16:15:36 2016

@author: Spencer
"""

import os
os.chdir("/Users/Spencer/Documents/Python Scripts/Rentrak 0304/BFV/BFV rentrak")
os.chdir("/Users/Spencer/Documents/Python Scripts/Rentrak 0304/BFV")
import pandas as pd
import numpy as np
import statsmodels.api as sm

w0=pd.read_csv("USNG_deciles_2015-09-07-2015-09-20-1856678.txt",sep=",")
w1=pd.read_csv("USNG_deciles_2015-09-21-2015-10-04-1856678.txt",sep=",")
w2=pd.read_csv("USNG_deciles_2015-10-05-2015-10-18-1856678.txt",sep=",")
w4=pd.read_csv("USNG_deciles_2015-11-02-2015-11-15-1856678.txt",sep=",")
w5=pd.read_csv("USNG_deciles_2015-11-16-2015-11-29-1856678.txt",sep=",")
w6=pd.read_csv("USNG_deciles_2015-11-30-2015-12-13-1856678.txt",sep=",")
w7=pd.read_csv("USNG_deciles_2015-12-14-2015-12-27-1856678.txt",sep=",")
w8=pd.read_csv("USNG_deciles_2015-12-28-2016-01-10-1856678.txt",sep=",")
w9=pd.read_csv("USNG_deciles_2016-01-11-2016-01-24-1856678.txt",sep=",")
w10=pd.read_csv("USNG_deciles_2016-01-25-2016-02-07-1856678.txt",sep=",")
w11=pd.read_csv("USNG_deciles_2016-02-08-2016-02-21-1856678_reissue.txt",sep=",")

w0.columns=['USNG_100','s1w0']
w1.columns=['USNG_100','s1w1']
w2.columns=['USNG_100','s1w2']
w4.columns=['USNG_100','s1w4']
w5.columns=['USNG_100','s1w5']
w6.columns=['USNG_100','s1w6']
w7.columns=['USNG_100','s1w7']
w8.columns=['USNG_100','s1w8']
w9.columns=['USNG_100','s1w9']
w10.columns=['USNG_100','s1w10']
w11.columns=['USNG_100','s1w11']


show6=pd.merge(w0,w1,how='outer',on='USNG_100')
show6=pd.merge(show6,w2,how='outer',on='USNG_100')
show6=pd.merge(show6,w4,how='outer',on='USNG_100')
show6=pd.merge(show6,w5,how='outer',on='USNG_100')
show6=pd.merge(show6,w6,how='outer',on='USNG_100')
show6=pd.merge(show6,w7,how='outer',on='USNG_100')
show6=pd.merge(show6,w8,how='outer',on='USNG_100')
show6=pd.merge(show6,w9,how='outer',on='USNG_100')
show6=pd.merge(show6,w10,how='outer',on='USNG_100')
show6=pd.merge(show6,w11,how='outer',on='USNG_100')
show1=show6
zip=pd.read_csv("/Users/Spencer/Documents/Python Scripts/Rentrak 0304/zip4_lat_lon_usng.csv",dtype={'ZIP':'object'})
zip=zip[['ZIP', 'ZIP4', 'USNG']]
zip.columns=['Zip','Zip4','USNG_100']

show1=pd.merge(show1,zip,on='USNG_100')

del w0
del w1
del w2
del w4
del w5
del w6
del w7
del w8
del w9
del w10
del w11
del show6

import gc
gc.collect()

show1.to_csv('1856678.csv',index='False',dtype={'Zip':'object','Zip4':'object'})

w7=pd.read_csv("USNG_deciles_2015-12-14-2015-12-27-2021069.txt",sep=",")
w8=pd.read_csv("USNG_deciles_2015-12-28-2016-01-10-2021069.txt",sep=",")
w9=pd.read_csv("USNG_deciles_2016-01-11-2016-01-24-2021069.txt",sep=",")
w10=pd.read_csv("USNG_deciles_2016-01-25-2016-02-07-2021069.txt",sep=",")
w11=pd.read_csv("USNG_deciles_2016-02-08-2016-02-21-2021069_reissue.txt",sep=",")
w12=pd.read_csv("USNG_deciles_2016-02-22-2016-03-06-2021069_reissue.txt",sep=",")
w13=pd.read_csv("USNG_deciles_2016-03-07-2016-03-20-2021069.txt",sep=",")

w7.columns=['USNG_100','s2w7']
w8.columns=['USNG_100','s2w8']
w9.columns=['USNG_100','s2w9']
w10.columns=['USNG_100','s2w10']
w11.columns=['USNG_100','s2w11']
w12.columns=['USNG_100','s2w12']
w13.columns=['USNG_100','s2w13']

show2=pd.merge(w7,w8,how='outer',on='USNG_100')
show2=pd.merge(show2,w9,how='outer',on='USNG_100')
show2=pd.merge(show2,w10,how='outer',on='USNG_100')
show2=pd.merge(show2,w11,how='outer',on='USNG_100')
show2=pd.merge(show2,w12,how='outer',on='USNG_100')
show2=pd.merge(show2,w13,how='outer',on='USNG_100')

show2=pd.merge(show2,zip,on='USNG_100')

del w7
del w8
del w9
del w10
del w11
del w12
del w13

import gc
gc.collect()

show2.to_csv('2021069.csv',index='False',dtype={'Zip':'object','Zip4':'object'})
show2.describe()


w4=pd.read_csv("USNG_deciles_2015-11-02-2015-11-15-1887065.txt",sep=",")
w5=pd.read_csv("USNG_deciles_2015-11-16-2015-11-29-1887065.txt",sep=",")
w6=pd.read_csv("USNG_deciles_2015-11-30-2015-12-13-1887065.txt",sep=",")
w7=pd.read_csv("USNG_deciles_2015-12-14-2015-12-27-1887065.txt",sep=",")
w8=pd.read_csv("USNG_deciles_2015-12-28-2016-01-10-1887065.txt",sep=",")
w9=pd.read_csv("USNG_deciles_2016-01-11-2016-01-24-1887065.txt",sep=",")
w10=pd.read_csv("USNG_deciles_2016-01-25-2016-02-07-1887065.txt",sep=",")
w11=pd.read_csv("USNG_deciles_2016-02-08-2016-02-21-1887065_reissue.txt",sep=",")
w12=pd.read_csv("USNG_deciles_2016-02-22-2016-03-06-1887065_reissue.txt",sep=",")
w13=pd.read_csv("USNG_deciles_2016-03-07-2016-03-20-1887065.txt",sep=",")

w4.columns=['USNG_100','s3w4']
w5.columns=['USNG_100','s3w5']
w6.columns=['USNG_100','s3w6']
w7.columns=['USNG_100','s3w7']
w8.columns=['USNG_100','s3w8']
w9.columns=['USNG_100','s3w9']
w10.columns=['USNG_100','s3w10']
w11.columns=['USNG_100','s3w11']
w12.columns=['USNG_100','s3w12']
w13.columns=['USNG_100','s3w13']


show3=pd.merge(w4,w5,how='outer',on='USNG_100')
show3=pd.merge(show3,w6,how='outer',on='USNG_100')
show3=pd.merge(show3,w7,how='outer',on='USNG_100')
show3=pd.merge(show3,w8,how='outer',on='USNG_100')
show3=pd.merge(show3,w9,how='outer',on='USNG_100')
show3=pd.merge(show3,w10,how='outer',on='USNG_100')
show3=pd.merge(show3,w11,how='outer',on='USNG_100')
show3=pd.merge(show3,w12,how='outer',on='USNG_100')
show3=pd.merge(show3,w13,how='outer',on='USNG_100')

show3=pd.merge(show3,zip,on='USNG_100')
show3.columns

del w4
del w5
del w6
del w7
del w8
del w9
del w10
del w11
del w12
del w13
import gc
gc.collect()

show3.to_csv('1887065.csv',index='False',dtype={'Zip':'object','Zip4':'object'})
show3.describe()

w6=pd.read_csv("USNG_deciles_2015-11-30-2015-12-13-2172792.txt",sep=",")
w7=pd.read_csv("USNG_deciles_2015-12-14-2015-12-27-2172792.txt",sep=",")
w8=pd.read_csv("USNG_deciles_2015-12-28-2016-01-10-2172792.txt",sep=",")
w9=pd.read_csv("USNG_deciles_2016-01-11-2016-01-24-2172792.txt",sep=",")
w10=pd.read_csv("USNG_deciles_2015-01-25-2015-02-07-2172792_reissue.txt",sep=",")
w11=pd.read_csv("USNG_deciles_2016-02-08-2016-02-21-2172792_reissue.txt",sep=",")
w12=pd.read_csv("USNG_deciles_2016-02-22-2016-03-06-2172792_reissue.txt",sep=",")

w6.columns=['USNG_100','s4w6']
w7.columns=['USNG_100','s4w7']
w8.columns=['USNG_100','s4w8']
w9.columns=['USNG_100','s4w9']
w10.columns=['USNG_100','s4w10']
w11.columns=['USNG_100','s4w11']
w12.columns=['USNG_100','s4w12']

show4=pd.merge(w6,w7,how='outer',on='USNG_100')
show4=pd.merge(show4,w8,how='outer',on='USNG_100')
show4=pd.merge(show4,w9,how='outer',on='USNG_100')
show4=pd.merge(show4,w10,how='outer',on='USNG_100')
show4=pd.merge(show4,w11,how='outer',on='USNG_100')
show4=pd.merge(show4,w12,how='outer',on='USNG_100')

show4=pd.merge(show4,zip,on='USNG_100')
show4.columns

del w6
del w7
del w8
del w9
del w10
del w11
del w12
import gc
gc.collect()

show4.to_csv('2172792.csv',index='False',dtype={'Zip':'object','Zip4':'object'})
show4.describe()

show5=pd.read_csv("Show2091417W11.csv",dtype={'Zip':'object','Zip4':'object'})
show5.dtypes
show5.describe()

show5.columns = ['ShowID','Network_no', 'ShowName','USNG_100', 'Zip','Zip4',
's5w0','s5w1','s5w2','s5w3','s5w4','s5w5','s5w6','s5w7','s5w8','s5w9','s5w10','s5w11']

show5=show5[['USNG_100', 'Zip','Zip4',
's5w0','s5w1','s5w2','s5w3','s5w4','s5w5']]

w11=pd.read_csv("USNG_deciles_2016-02-08-2016-02-21-2091417_reissue.txt",sep=",")
w12=pd.read_csv("USNG_deciles_2016-02-22-2016-03-06-2091417_reissue.txt",sep=",")
w13=pd.read_csv("USNG_deciles_2016-03-07-2016-03-20-2091417.txt",sep=",")

w11.columns=['USNG_100','s5w11']
w12.columns=['USNG_100','s5w12']
w13.columns=['USNG_100','s5w13']

w11=pd.merge(w11,w12,on='USNG_100')
w11=pd.merge(w11,w13,on='USNG_100')
w11=pd.merge(w11,zip,on='USNG_100')

show5=pd.merge(show5,w11,how='outer',on=['USNG_100', 'Zip','Zip4'])

del w11
del w12
del w13
gc.collect()

show6=pd.read_csv("Show2294631W11.csv",dtype={'Zip':'object','Zip4':'object'})
show6.dtypes
show6.describe()

show6.columns = ['ShowID','Network_no', 'ShowName','USNG_100', 'Zip','Zip4',
's6w0','s6w1','s6w2','s6w3','s6w4','s6w5','s6w6','s6w7','s6w8','s6w9','s6w10','s6w11']

show6=show6[['USNG_100', 'Zip','Zip4',
's6w0','s6w1','s6w2','s6w3','s6w4','s6w5']]


w11=pd.read_csv("USNG_deciles_2016-02-08-2016-02-21-2294631_reissue.txt",sep=",")
w12=pd.read_csv("USNG_deciles_2016-02-22-2016-03-06-2294631_reissue.txt",sep=",")
w13=pd.read_csv("USNG_deciles_2016-03-07-2016-03-20-2294631.txt",sep=",")

w11.columns=['USNG_100','s6w11']
w12.columns=['USNG_100','s6w12']
w13.columns=['USNG_100','s6w13']

w11=pd.merge(w11,w12,on='USNG_100')
w11=pd.merge(w11,w13,on='USNG_100')
w11=pd.merge(w11,zip,on='USNG_100')

show6=pd.merge(show6,w11,how='outer',on=['USNG_100', 'Zip','Zip4'])

combined=pd.merge(show1,show2,how='outer',on=['USNG_100', 'Zip','Zip4'])
combined=pd.merge(combined,show3,how='outer',on=['USNG_100', 'Zip','Zip4'])
combined=pd.merge(combined,show4,how='outer',on=['USNG_100', 'Zip','Zip4'])
combined=pd.merge(combined,show5,how='outer',on=['USNG_100', 'Zip','Zip4'])
combined=pd.merge(combined,show6,how='outer',on=['USNG_100', 'Zip','Zip4'])

combined=combined.fillna(0)
combined.head(50)

combined.columns
combined=pd.read_csv("BFVRentrakInternal.csv",dtype={'Zip':'object','Zip4':'object'})

##combined['Segment']=np.where(,'A',np.where(,'B',np.where(,'C',np.where(,'D','Other'))))


combined['Segment']=np.where(((combined['s1w1']<4)&(combined['s1w1']>=0))&
((combined['s1w2']<4)&(combined['s1w2']>=0))&
((combined['s1w4']<4)&(combined['s1w4']>=0))&
((combined['s1w5']<4)&(combined['s1w5']>=0))&
((combined['s1w6']<4)&(combined['s1w6']>=0))&
((combined['s1w7']<4)&(combined['s1w7']>=0))&
((combined['s1w8']<4)&(combined['s1w8']>=0))&
((combined['s1w9']<4)&(combined['s1w9']>=0))&
((combined['s1w10']<4)&(combined['s1w10']>=0))&
((combined['s1w11']<4)&(combined['s1w11']>=0))&
((combined['s2w7']<4)&(combined['s2w7']>=0))&
((combined['s2w8']<4)&(combined['s2w8']>=0))&
((combined['s2w9']<4)&(combined['s2w9']>=0))&
((combined['s2w10']<4)&(combined['s2w10']>=0))&
((combined['s2w11']<4)&(combined['s2w11']>=0))&
((combined['s2w12']<4)&(combined['s2w12']>=0))&
((combined['s2w13']<4)&(combined['s2w13']>=0))&
((combined['s3w4']<4)&(combined['s3w4']>=0))&
((combined['s3w5']<4)&(combined['s3w5']>=0))&
((combined['s3w6']<4)&(combined['s3w6']>=0))&
((combined['s3w7']<4)&(combined['s3w7']>=0))&
((combined['s3w8']<4)&(combined['s3w8']>=0))&
((combined['s3w9']<4)&(combined['s3w9']>=0))&
((combined['s3w10']<4)&(combined['s3w10']>=0))&
((combined['s3w11']<4)&(combined['s3w11']>=0))&
((combined['s3w12']<4)&(combined['s3w12']>=0))&
((combined['s3w13']<4)&(combined['s3w13']>=0))&
((combined['s4w6']<4)&(combined['s4w6']>=0))&
((combined['s4w7']<4)&(combined['s4w7']>=0))&
((combined['s4w8']<4)&(combined['s4w8']>=0))&
((combined['s4w9']<4)&(combined['s4w9']>=0))&
((combined['s4w10']<4)&(combined['s4w10']>=0))&
((combined['s4w11']<4)&(combined['s4w11']>=0))&
((combined['s4w12']<4)&(combined['s4w12']>=0))&
((combined['s5w0']<4)&(combined['s5w0']>=0))&
((combined['s5w1']<4)&(combined['s5w1']>=0))&
((combined['s5w2']<4)&(combined['s5w2']>=0))&
((combined['s5w3']<4)&(combined['s5w3']>=0))&
((combined['s5w4']<4)&(combined['s5w4']>=0))&
((combined['s5w5']<4)&(combined['s5w5']>=0))&
((combined['s5w11']<4)&(combined['s5w11']>=0))&
((combined['s5w12']<4)&(combined['s5w12']>=0))&
((combined['s5w13']<4)&(combined['s5w13']>=0))&
((combined['s6w0']<4)&(combined['s6w0']>=0))&
((combined['s6w1']<4)&(combined['s6w1']>=0))&
((combined['s6w2']<4)&(combined['s6w2']>=0))&
((combined['s6w3']<4)&(combined['s6w3']>=0))&
((combined['s6w4']<4)&(combined['s6w4']>=0))&
((combined['s6w5']<4)&(combined['s6w5']>=0))&
((combined['s6w11']<4)&(combined['s6w11']>=0))&
((combined['s6w12']<4)&(combined['s6w12']>=0))&
((combined['s6w13']<4)&(combined['s6w13']>=0)),'A',np.where((((combined['s1w1']<4)&(combined['s1w1']>0))|
((combined['s1w1']<4)&(combined['s1w2']>0))|
((combined['s1w1']<4)&(combined['s1w4']>0))|
((combined['s1w1']<4)&(combined['s1w5']>0))|
((combined['s1w1']<4)&(combined['s1w6']>0))|
((combined['s1w1']<4)&(combined['s1w7']>0))|
((combined['s1w1']<4)&(combined['s1w8']>0))|
((combined['s1w1']<4)&(combined['s1w9']>0))|
((combined['s1w1']<4)&(combined['s1w10']>0))|
((combined['s1w1']<4)&(combined['s1w11']>0)))&
(((combined['s1w1']<4)&(combined['s2w7']>0))|
((combined['s1w1']<4)&(combined['s2w8']>0))|
((combined['s1w1']<4)&(combined['s2w9']>0))|
((combined['s1w1']<4)&(combined['s2w10']>0))|
((combined['s1w1']<4)&(combined['s2w11']>0))|
((combined['s1w1']<4)&(combined['s2w12']>0))|
((combined['s1w1']<4)&(combined['s2w13']>0)))&
(((combined['s1w1']<4)&(combined['s3w4']>0))|
((combined['s1w1']<4)&(combined['s3w5']>0))|
((combined['s1w1']<4)&(combined['s3w6']>0))|
((combined['s1w1']<4)&(combined['s3w7']>0))|
((combined['s1w1']<4)&(combined['s3w8']>0))|
((combined['s1w1']<4)&(combined['s3w9']>0))|
((combined['s1w1']<4)&(combined['s3w10']>0))|
((combined['s1w1']<4)&(combined['s3w11']>0))|
((combined['s1w1']<4)&(combined['s3w12']>0))|
((combined['s1w1']<4)&(combined['s3w13']>0)))&
(((combined['s1w1']<4)&(combined['s4w6']>0))|
((combined['s1w1']<4)&(combined['s4w7']>0))|
((combined['s1w1']<4)&(combined['s4w8']>0))|
((combined['s1w1']<4)&(combined['s4w9']>0))|
((combined['s1w1']<4)&(combined['s4w10']>0))|
((combined['s1w1']<4)&(combined['s4w11']>0))|
((combined['s1w1']<4)&(combined['s4w12']>0)))&
(((combined['s1w1']<4)&(combined['s5w0']>0))|
((combined['s1w1']<4)&(combined['s5w1']>0))|
((combined['s1w1']<4)&(combined['s5w2']>0))|
((combined['s1w1']<4)&(combined['s5w3']>0))|
((combined['s1w1']<4)&(combined['s5w4']>0))|
((combined['s1w1']<4)&(combined['s5w5']>0))|
((combined['s1w1']<4)&(combined['s5w11']>0))|
((combined['s1w1']<4)&(combined['s5w12']>0))|
((combined['s1w1']<4)&(combined['s5w13']>0)))&
(((combined['s1w1']<4)&(combined['s6w0']>0))|
((combined['s1w1']<4)&(combined['s6w1']>0))|
((combined['s1w1']<4)&(combined['s6w2']>0))|
((combined['s1w1']<4)&(combined['s6w3']>0))|
((combined['s1w1']<4)&(combined['s6w4']>0))|
((combined['s1w1']<4)&(combined['s6w5']>0))|
((combined['s1w1']<4)&(combined['s6w11']>0))|
((combined['s1w1']<4)&(combined['s6w12']>0))|
((combined['s1w1']<4)&(combined['s6w13']>0))),'B',np.where((((combined['s1w1']<4)&(combined['s1w1']>=0))&
((combined['s1w1']<4)&(combined['s1w2']>=0))&
((combined['s1w1']<4)&(combined['s1w4']>=0))&
((combined['s1w1']<4)&(combined['s1w5']>=0))&
((combined['s1w1']<4)&(combined['s1w6']>=0))&
((combined['s1w1']<4)&(combined['s1w7']>=0))&
((combined['s1w1']<4)&(combined['s1w8']>=0))&
((combined['s1w1']<4)&(combined['s1w9']>=0))&
((combined['s1w1']<4)&(combined['s1w10']>=0))&
((combined['s1w1']<4)&(combined['s1w11']>=0)))|
(((combined['s1w1']<4)&(combined['s2w7']>=0))&
((combined['s1w1']<4)&(combined['s2w8']>=0))&
((combined['s1w1']<4)&(combined['s2w9']>=0))&
((combined['s1w1']<4)&(combined['s2w10']>=0))&
((combined['s1w1']<4)&(combined['s2w11']>=0))&
((combined['s1w1']<4)&(combined['s2w12']>=0))&
((combined['s1w1']<4)&(combined['s2w13']>=0)))|
(((combined['s1w1']<4)&(combined['s3w4']>=0))&
((combined['s1w1']<4)&(combined['s3w5']>=0))&
((combined['s1w1']<4)&(combined['s3w6']>=0))&
((combined['s1w1']<4)&(combined['s3w7']>=0))&
((combined['s1w1']<4)&(combined['s3w8']>=0))&
((combined['s1w1']<4)&(combined['s3w9']>=0))&
((combined['s1w1']<4)&(combined['s3w10']>=0))&
((combined['s1w1']<4)&(combined['s3w11']>=0))&
((combined['s1w1']<4)&(combined['s3w12']>=0))&
((combined['s1w1']<4)&(combined['s3w13']>=0)))|
(((combined['s1w1']<4)&(combined['s4w6']>=0))&
((combined['s1w1']<4)&(combined['s4w7']>=0))&
((combined['s1w1']<4)&(combined['s4w8']>=0))&
((combined['s1w1']<4)&(combined['s4w9']>=0))&
((combined['s1w1']<4)&(combined['s4w10']>=0))&
((combined['s1w1']<4)&(combined['s4w11']>=0))&
((combined['s1w1']<4)&(combined['s4w12']>=0)))|
(((combined['s1w1']<4)&(combined['s5w0']>=0))&
((combined['s1w1']<4)&(combined['s5w1']>=0))&
((combined['s1w1']<4)&(combined['s5w2']>=0))&
((combined['s1w1']<4)&(combined['s5w3']>=0))&
((combined['s1w1']<4)&(combined['s5w4']>=0))&
((combined['s1w1']<4)&(combined['s5w5']>=0))&
((combined['s1w1']<4)&(combined['s5w11']>=0))&
((combined['s1w1']<4)&(combined['s5w12']>=0))&
((combined['s1w1']<4)&(combined['s5w13']>=0)))|
(((combined['s1w1']<4)&(combined['s6w0']>=0))&
((combined['s1w1']<4)&(combined['s6w1']>=0))&
((combined['s1w1']<4)&(combined['s6w2']>=0))&
((combined['s1w1']<4)&(combined['s6w3']>=0))&
((combined['s1w1']<4)&(combined['s6w4']>=0))&
((combined['s1w1']<4)&(combined['s6w5']>=0))&
((combined['s1w1']<4)&(combined['s6w11']>=0))&
((combined['s1w1']<4)&(combined['s6w12']>=0))&
((combined['s1w1']<4)&(combined['s6w13']>=0))),'C',np.where(((combined['s1w1']<4)&(combined['s1w1']>0))|
((combined['s1w2']<4)&(combined['s1w2']>0))|
((combined['s1w4']<4)&(combined['s1w4']>0))|
((combined['s1w5']<4)&(combined['s1w5']>0))|
((combined['s1w6']<4)&(combined['s1w6']>0))|
((combined['s1w7']<4)&(combined['s1w7']>0))|
((combined['s1w8']<4)&(combined['s1w8']>0))|
((combined['s1w9']<4)&(combined['s1w9']>0))|
((combined['s1w10']<4)&(combined['s1w10']>0))|
((combined['s1w11']<4)&(combined['s1w11']>0))|
((combined['s2w7']<4)&(combined['s2w7']>0))|
((combined['s2w8']<4)&(combined['s2w8']>0))|
((combined['s2w9']<4)&(combined['s2w9']>0))|
((combined['s2w10']<4)&(combined['s2w10']>0))|
((combined['s2w11']<4)&(combined['s2w11']>0))|
((combined['s2w12']<4)&(combined['s2w12']>0))|
((combined['s2w13']<4)&(combined['s2w13']>0))|
((combined['s3w4']<4)&(combined['s3w4']>0))|
((combined['s3w5']<4)&(combined['s3w5']>0))|
((combined['s3w6']<4)&(combined['s3w6']>0))|
((combined['s3w7']<4)&(combined['s3w7']>0))|
((combined['s3w8']<4)&(combined['s3w8']>0))|
((combined['s3w9']<4)&(combined['s3w9']>0))|
((combined['s3w10']<4)&(combined['s3w10']>0))|
((combined['s3w11']<4)&(combined['s3w11']>0))|
((combined['s3w12']<4)&(combined['s3w12']>0))|
((combined['s3w13']<4)&(combined['s3w13']>0))|
((combined['s4w6']<4)&(combined['s4w6']>0))|
((combined['s4w7']<4)&(combined['s4w7']>0))|
((combined['s4w8']<4)&(combined['s4w8']>0))|
((combined['s4w9']<4)&(combined['s4w9']>0))|
((combined['s4w10']<4)&(combined['s4w10']>0))|
((combined['s4w11']<4)&(combined['s4w11']>0))|
((combined['s4w12']<4)&(combined['s4w12']>0))|
((combined['s5w0']<4)&(combined['s5w0']>0))|
((combined['s5w1']<4)&(combined['s5w1']>0))|
((combined['s5w2']<4)&(combined['s5w2']>0))|
((combined['s5w3']<4)&(combined['s5w3']>0))|
((combined['s5w4']<4)&(combined['s5w4']>0))|
((combined['s5w5']<4)&(combined['s5w5']>0))|
((combined['s5w11']<4)&(combined['s5w11']>0))|
((combined['s5w12']<4)&(combined['s5w12']>0))|
((combined['s5w13']<4)&(combined['s5w13']>0))|
((combined['s6w0']<4)&(combined['s6w0']>0))|
((combined['s6w1']<4)&(combined['s6w1']>0))|
((combined['s6w2']<4)&(combined['s6w2']>0))|
((combined['s6w3']<4)&(combined['s6w3']>0))|
((combined['s6w4']<4)&(combined['s6w4']>0))|
((combined['s6w5']<4)&(combined['s6w5']>0))|
((combined['s6w11']<4)&(combined['s6w11']>0))|
((combined['s6w12']<4)&(combined['s6w12']>0))|
((combined['s6w13']<4)&(combined['s6w13']>0)),'D','Other'))))

test=combined[['s1w1','Segment']]
test.groupby('Segment').count()



combined['Segment2']=np.where(((combined['s1w1']<2)&(combined['s1w1']>=0))&
((combined['s1w2']<2)&(combined['s1w2']>=0))&
((combined['s1w4']<2)&(combined['s1w4']>=0))&
((combined['s1w5']<2)&(combined['s1w5']>=0))&
((combined['s1w6']<2)&(combined['s1w6']>=0))&
((combined['s1w7']<2)&(combined['s1w7']>=0))&
((combined['s1w8']<2)&(combined['s1w8']>=0))&
((combined['s1w9']<2)&(combined['s1w9']>=0))&
((combined['s1w10']<2)&(combined['s1w10']>=0))&
((combined['s1w11']<2)&(combined['s1w11']>=0))&
((combined['s2w7']<2)&(combined['s2w7']>=0))&
((combined['s2w8']<2)&(combined['s2w8']>=0))&
((combined['s2w9']<2)&(combined['s2w9']>=0))&
((combined['s2w10']<2)&(combined['s2w10']>=0))&
((combined['s2w11']<2)&(combined['s2w11']>=0))&
((combined['s2w12']<2)&(combined['s2w12']>=0))&
((combined['s2w13']<2)&(combined['s2w13']>=0))&
((combined['s3w4']<2)&(combined['s3w4']>=0))&
((combined['s3w5']<2)&(combined['s3w5']>=0))&
((combined['s3w6']<2)&(combined['s3w6']>=0))&
((combined['s3w7']<2)&(combined['s3w7']>=0))&
((combined['s3w8']<2)&(combined['s3w8']>=0))&
((combined['s3w9']<2)&(combined['s3w9']>=0))&
((combined['s3w10']<2)&(combined['s3w10']>=0))&
((combined['s3w11']<2)&(combined['s3w11']>=0))&
((combined['s3w12']<2)&(combined['s3w12']>=0))&
((combined['s3w13']<2)&(combined['s3w13']>=0))&
((combined['s4w6']<2)&(combined['s4w6']>=0))&
((combined['s4w7']<2)&(combined['s4w7']>=0))&
((combined['s4w8']<2)&(combined['s4w8']>=0))&
((combined['s4w9']<2)&(combined['s4w9']>=0))&
((combined['s4w10']<2)&(combined['s4w10']>=0))&
((combined['s4w11']<2)&(combined['s4w11']>=0))&
((combined['s4w12']<2)&(combined['s4w12']>=0))&
((combined['s5w0']<2)&(combined['s5w0']>=0))&
((combined['s5w1']<2)&(combined['s5w1']>=0))&
((combined['s5w2']<2)&(combined['s5w2']>=0))&
((combined['s5w3']<2)&(combined['s5w3']>=0))&
((combined['s5w4']<2)&(combined['s5w4']>=0))&
((combined['s5w5']<2)&(combined['s5w5']>=0))&
((combined['s5w11']<2)&(combined['s5w11']>=0))&
((combined['s5w12']<2)&(combined['s5w12']>=0))&
((combined['s5w13']<2)&(combined['s5w13']>=0))&
((combined['s6w0']<2)&(combined['s6w0']>=0))&
((combined['s6w1']<2)&(combined['s6w1']>=0))&
((combined['s6w2']<2)&(combined['s6w2']>=0))&
((combined['s6w3']<2)&(combined['s6w3']>=0))&
((combined['s6w4']<2)&(combined['s6w4']>=0))&
((combined['s6w5']<2)&(combined['s6w5']>=0))&
((combined['s6w11']<2)&(combined['s6w11']>=0))&
((combined['s6w12']<2)&(combined['s6w12']>=0))&
((combined['s6w13']<2)&(combined['s6w13']>=0)),'A','Other')
test=combined[['s1w1','Segment2']]
test.groupby('Segment2').count()






combined.to_csv("BFVRentrak0316.csv",index=False,dtype={'Zip':'object','Zip4':'object'})
combined=pd.read_csv("BFVRentrak0316.csv",dtype={'Zip':'object','Zip4':'object'})

combined=combined[combined.Zip4.str.contains('"')==False]
combined=combined[combined.Zip!='0']
combined=combined[combined.Zip!='0.0']
combined=combined[combined.Zip4!='0']
combined=combined[combined.Zip4!='0000']
combined['Zip4']=combined['Zip4'].str[:4]
combined=combined.drop_duplicates(['Zip','Zip4'])

usng=combined[['USNG_100','Zip']]
usng2=usng.groupby('USNG_100').count()

usng2.describe()

usng2=usng2[usng2.Zip<=5]
usng2.head()

usng2.reset_index(inplace=True)

usng2.columns=['USNG_100','count']

combined=pd.merge(combined,usng2,on='USNG_100')
combined.columns

combined3=combined[combined.Segment!='Other']
combined3=combined3[['Zip','Zip4']]
combined3=combined3[combined3.Zip4!='0000']
combined3=combined3.drop_duplicates(['Zip','Zip4'])
combined3.to_csv("BFV_Rentrak_0316.csv",index=False,dtype={'Zip':'object','Zip4':'object'})


combined2=combined2[combined2.Segment!='Other']
combined2=combined2[['Zip','Zip4','Segment']]
combined2.groupby('Segment').count()
combined2=combined2[combined2.Zip4!='0000']
combined2.to_csv("BFV_Rentrak_0313_withsegment.csv",index=False,dtype={'Zip':'object','Zip4':'object'})


###code on Mar 22, Rentrak Demo
combined2=pd.read_csv("BFV_Rentrak_0313_withsegment.csv",dtype={'Zip':'object','Zip4':'object'})
combined2.head()

combined2['Segment2']=np.where(combined2['Segment2']=='A','A','BCD')
combined2.groupby('Segment2').count()

DemoA2=pd.read_csv("/Users/Spencer/Documents/Python Scripts/Rentrak 0304/DemoA2.csv",dtype={'ZIP_CODE':'object','ZIP4':'object'})
DemoB2=pd.read_csv("/Users/Spencer/Documents/Python Scripts/Rentrak 0304/DemoB2.csv",dtype={'ZIP_CODE':'object','ZIP4':'object'})
DemoC2=pd.read_csv("/Users/Spencer/Documents/Python Scripts/Rentrak 0304/DemoC2.csv",dtype={'ZIP_CODE':'object','ZIP4':'object'})

DemoA2.columns
DemoB2.columns
DemoC2.columns

DemoA2['ZIP_CODE']=DemoA2['ZIP_CODE'].str.replace('\""','')
DemoA2['ZIP_CODE']=DemoA2['ZIP_CODE'].str.replace('\\','')
DemoA2['ZIP4']=DemoA2['ZIP4'].str.replace('\""','')
DemoA2['ZIP4']=DemoA2['ZIP4'].str.replace('\\','')
DemoA2.head()

DemoB2['ZIP_CODE']=DemoB2['ZIP_CODE'].str.replace('\""','')
DemoB2['ZIP_CODE']=DemoB2['ZIP_CODE'].str.replace('\\','')
DemoB2['ZIP4']=DemoB2['ZIP4'].str.replace('\""','')
DemoB2['ZIP4']=DemoB2['ZIP4'].str.replace('\\','')
DemoB2.head()

DemoC2['ZIP_CODE']=DemoC2['ZIP_CODE'].str.replace('\""','')
DemoC2['ZIP_CODE']=DemoC2['ZIP_CODE'].str.replace('\\','')
DemoC2['ZIP4']=DemoC2['ZIP4'].str.replace('\""','')
DemoC2['ZIP4']=DemoC2['ZIP4'].str.replace('\\','')
DemoC2.head()

del DemoA2['CRRTYPE']
del DemoB2['CRRTYPE']
del DemoB2['POP15']
del DemoB2['HH15']
del DemoC2['HH15']
del DemoC2['CRRTYPE']

Demo=pd.merge(DemoA2,DemoB2,on=['ZIP_CODE','ZIP4'])
Demo=pd.merge(Demo,DemoC2,on=['ZIP_CODE','ZIP4'])
del DemoA2
del DemoB2
del DemoC2
gc.collect()
Demo.to_csv("DemoALL.csv",index=False,dtype={'ZIP_CODE':'object','ZIP4':'object'})

combined2.columns=['ZIP_CODE','ZIP4','Segment']
combined2=combined2.drop_duplicates(['ZIP_CODE','ZIP4'])

combined2=pd.merge(combined2,DemoA2,on=['ZIP_CODE','ZIP4'])
combined2=pd.merge(combined2,DemoB2,on=['ZIP_CODE','ZIP4'])
combined2=pd.merge(combined2,DemoC2,on=['ZIP_CODE','ZIP4'])
combined2.to_csv("BFV_Rentrak_0322_withdemo.csv",index=False,dtype={'ZIP_CODE':'object','ZIP4':'object'})

del DemoA2
del DemoB2
del DemoC2
import gc
gc.collect()


combinedsum=combined2.groupby('Segment').mean()
combinedsum.to_csv("sum.csv")
from scipy.stats import ttest_ind

cat1 = combined2[combined2['Segment']=='A']
cat2 = combined2[combined2['Segment']=='BCD']
combined2.columns

ttest_ind(cat1['PCTHH45_54'], cat2['PCTHH45_54'])


file1=DemoB2[['PCTASPOP','PCTORPOP','PCTHISPOP','POP15','ZIP_CODE','ZIP4']]
file1=file1[(file1['POP15']>0)]
file1=file1[(file1['PCTASPOP']>=8.4)|(file1['PCTHISPOP']>=23.9)|(file1['PCTORPOP']>=9.3)]

file2=DemoA2[['MEDVALOCC','MEDRENT','PCTHH200P','POP15','ZIP_CODE','ZIP4']]
file2=file2[(file2['POP15']>0)]
file2=file2[(file2['MEDVALOCC']>=311089)|(file2['MEDRENT']>=1049)|(file2['PCTHH200P']>=8.9)]

file1=file1[['ZIP_CODE','ZIP4']]
file2=file2[['ZIP_CODE','ZIP4']]

file1=file1.append(file2,ignore_index=True)

file=file1.drop_duplicates()
file=pd.merge(file,combined2,on=['ZIP_CODE','ZIP4'],how='outer')
file=file.fillna('Test')
file.groupby('Segment').count()
file=file.drop_duplicates(['ZIP_CODE','ZIP4'])
file.to_csv("BFV_Rentrak_0322_withtest.csv",index=False,dtype={'ZIP_CODE':'object','ZIP4':'object'})

del combined
import gc
gc.collect()


combined2['Segment3']=np.where(((combined2['s1w1']<5)&(combined2['s1w1']>=0))&
((combined2['s1w2']<5)&(combined2['s1w2']>=0))&
((combined2['s1w4']<5)&(combined2['s1w4']>=0))&
((combined2['s1w5']<5)&(combined2['s1w5']>=0))&
((combined2['s1w6']<5)&(combined2['s1w6']>=0))&
((combined2['s1w7']<5)&(combined2['s1w7']>=0))&
((combined2['s1w8']<5)&(combined2['s1w8']>=0))&
((combined2['s1w9']<5)&(combined2['s1w9']>=0))&
((combined2['s1w10']<5)&(combined2['s1w10']>=0))&
((combined2['s1w11']<5)&(combined2['s1w11']>=0))&
((combined2['s2w7']<5)&(combined2['s2w7']>=0))&
((combined2['s2w8']<5)&(combined2['s2w8']>=0))&
((combined2['s2w9']<5)&(combined2['s2w9']>=0))&
((combined2['s2w10']<5)&(combined2['s2w10']>=0))&
((combined2['s2w11']<5)&(combined2['s2w11']>=0))&
((combined2['s2w12']<5)&(combined2['s2w12']>=0))&
((combined2['s2w13']<5)&(combined2['s2w13']>=0))&
((combined2['s3w4']<5)&(combined2['s3w4']>=0))&
((combined2['s3w5']<5)&(combined2['s3w5']>=0))&
((combined2['s3w6']<5)&(combined2['s3w6']>=0))&
((combined2['s3w7']<5)&(combined2['s3w7']>=0))&
((combined2['s3w8']<5)&(combined2['s3w8']>=0))&
((combined2['s3w9']<5)&(combined2['s3w9']>=0))&
((combined2['s3w10']<5)&(combined2['s3w10']>=0))&
((combined2['s3w11']<5)&(combined2['s3w11']>=0))&
((combined2['s3w12']<5)&(combined2['s3w12']>=0))&
((combined2['s3w13']<5)&(combined2['s3w13']>=0))&
((combined2['s4w6']<5)&(combined2['s4w6']>=0))&
((combined2['s4w7']<5)&(combined2['s4w7']>=0))&
((combined2['s4w8']<5)&(combined2['s4w8']>=0))&
((combined2['s4w9']<5)&(combined2['s4w9']>=0))&
((combined2['s4w10']<5)&(combined2['s4w10']>=0))&
((combined2['s4w11']<5)&(combined2['s4w11']>=0))&
((combined2['s4w12']<5)&(combined2['s4w12']>=0))&
((combined2['s5w0']<5)&(combined2['s5w0']>=0))&
((combined2['s5w1']<5)&(combined2['s5w1']>=0))&
((combined2['s5w2']<5)&(combined2['s5w2']>=0))&
((combined2['s5w3']<5)&(combined2['s5w3']>=0))&
((combined2['s5w4']<5)&(combined2['s5w4']>=0))&
((combined2['s5w5']<5)&(combined2['s5w5']>=0))&
((combined2['s5w11']<5)&(combined2['s5w11']>=0))&
((combined2['s5w12']<5)&(combined2['s5w12']>=0))&
((combined2['s5w13']<5)&(combined2['s5w13']>=0))&
((combined2['s6w0']<5)&(combined2['s6w0']>=0))&
((combined2['s6w1']<5)&(combined2['s6w1']>=0))&
((combined2['s6w2']<5)&(combined2['s6w2']>=0))&
((combined2['s6w3']<5)&(combined2['s6w3']>=0))&
((combined2['s6w4']<5)&(combined2['s6w4']>=0))&
((combined2['s6w5']<5)&(combined2['s6w5']>=0))&
((combined2['s6w11']<5)&(combined2['s6w11']>=0))&
((combined2['s6w12']<5)&(combined2['s6w12']>=0))&
((combined2['s6w13']<5)&(combined2['s6w13']>=0)),'A','Other')
