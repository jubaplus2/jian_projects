# -*- coding: utf-8 -*-
"""
Created on Wed Sep  7 15:06:41 2016

@author: jubaplus1
"""
import datetime
print(datetime.datetime.now())
#####update the direcotry--all output files will be saved in this directory
foldername='/Users/Jubaplus1/Desktop/'

###### make sure the directory file is up to date
hhintab='/Users/jubaplus1/Desktop/Nielsen/type25_0103_0821.csv'
####put startdate and enddate for the project here:
Startdate=20160221
Enddate=20160821

####code start here, do not change
import os
os.chdir(foldername)

import pandas as pd
import numpy as np

HHIntab=pd.read_csv(hhintab,dtype={'Week':'object','Weeknumber':'int'})
HHIntab['ViewDate2']=pd.to_datetime(HHIntab['Week'])
HHIntab.groupby('Weeknumber').count()
HHIntab['Weeknumber']=HHIntab['Weeknumber']-7

HHIntab['ViewDate']=np.where(HHIntab['Weeknumber']==-1,(HHIntab['ViewDate2']+pd.DateOffset(-1)),
np.where(HHIntab['Weeknumber']==-2,(HHIntab['ViewDate2']+pd.DateOffset(-2)),
np.where(HHIntab['Weeknumber']==-3,(HHIntab['ViewDate2']+pd.DateOffset(-3)),
np.where(HHIntab['Weeknumber']==-4,(HHIntab['ViewDate2']+pd.DateOffset(-4)),
np.where(HHIntab['Weeknumber']==-5,(HHIntab['ViewDate2']+pd.DateOffset(-5)),
np.where(HHIntab['Weeknumber']==-6,(HHIntab['ViewDate2']+pd.DateOffset(-6)),
(HHIntab['ViewDate2'])))))))
HHIntab=HHIntab[['HHID', 'HHIntab','ViewDate']]
HHIntab=HHIntab[HHIntab['HHIntab']>0]
HHIntab=HHIntab.drop_duplicates(['HHID', 'ViewDate'])

import datetime
a=datetime.datetime.strptime(str(Startdate), '%Y%m%d').date()
b=6-a.weekday()
startdate=a+datetime.timedelta(days=b)
c=datetime.datetime.strptime(str(Enddate), '%Y%m%d').date()
d=6-c.weekday()
enddate=c+datetime.timedelta(days=d)

HHIntab2=HHIntab[(HHIntab['ViewDate']>=a)&(HHIntab['ViewDate']<=c)]
del(a,b,c,d)

HHIntab2=HHIntab2.groupby('HHID').mean()
HHIntab2.reset_index(inplace=True)

HHIntab2.to_csv('HHintab.csv',index=False)
print(datetime.datetime.now())

