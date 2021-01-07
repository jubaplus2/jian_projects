# -*- coding: utf-8 -*-
"""
Created on Thu Jan 18 14:21:15 2018

@author: Jubaplus
"""

import datetime
import pandas as pd
import numpy as np
import gc
import os
from sklearn import preprocessing
from sklearn.cluster import KMeans

ncluster1 = 10
ncluster2 = 12
ncluster3 = 15
startdate = 20161120
enddate = 20171217

folderpath = '/home/nielsen/OT/MGM/Comet/step2outputs/'
personintabpath = '/home/nielsen/MicroSegment/Outputs/PersonIntab/personintab_160101_171217.csv'
itlistpath = '/home/nielsen/OT/MGM/comet_it_list.csv'
alltransactionpath = '/home/nielsen/OT/MGM/Comet/alltransaction.csv'

###code start here do not change!!!
startdate = datetime.datetime.strptime(str(startdate), '%Y%m%d').date()
enddate = datetime.datetime.strptime(str(enddate), '%Y%m%d').date()

df = pd.read_csv(alltransactionpath,
                usecols=['HHID_PersonID','CableName','ProgramName','min'])
df['ProgramFinal'] = df['CableName'] + '_' + df['ProgramName']
df_itlist = pd.read_csv(itlistpath,header = None)
df_itlist = list(df_itlist[0])

df_total_id = df[['HHID_PersonID','min']].groupby('HHID_PersonID').sum()
df_total_id.columns = ['Totalmins']
df_total_id.reset_index(inplace = True)

df_total_show = df[['ProgramFinal','min']].groupby('ProgramFinal').sum()
df_total_show.reset_index(inplace = True)
df_total_show = df_total_show.sort_values('min',ascending = False)
df_total_show.to_csv(folderpath + 'totalminsbyshow.csv',index = False)
df_total_show = df_total_show[~df_total_show['ProgramFinal'].isin(df_itlist)]
df_total_show.reset_index(drop = True,inplace = True)
df_total_show.reset_index(inplace = True)

topshowlist = list(df_total_show[df_total_show['index']<25]['ProgramFinal'])
topshowlist = topshowlist + df_itlist

###step 3
df_topshow = df[df['ProgramFinal'].isin(topshowlist)]
df_topshow = df_topshow[['HHID_PersonID','ProgramFinal',
                         'min']].groupby(['HHID_PersonID','ProgramFinal']).sum().unstack('ProgramFinal')
df_topshow.columns = df_topshow.columns.get_level_values(1)
df_topshow.fillna(0,inplace = True)
df_topshow.reset_index(inplace = True)
df_topshow.to_csv(folderpath + 'programmin_top_shows.csv',index = False)

intab = pd.read_csv(personintabpath,dtype = 'str',
                   usecols=[0,1,2,3])
intab['HHID_PersonID'] = intab['HHID'] +'_'+ intab['PersonID']
intab['ViewDate'] = pd.to_datetime(intab['ViewDate'])
intab = intab[intab['ViewDate']>=startdate]
intab = intab[intab['ViewDate']<=enddate]
intab['PersonIntab'] = intab['PersonIntab'].astype('float')
intab = intab[['HHID_PersonID','PersonIntab']].groupby('HHID_PersonID').mean()
intab.reset_index(inplace = True)
df_topshow = pd.merge(intab,df_topshow,on = 'HHID_PersonID')
df_topshow.fillna(0,inplace = True)

###step 4 clustering
df2 = df_topshow.copy()
del df2['HHID_PersonID'],df2['PersonIntab']

df2 = df2.as_matrix(columns=None)
X_scaled = preprocessing.scale(df2)
##cluster1
np.random.seed(10)
kmeans = KMeans(n_clusters=ncluster1)
kmeans.fit(X_scaled)
labels = kmeans.labels_
df_topshow['Cluster1'] = labels

##cluster2
np.random.seed(10)
kmeans = KMeans(n_clusters=ncluster2)
kmeans.fit(X_scaled)
labels = kmeans.labels_
df_topshow['Cluster2'] = labels

##cluster3
np.random.seed(10)
kmeans = KMeans(n_clusters=ncluster3)
kmeans.fit(X_scaled)
labels = kmeans.labels_
df_topshow['Cluster3'] = labels

df_topshow = pd.merge(df_total_id,df_topshow,on = 'HHID_PersonID')
df_topshow.to_csv(folderpath + 'clusters.csv',index = False)
df_topshow['count'] = 1
showlist2 = list(df_topshow.columns)

showlist2.remove('HHID_PersonID')
showlist2.remove('PersonIntab')
showlist2.remove('Totalmins')
showlist2.remove('Cluster1')
showlist2.remove('Cluster2')
showlist2.remove('Cluster3')
showlist2.remove('count')

for i in range(3):
    df_clsummary = df_topshow[['Cluster'+str(i+1),'PersonIntab','count','Totalmins']+showlist2]
    df_clsummary = df_clsummary.groupby('Cluster'+str(i+1)).sum()
    df_clsummary.fillna(0,inplace = True)
    df_clsummary.reset_index(inplace = True)
    df_clsummary.to_csv(folderpath + 'clustersummary' +str(i+1)+'.csv',index = False)











