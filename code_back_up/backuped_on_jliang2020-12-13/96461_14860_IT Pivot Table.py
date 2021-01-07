# -*- coding: utf-8 -*-
"""
Created on Mon Aug 15 11:21:46 2016

@author: jubaplus1
"""
#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/CMT - Nashville/'

daypartspath='IT-HH-ViewershipDayPartsNew.csv'
clusterpath='nash cluster.csv'
balance_cluster=[3]
occasional_cluster=[7,8]

#####chose the clusters needed the value could be 'Occasional','Core','Balance'
clusterselection=['Occasional','Core','Balance']
#####chose the shifted code needed , the value could be 1, 2, 3
#####1 is live only, 2 is shifted only, 3 is both
shiftedviewingselection=[2,3]

###if episode name has been changed. change below indicator to 'T' and update episode name path 
episodenamechangeindicator='T'
episodenamepath='Epnames.csv'

####quads included, if quads are not included, change this value to 'F'
quadsindicator='F'
quadpath='CMT 6month.csv'

#####if include not live+3 days, change below value to 'F' 
live3indicator='T'







#####code starts here, DO NOT make any changes
import os
os.chdir(foldername)

import pandas as pd
import numpy as np

df=pd.read_csv(daypartspath,sep="\t",dtype={'Episode Name':'object'})
df=df.fillna('na')

if episodenamechangeindicator=='T':
    test=pd.read_csv(episodenamepath)
    df=pd.merge(df,test,on=['Program Code','Program Name','Episode Name','Broadcast Date'])
    df['Program Name']=df['NewProgramName']
    df['Program Code']=df['NewProgramCode']

if live3indicator=='T':
    df=df[df['Minimum Play Delay Minutes']<=4500]


Cluster=pd.read_csv(clusterpath)
Cluster.columns=['HHID','Clusters']
Cluster['Clusters']=np.where(Cluster.Clusters.isin(balance_cluster),'Balance',np.where(Cluster.Clusters.isin(occasional_cluster),'Occasional','Core'))

Cluster=Cluster[Cluster.Clusters.isin(clusterselection)]
#print(Cluster.groupby('Clusters').count())


hhintab=df[['HH In-tab Weight','Household Number']]
hhintab=hhintab.groupby('Household Number').mean()
hhintab.reset_index(inplace=True)  
hhintab.columns=['HHID', 'HHIntab']


shiftview2=df[['Household Number','Time Shifted Viewing Code']]
shiftview2=shiftview2.drop_duplicates()
shiftview2['Time Shifted Viewing Code']=np.where(shiftview2['Time Shifted Viewing Code']==1,1,0)
shiftview2=shiftview2.groupby(['Household Number']).mean()
shiftview2.reset_index(inplace=True)  
shiftview2['Time Shifted Viewing Code']=np.where(shiftview2['Time Shifted Viewing Code']==1,1,
np.where(shiftview2['Time Shifted Viewing Code']==0,2,3))
shiftview2.columns=['HHID','ShiftedViewindicator']

shiftview2=shiftview2[shiftview2.ShiftedViewindicator.isin(shiftedviewingselection)]
#print(shiftview2.groupby('ShiftedViewindicator').count())

    
hhdf=df[['Household Number','Program Code','Program Name']]
hhdf.columns=['HHID', 'Program Code', 'Program Name']
hhdf=hhdf.drop_duplicates()
hhdf=pd.merge(hhdf,hhintab,on='HHID')


if quadsindicator=='T':
    Quads=pd.read_csv(quadpath)
    Quads=Quads[['HHID']]
    Quads=Quads.drop_duplicates()
    Quads=pd.merge(Quads,hhintab,on='HHID')
    Quads['Program Code']=999
    Quads['Program Name']='Quads'
    hhdf=hhdf.append(Quads,ignore_index=True)
    
    
hhdf=pd.merge(hhdf,Cluster,on='HHID')
hhdf=pd.merge(hhdf,shiftview2,on='HHID')

df2=pd.DataFrame()
df4=pd.DataFrame()
program=hhdf[['Program Code','Program Name']]
program=program.drop_duplicates()

for i in program['Program Name']:
    df2=hhdf[hhdf['Program Name']==i]
    df2=df2[['HHID','Program Name']]
    df2=df2.drop_duplicates()
    df2.columns=['HHID','program_name']
    df2=pd.merge(hhdf,df2,on='HHID')
    df4=df4.append(df2,ignore_index=True)

df4=pd.pivot_table(df4, values='HHIntab', index=['program_name'], columns=['Program Name'], aggfunc=np.sum)

df4.to_csv('pivottableONLYshifted.csv')
print('Clusters Selected:',clusterselection)
print('Shifted Code Selected:',shiftedviewingselection)

