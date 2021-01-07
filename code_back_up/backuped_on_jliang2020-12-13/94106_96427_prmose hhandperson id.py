# -*- coding: utf-8 -*-
"""
Created on Tue Feb 14 14:09:08 2017

@author: jubaplus1
"""

import datetime
import pandas as pd
print(datetime.datetime.now())

#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop'

###### make sure the directory file is up to date
personintab='/Users/jubaplus1/Desktop/personintab_150101_170108.csv'
demofilepath='/Users/jubaplus1/Desktop/spark Demographicpersonlevel_150101_170101.csv'

intab=pd.read_csv(personintab,dtype={'HHID':'object','PersonID':'object'})

personintab=intab[['HHID','PersonID','PersonIntab']]
personintab=personintab.groupby(['HHID','PersonID']).mean()
personintab.reset_index(inplace=True)
hhintab=intab[['HHID','HHIntab']]
hhintab=hhintab.drop_duplicates()
hhintab=hhintab.groupby('HHID').mean()
hhintab.reset_index(inplace=True)
intab=pd.merge(personintab,hhintab,on='HHID')
intab['HHID_PersonID']=intab['HHID']+'_'+intab['PersonID']



demo=pd.read_csv(demofilepath,dtype={'HHID':'object','PersonID':'object'})
demo['HHID']=demo['HHID'].apply(lambda x: x.zfill(7))
demo['PersonID']=demo['PersonID'].apply(lambda x: x.zfill(2))
demo['HHID_PersonID']=demo['HHID']+'_'+demo['PersonID']


df=pd.merge(demo,intab,on=['HHID','PersonID'])
df['HHID_PersonID']=df['HHID']+'_'+df['PersonID']
df=df.drop_duplicates(['HHID_PersonID'])
df.to_csv("AudienceSizingspark.csv",index=False)
