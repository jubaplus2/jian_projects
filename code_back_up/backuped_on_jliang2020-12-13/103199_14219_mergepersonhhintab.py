# -*- coding: utf-8 -*-
"""
Created on Mon Nov 28 15:57:51 2016

@author: jubaplus1
"""
###### make sure the directory file is up to date
personintab='/Users/jubaplus1/Desktop/Vivica/Sizing/personintab151108_161023.csv'
Startdate=20160423
Enddate=20161023
import pandas as pd
intab=pd.read_csv(personintab,dtype={'HHID':'object','PersonID':'object'})

personintab=intab[['HHID','PersonID','PersonIntab']]
personintab=personintab.groupby(['HHID','PersonID']).mean()
personintab.reset_index(inplace=True)
hhintab=intab[['HHID','HHIntab']]
hhintab=hhintab.drop_duplicates()
hhintab=hhintab.groupby('HHID').mean()
hhintab.reset_index(inplace=True)
intab=pd.merge(personintab,hhintab,on='HHID')

intab.to_csv('',index=False)


