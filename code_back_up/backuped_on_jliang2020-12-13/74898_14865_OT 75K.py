# -*- coding: utf-8 -*-
"""
Created on Fri Sep 30 15:07:58 2016

@author: jubaplus1
"""

Income75k='/Users/jubaplus1/Downloads/75K.csv'
OT='/Users/jubaplus1/Desktop/Work/CMT - Nashville/CABLE OT/alltransaction.csv'
import pandas as pd

otfiles=pd.read_csv(OT)
income=pd.read_csv(Income75k)
print(otfiles.columns)
income.columns
otfiles['freq']=1
otfiles['programnew']=otfiles['CableName']+'_'+otfiles['ProgramName']

otfiles=pd.merge(otfiles,income,on='HHID')
import numpy as np
df=pd.pivot_table(otfiles, values=['freq'], index=['programnew'], columns=['75k'], aggfunc=np.sum)

df.to_csv('/Users/jubaplus1/Downloads/OT 75k.csv')


