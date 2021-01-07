# -*- coding: utf-8 -*-
"""
Created on Tue Feb 27 21:25:31 2018

@author: Jubaplus
"""

import datetime
from sqlalchemy import create_engine
import pandas as pd
import MySQLdb
import numpy as np
import os
import Functions25_30_36

outputpath = '/home/nielsen/Spencer/engine/amc1516/'
network = 'AMC  '
startdate = 20151004
enddate = 20160808
minage = 25
maxage = 54
gender = ['F','M']
programid = ['0385635',' 199085']


try:
    os.stat(outputpath)
except:
    os.mkdir(outputpath)
engine = create_engine('mysql://linked_admin:EyAeRxy4PH3Ut5L8@localhost/linked')


persons=Functions25_30_36.demographics(startdate,enddate)
persons['Age'] = persons['Age'].astype('int')
persons = persons[persons['Age'].isin(range(minage,(maxage+1)))]
persons = persons[persons['Gender'].isin(gender)]
persons['HHID_PersonID'] = persons['HHID']+ '_' + persons['PersonID']

PersonIntab = pd.read_csv(outputpath + 'personintab.csv')
df = pd.read_csv(outputpath + 'IDlist.csv')
df = df[df['Cluster']!='Balance']

import os
filstlist = os.listdir(outputpath)

dfprogram = pd.DataFrame()
for i in filstlist:
    if ('programp' in i)&('allprograms' not in i)&('AllProgram' not in i):
        df_data = pd.read_csv(outputpath + i)
        df_data = df_data[['HHID_PersonID','ProgramID','NetworkName','ProgramName','est_season','mins','flipfreq','shareoffreq', 'shareofmins']]
        df_data = df_data[df_data['ProgramID'].isin(programid)]
        dfprogram = dfprogram.append(df_data,ignore_index = True)

dfprogram = pd.merge(dfprogram,df[['HHID_PersonID']],on = 'HHID_PersonID')
dfprogram = pd.merge(dfprogram,persons[['HHID_PersonID','Gender']],on = 'HHID_PersonID')
dfprogram = pd.merge(dfprogram,PersonIntab,on = 'HHID_PersonID')

for i in ['/home/nielsen/MicroSegment/Outputs/Quads/AMC_1516.csv',
'/home/nielsen/MicroSegment/Outputs/Quads/WETV_1516.csv',
'/home/nielsen/MicroSegment/Outputs/Quads/SUND_1516.csv',
'/home/nielsen/MicroSegment/Outputs/Quads/IFC_1516.csv',
'/home/nielsen/MicroSegment/Outputs/Quads/BBCA_1516.csv']:
    dfco = pd.read_csv(i)
    dfco = dfco[dfco['Cluster']=='Core_Occ']
    dfco = pd.merge(dfco[['HHID_PersonID','Cluster']],
                    dfprogram,on = 'HHID_PersonID',how= 'right')
    dfco['Cluster'].fillna('other',inplace = True)
    dfco['intabcross'] = np.where(dfco['Cluster']=='Core_Occ',
                                  dfco['PersonIntab'],0 )
    dfco = dfco[['ProgramID','NetworkName','ProgramName',
                 'est_season','Gender','PersonIntab','intabcross']].groupby(['ProgramID','NetworkName',
                         'ProgramName','est_season','Gender']).sum().unstack('Gender')
    dfco.to_csv(outputpath +i[41:44]+'new.csv')



