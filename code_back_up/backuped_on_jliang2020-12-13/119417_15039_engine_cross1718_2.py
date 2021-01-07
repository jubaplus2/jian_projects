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

outputpath = '/home/nielsen/MicroSegment/Engine/Client/WGNA/LMS/'
network = 'WGNA '
startdate = 20170213
enddate = 20180211
minage = 25
maxage = 54
gender = ['F','M']
show_dic={'ProgramID':['0377984','0390412','0384014','0322893','0391785','0288526','0357056','0378556','0336507'],
         'startdate':['2017-02-13','2017-02-13','2017-02-13','2017-02-13','2017-02-13','2017-02-13','2017-02-13','2017-02-13','2017-02-13'],
         'enddate':['2018-02-11','2018-02-11','2018-02-11','2018-02-11','2018-02-11','2018-02-11','2018-02-11','2018-02-11','2018-02-11']}

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

dfprogram=pd.read_sql(("select HHID_PersonID,NetworkName,ProgramName,est_season,ProgramID from programs where ProgramID in ('0377984','0390412','0384014','0322893','0391785','0288526','0357056','0378556','0336507')"),con=engine)


dfprogram = pd.merge(dfprogram,df[['HHID_PersonID']],on = 'HHID_PersonID')
dfprogram = pd.merge(dfprogram,persons[['HHID_PersonID','Gender']],on = 'HHID_PersonID')
dfprogram = pd.merge(dfprogram,PersonIntab,on = 'HHID_PersonID')

for i in ['/home/nielsen/MicroSegment/Outputs/Quads/WGNA_l3m_180318.csv',]:
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
    dfco.to_csv(outputpath +i[41:44]+'.csv')



