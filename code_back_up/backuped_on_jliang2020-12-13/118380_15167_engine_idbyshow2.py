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

outputpath = '/home/nielsen/Spencer/engine/amc1718/'
network = 'AMC  '
startdate = 20170817
enddate = 20180211
minage = 25
maxage = 54
gender = ['F','M']
show_dic={'ProgramID':[' 839643','0380668'],
         'startdate':['2017-10-13','2017-08-17'],
         'enddate':['2018-02-11','2017-09-21']}

try:
    os.stat(outputpath)
except:
    os.mkdir(outputpath)
engine = create_engine('mysql://linked_admin:EyAeRxy4PH3Ut5L8@localhost/linked')


programlist=pd.read_csv(outputpath + 'allprograms.csv',dtype='str')
programlist=programlist[(programlist['ProgramID'].isin(show_dic['ProgramID']))]
#programlist=programlist[(programlist['NetworkName'].isin(network2))]
#programlist=programlist[(programlist['EpisodeName'].str.contains(show)==True)]
programlist['TelecastN'] = programlist['TelecastN'].apply(lambda x:x.zfill(7))
programlist['ProgramID_TelecastN'] = programlist['ProgramID'] + '_'+ programlist['TelecastN']
programlist = programlist[['ProgramID_TelecastN', 'ProgramName','EpisodeName','ProgramDate', 
                           'firstdate','lastdate','NetworkName','ProgramID']]
programlist['Week'] = programlist['ProgramDate'].apply(lambda x:(datetime.datetime.strptime(str(x), '%Y-%m-%d').date() +\
                                                                datetime.timedelta(days= (6-datetime.datetime.strptime(str(x), '%Y-%m-%d').date().weekday()))))

show_df = pd.DataFrame(show_dic)
programlist = pd.merge(programlist,show_df,on = ['ProgramID'])
programlist = programlist[(programlist['ProgramDate']>=programlist['startdate'])&\
                          (programlist['ProgramDate']<=programlist['enddate']) ]
programlist.to_csv(outputpath + 'showlist.csv' , index = False)                          
programlist2=programlist[['ProgramID_TelecastN','firstdate','lastdate', 'ProgramName']]

programlist=programlist['ProgramID_TelecastN']
programin='('
for i in programlist:
    programin=programin+'"'+i+'",'
programin=programin[0:(len(programin)-1)]  
programin=programin+')'

persons=Functions25_30_36.demographics(startdate,enddate)
PersonIntab = Functions25_30_36.personintab(startdate,enddate)
persons['Age'] = persons['Age'].astype('int')
persons = persons[persons['Age'].isin(range(minage,(maxage+1)))]
persons = persons[persons['Gender'].isin(gender)]
persons['HHID_PersonID'] = persons['HHID']+ '_' + persons['PersonID']

def unifiedintab(PersonIntab,startdate,enddate):
    startdate2=datetime.datetime.strptime(str(startdate), '%Y%m%d').date()
    enddate2=datetime.datetime.strptime(str(enddate), '%Y%m%d').date()
    daydiff=(pd.Timedelta(enddate2-startdate2).days+1)*0.75
    PersonIntab=PersonIntab[PersonIntab['ViewDate']>=startdate2]
    PersonIntab=PersonIntab[PersonIntab['ViewDate']<=enddate2]
    intabfinal=PersonIntab[['HHID','PersonID','ViewDate']]
    intabfinal=intabfinal.groupby(['HHID','PersonID']).count()
    intabfinal.reset_index(inplace=True)
    intabfinal=intabfinal[intabfinal['ViewDate']>=daydiff]
    intabfinal=intabfinal[['HHID','PersonID']]
    PersonIntab=PersonIntab[['HHID','PersonID','PersonIntab']]
    PersonIntab['PersonIntab']=PersonIntab['PersonIntab'].round(decimals=4)
    PersonIntab=pd.merge(intabfinal,PersonIntab,on=['HHID','PersonID'])
    PersonIntab=PersonIntab.groupby(['HHID','PersonID']).mean()
    PersonIntab.reset_index(inplace=True)
    PersonIntab['HHID_PersonID'] = PersonIntab['HHID']+ '_' + PersonIntab['PersonID']
    del PersonIntab['HHID'],PersonIntab['PersonID']
    return PersonIntab

PersonIntab = unifiedintab(PersonIntab,startdate,enddate)
df=pd.read_sql(("select HHID_PersonID,flipfreq,mins,ProgramID_TelecastN from telecastlevel where ProgramID_TelecastN in %s"% programin),con=engine)
df = pd.merge(df,persons[['HHID_PersonID','Household_Online_Date','LastUpdateDate']],
              on='HHID_PersonID')
df = pd.merge(df,PersonIntab[['HHID_PersonID']],on='HHID_PersonID')
df=pd.merge(df,programlist2,on='ProgramID_TelecastN')
df['firstdate']=pd.to_datetime(df['firstdate'])
df['lastdate']=pd.to_datetime(df['lastdate'])
df['Household_Online_Date']=pd.to_datetime(df['Household_Online_Date'])
df['LastUpdateDate']=pd.to_datetime(df['LastUpdateDate'])
df=df[(df['Household_Online_Date']<=df['firstdate'])&(df['lastdate']<=df['LastUpdateDate'])]


df.to_csv(outputpath + 'idbyshow.csv',index = False)


                                                                
