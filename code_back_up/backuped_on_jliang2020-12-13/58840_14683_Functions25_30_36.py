# -*- coding: utf-8 -*-
"""
Created on Wed Feb 1 2017

@author: Jubaplus
"""
#post analysis functions

import logging
import datetime
import pandas as pd
import numpy as np

def demographics(Startdate,Enddate):
    Startdate=datetime.datetime.strptime(str(Startdate), '%Y%m%d').date()
    startdate=Startdate+datetime.timedelta(days=(6-Startdate.weekday()))
    Enddate=datetime.datetime.strptime(str(Enddate), '%Y%m%d').date()
    enddate=Enddate+datetime.timedelta(days=(6-Enddate.weekday()))
    directory=pd.read_csv('/home/nielsen/Nielsen_Directory_Server/NielsenDirectoryServer.csv')
    directory['Week']=pd.to_datetime(directory['Week'])
    directory=directory.sort_values('Week',ascending=0)
    names2024=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table']=='EHH')]
    names2024=names2024['FilePath']
    names14=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table']=='HHC')]
    names14=names14['FilePath']
    names65=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table']=='MVD')]
    names65=names65['FilePath']
    names67=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table']=='SVD')]
    names67=names67['FilePath']
    names67=sorted(names67)
    names67=reversed(names67)
    df=pd.DataFrame()
    df2=pd.DataFrame()
    type20=pd.DataFrame()
    type24=pd.DataFrame()
    for i in names2024:
        df=pd.read_csv(i,sep='\t')
        df.columns=['all']
        df['type']=df['all'].str[0:2]
        df2=df[df['type']=='24']
        df=df[df['type']=='20']
        df['HHID']=df['all'].str[13:20]
        df['Race']=df['all'].str[39:40]
        df['Household_Online_Date']=df['all'].str[82:90]
        df['Household_Online_Date']=pd.to_datetime(df['Household_Online_Date'])
        df['PresentofChildren']=df['all'].str[41:42]
        df['Education']=df['all'].str[58:59]
        df['TVwithpay']=df['all'].str[73:75]
        df['DVR']=df['all'].str[70:71]
        df['VideoGame']=df['all'].str[81:82]
        df['Household_Income_Ranges']=df['all'].str[52:54]
        df['County_Size_Code']=df['all'].str[38:39]
        df['Geo_Code']=df['all'].str[36:37]
        df['Presence_of_Children']=df['all'].str[41:42]
        df['Presence_of_Children0_3']=df['all'].str[43:44]
        df['Presence_of_Children6_11']=df['all'].str[46:47]
        df['Presence_of_Children12_17']=df['all'].str[47:48]
        df['Head_of_Household_Origin_Code']=df['all'].str[90:91]
        df['HHAsian']=df['all'].str[156:157]
        df['NumberofTruck']=df['all'].str[99:101]
        df['starteffetivedate']=df['all'].str[20:28].astype('int')
        df=df.sort_values('starteffetivedate',ascending=0)
        df=df.drop_duplicates('HHID')
        j=i[37:47].replace('_','')
        df['LastUpdateDate']=j
        df['LastUpdateDate']=pd.to_datetime(df['LastUpdateDate'])
        type20=type20.append(df,ignore_index=True)
        type20=type20.sort_values(['starteffetivedate','LastUpdateDate'],ascending=False)
        type20=type20.drop_duplicates('HHID')
        df2['HHID']=df2['all'].str[13:20]
        df2['PersonID']=df2['all'].str[20:22]
        df2['Age']=df2['all'].str[38:41]
        df2['Gender']=df2['all'].str[41:42]
        df2['Principal_Moviegoer_Code']=df2['all'].str[60:61]
        df2['starteffetivedate']=df2['all'].str[22:30].astype('int')
        df2=df2.sort_values('starteffetivedate',ascending=0)
        df2=df2.drop_duplicates(['HHID','PersonID'])
        df2['ReportWeek24']=j
        df2['ReportWeek24']=pd.to_datetime(df2['ReportWeek24'])
        type24=type24.append(df2,ignore_index=True)
        type24=type24.sort_values(['starteffetivedate','ReportWeek24'],ascending=False)
        type24=type24.drop_duplicates(['HHID','PersonID'])
        logging.info(i)
    type20['HHAsian'].fillna('N',inplace=True)
    type20['Ethnicity']=np.where(type20['Head_of_Household_Origin_Code']=='1','Hispanic',np.where(type20['Race']=='1','Black',np.where(type20['Race']=='2','White',np.where(type20['HHAsian']=='Y','Asian','Other'))))   
    type20=type20[['HHID','Race','Ethnicity','Household_Online_Date','LastUpdateDate',
                   'Geo_Code','Education','TVwithpay','DVR','VideoGame','PresentofChildren','Household_Income_Ranges','County_Size_Code','Presence_of_Children','Presence_of_Children0_3','Presence_of_Children6_11','Presence_of_Children12_17','Head_of_Household_Origin_Code','HHAsian','NumberofTruck','starteffetivedate']]
    type24=type24[['HHID','PersonID','Age','Gender','Principal_Moviegoer_Code']]   
    alldemo=pd.merge(type24,type20,on='HHID')
    df=pd.DataFrame()
    type14=pd.DataFrame()
    for i in names14:
        df=pd.read_csv(i,sep='\t')
        df.columns=['all']
        df['type']=df['all'].str[0:2]
        df=df[df['type']=='14']
        df['HHID']=df['all'].str[13:20]
        df['LPMFlag']=df['all'].str[64:65]
        df['LPMCode']=df['all'].str[65:68]
        df=df.drop_duplicates()   
        type14=type14.append(df,ignore_index=True)
        logging.info(i)    
    type14=type14[['HHID','LPMFlag','LPMCode']]
    type14=type14.drop_duplicates('HHID')
    alldemo=pd.merge(alldemo,type14,on='HHID')
    df=pd.DataFrame()
    type65=pd.DataFrame()
    for i in names65:
        df=pd.read_csv(i,sep='\t')
        df.columns=['all']
        df['type']=df['all'].str[0:2]
        df=df[df['type']=='65']
        df['HHID']=df['all'].str[13:20]
        df['starteffetivedate']=df['all'].str[20:28].astype('int')       
        df['ATT']=df['all'].str[37:38]
        df['Brighthouse']=df['all'].str[38:39]
        df['Cablevision']=df['all'].str[39:40]
        df['Charter']=df['all'].str[40:41]
        df['Comcast']=df['all'].str[41:42]
        df['Cox']=df['all'].str[42:43]
        df['Time Warner']=df['all'].str[43:44]
        df['Verizon']=df['all'].str[44:45]
        df['DirecTV']=df['all'].str[45:46]
        df['Dish Network']=df['all'].str[46:47]
        df=df.drop_duplicates()   
        type65=type65.append(df,ignore_index=True)
        logging.info(i)  
    type65=type65.sort_values('starteffetivedate',ascending=0)
    type65=type65.drop_duplicates('HHID')
    df=pd.DataFrame()
    type65v2=pd.DataFrame()
    MVPD=['ATT', 'Brighthouse', 'Cablevision', 'Charter', 'Comcast', 'Cox','Time Warner','Verizon','DirecTV','Dish Network']
    for i in MVPD:
        df=type65[['HHID', i]]
        df.columns=['HHID', 'MVPD']
        df=df[df['MVPD']=='Y']
        df['MVPD']=i
        type65v2=type65v2.append(df,ignore_index=True)
    alldemo=pd.merge(alldemo,type65v2,on='HHID',how='left')
    alldemo['MVPD'].fillna('Other',inplace=True)
    df=pd.DataFrame()
    type67=pd.DataFrame()
    for i in names67:
        df=pd.read_csv(i,sep='\t')
        df.columns=['all']
        df['type']=df['all'].str[0:2]
        df=df[df['type']=='67']
        df['HHID']=df['all'].str[13:20]
        df['starteffetivedate']=df['all'].str[20:28].astype('int')
        df['SVODSubscription']=df['all'].str[37:38]
        df['AmazonPrimeSubscription']=df['all'].str[38:39]
        df['HuluSubscription']=df['all'].str[39:40]
        df['NetflixSubscription']=df['all'].str[40:41]
        df=df.drop_duplicates()   
        j=i[37:47].replace('_','')
        df['ReportWeek67']=j
        type67=type67.append(df,ignore_index=True)
        logging.info(i)
    type67=type67.sort_values('starteffetivedate',ascending=0)
    type67=type67.drop_duplicates('HHID')
    type67=type67[['HHID','SVODSubscription','AmazonPrimeSubscription',
               'HuluSubscription','NetflixSubscription']]
    alldemo=pd.merge(alldemo,type67,on='HHID')
    alldemo=alldemo.drop_duplicates(['HHID','PersonID'])
    return alldemo

def personintab(Startdate,Enddate):
    Startdate=datetime.datetime.strptime(str(Startdate), '%Y%m%d').date()
    Startdate=Startdate+datetime.timedelta(days=(6-Startdate.weekday()))
    Enddate=datetime.datetime.strptime(str(Enddate), '%Y%m%d').date()
    Enddate=Enddate+datetime.timedelta(days=(6-Enddate.weekday()))
    directory=pd.read_csv('/home/nielsen/Nielsen_Directory_Server/NielsenDirectoryServer.csv')
    directory['Week']=pd.to_datetime(directory['Week'])
    directory=directory.sort_values('Week',ascending=0)
    names25=directory[(directory['Week']<=Enddate)&(directory['Week']>=Startdate)&(directory['Table']=='EHH')]
    names25=names25['FilePath']
    df=pd.DataFrame()
    type26=pd.DataFrame()
    for i in names25:
        df=pd.read_csv(i,sep='\t')
        j=i[37:47].replace('_','')
        df.columns=['all']
        df['type']=df['all'].str[0:2]
        df=df[df['type']=='26'] 
        df['HHID']=df['all'].str[13:20]
        df['PersonID']=df['all'].str[20:22]
        df['Week']=df['all'].str[5:13]
        #df=df[df['Week']==j]
        df['Mon']=df['all'].str[22:28]
        df['Tue']=df['all'].str[28:34]
        df['Wed']=df['all'].str[34:40]
        df['Thu']=df['all'].str[40:46]
        df['Fri']=df['all'].str[46:52]
        df['Sat']=df['all'].str[52:58]
        df['Sun']=df['all'].str[58:64]
        df=df[['HHID','PersonID','Week', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat','Sun']]
        df=df.drop_duplicates()   
        type26=type26.append(df,ignore_index=True)
        logging.info(i)
        logging.info(j)
    type26['ViewDate2']=pd.to_datetime(type26['Week'])
    df=pd.DataFrame()
    PersonIntab=pd.DataFrame()
    week=['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat','Sun']
    number=[1,2,3,4,5,6,7]
    for i,j in zip(week,number):
        df=type26[['HHID', 'Week','PersonID','ViewDate2',i]]
        df.columns=['HHID', 'Week','PersonID','ViewDate2','PersonIntab']
        df=df[df['PersonIntab']!='000000']
        df['ViewDate']=df['ViewDate2']+pd.DateOffset(j-7)
        df=df[['HHID','PersonID','PersonIntab','ViewDate']]
        PersonIntab=PersonIntab.append(df,ignore_index=True)
    PersonIntab['PersonIntab']=PersonIntab['PersonIntab'].astype(int)
    PersonIntab=PersonIntab.drop_duplicates(['HHID', 'PersonID','ViewDate'])
    return PersonIntab

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
    PersonIntab['HHID_PersonID'] = PersonIntab['HHID']+ '_' + PersonIntab['PersonID']
    del PersonIntab['HHID'],PersonIntab['PersonID']
    PersonIntab.reset_index(inplace=True)
    return PersonIntab
