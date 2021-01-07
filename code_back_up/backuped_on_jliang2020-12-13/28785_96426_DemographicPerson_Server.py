# -*- coding: utf-8 -*-
"""
Created on Wed May 25 16:00:04 2016

@author: Jubaplus
"""

import logging
logging.basicConfig(filename='audienceszinglogs.log', level=logging.INFO)
logging.info('File1 all demographicinfo')
logging.info('Started')
import datetime
logging.info(datetime.datetime.now())

filepath='/disk2/Projects/AudienceSizing/AllDemographicpersonlevel.csv'

import pandas as pd

names2024=['/disk2/Projects/NielsenData/NPM_ARD_W_2016_08_21_EHH_R0/N160820.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_08_14_EHH_R0/N160810.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_08_07_EHH_R0/N160750.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_31_EHH_R0/N160740.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_24_EHH_R0/N160730.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_17_EHH_R0/N160720.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_10_EHH_R0/N160710.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_03_EHH_R0/N160640.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_06_26_EHH_R0/N160630.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_06_19_EHH_R0/N160620.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_06_12_EHH_R0/N160610.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_06_05_EHH_R0/N160550.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_29_EHH_R0/N160540.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_22_EHH_R0/N160530.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_15_EHH_R0/N160520.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_08_EHH_R0/N160510.EHH']

names14=[
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_08_21_HHC_R0/W160820.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_08_14_HHC_R0/W160810.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_08_07_HHC_R0/W160750.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_07_31_HHC_R0/W160740.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_07_24_HHC_R0/W160730.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_07_17_HHC_R0/W160720.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_07_10_HHC_R0/W160710.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_07_03_HHC_R0/W160640.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_06_26_HHC_R0/W160630.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_06_19_HHC_R0/W160620.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_06_12_HHC_R0/W160610.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_06_05_HHC_R0/W160550.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_05_29_HHC_R0/W160540.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_05_22_HHC_R0/W160530.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_05_15_HHC_R0/W160520.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_05_08_HHC_R0/W160510.HHC']

names65=['/disk2/Projects/NielsenData/NPM_ARD_W_2016_08_21_MVD_R0/N160820.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_08_14_MVD_R0/N160810.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_08_07_MVD_R0/N160750.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_31_MVD_R0/N160740.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_24_MVD_R0/N160730.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_17_MVD_R0/N160720.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_10_MVD_R0/N160710.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_03_MVD_R0/N160640.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_06_26_MVD_R0/N160630.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_06_19_MVD_R0/N160620.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_06_12_MVD_R0/N160610.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_06_05_MVD_R0/N160550.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_29_MVD_R0/N160540.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_22_MVD_R0/N160530.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_15_MVD_R0/N160520.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_08_MVD_R0/N160510.MVD']

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
    df['GeoCode']=df['all'].str[36:37]
    df['TimeZone']=df['all'].str[37:38]
    df['CountySize']=df['all'].str[38:39]
    df['Race']=df['all'].str[39:40]
    df['Language']=df['all'].str[40:41]
    df['PresenceofChildren']=df['all'].str[41:42]
    df['Children<2']=df['all'].str[42:43]
    df['Children<3']=df['all'].str[43:44]
    df['Children2to5']=df['all'].str[44:45]
    df['Children<6']=df['all'].str[45:46]
    df['Children6to11']=df['all'].str[46:47]
    df['Children12to17']=df['all'].str[47:48]
    df['NumberofPersonsKids<3']=df['all'].str[48:49]
    df['NumberofPersonsChildren']=df['all'].str[49:50]
    df['NumberofPersonsAdult']=df['all'].str[50:51]
    df['HHSize']=df['all'].str[51:52]
    df['IncomeRange']=df['all'].str[52:54]
    df['NumberofIncomes']=df['all'].str[54:55]
    df['HeadHHAgeBreak']=df['all'].str[55:56]
    df['HeadHHAge']=df['all'].str[56:58]
    df['Education']=df['all'].str[58:59]
    df['Occupation']=df['all'].str[59:60]
    df['LadyPresentFlag']=df['all'].str[60:61]
    df['LadyOccupation']=df['all'].str[61:63]
    df['WiredCable']=df['all'].str[63:64]
    df['PayChannel']=df['all'].str[64:65]
    df['CablePlus']=df['all'].str[65:66]
    df['AltDelivery']=df['all'].str[66:67]
    df['WiredDigitalCable']=df['all'].str[67:68]
    df['DBSOwner']=df['all'].str[68:69]
    df['DVDOwner']=df['all'].str[69:70]
    df['DVR']=df['all'].str[70:71]
    df['NumberofTV']=df['all'].str[71:73]
    df['TVwithPay']=df['all'].str[73:75]
    df['TVwithCable']=df['all'].str[75:77]
    df['TVwithPayandCable']=df['all'].str[77:79]
    df['NumberofVCR']=df['all'].str[79:81]
    df['VideoGame']=df['all'].str[81:82]
    df['HHDate']=df['all'].str[82:90]
    df['HHOrigin']=df['all'].str[90:91]
    df['HHHispanicEthnicity']=df['all'].str[91:93]
    df['NumberofDVR']=df['all'].str[93:94]
    df['CableviaTelco']=df['all'].str[94:95]
    df['NumberofCars']=df['all'].str[97:99]
    df['NumberofTrucks']=df['all'].str[99:101]
    df['NewCarLast3Years']=df['all'].str[101:102]
    df['NewCarLast5Years']=df['all'].str[102:103]
    df['NewTruckLast3Years']=df['all'].str[103:104]
    df['NewTruckLast5Years']=df['all'].str[104:105]
    df['DomesticVehicle']=df['all'].str[105:106]
    df['ForeignVehicle']=df['all'].str[106:107]
    df['Dog']=df['all'].str[107:108]
    df['Cat']=df['all'].str[108:109]
    df['LongDistanceCarrier']=df['all'].str[109:110]
    df['PCAccess']=df['all'].str[110:111]
    df['PCAccessInternet']=df['all'].str[111:112]
    df['IncomeRangeDetails']=df['all'].str[112:114]
    df['HHIncomeAmount']=df['all'].str[114:119]
    df['HHIncomeNonWorking']=df['all'].str[119:120]
    df['HHGender']=df['all'].str[120:121]
    df['NumberofSmartphone']=df['all'].str[162:163]
    df=df.drop_duplicates()   
    type20=type20.append(df,ignore_index=True)
    type20=type20.drop_duplicates()
    df2['HHID']=df2['all'].str[13:20]
    df2['PersonID']=df2['all'].str[20:22]
    df2['Age']=df2['all'].str[38:41]
    df2['Gender']=df2['all'].str[41:42]
    df2['Age/Gender_Building_Block_Code']=df2['all'].str[42:43]
    df2['Visitor_Status_Code']=df2['all'].str[43:44]
    df2['Principal_Shopper']=df2['all'].str[44:45]
    df2['Working_Women_Full_Time_Flag']=df2['all'].str[45:46]
    df2['Working_Women_Part_Time_Flag']=df2['all'].str[46:47]
    df2['Language_Class_Code']=df2['all'].str[47:48]
    df2['Frequent_Moviegoer_Code']=df2['all'].str[48:49]
    df2['Avid_Moviegoer_Code']=df2['all'].str[49:50]
    df2['Number_of_Working_Hours']=df2['all'].str[50:53]
    df2['Lady_of_Household_Flag']=df2['all'].str[53:54]
    df2['Head_of_Household_Flag']=df2['all'].str[54:55]
    df2['Relationship_to_Head_of_Household_Code']=df2['all'].str[55:57]
    df2['Nielsen_Occupation_Code']=df2['all'].str[57:59]
    df2['Works_Outside_of_Home_Flag']=df2['all'].str[59:60]
    df2['Principal_Moviegoer_Code']=df2['all'].str[60:61]
    df2['Education_Ranges']=df2['all'].str[61:62]
    df2['Internet_Usage-Home']=df2['all'].str[62:65]
    df2['Internet_Usage-_Work']=df2['all'].str[65:68]
    df2=df2.drop_duplicates()   
    type24=type24.append(df2,ignore_index=True)
    type24=type24.drop_duplicates()
    logging.info(i)
    
type20=type20[['HHID','GeoCode','TimeZone','CountySize','Race','Language','PresenceofChildren','Children<2','Children<3','Children2to5','Children<6','Children6to11','Children12to17','NumberofPersonsKids<3','NumberofPersonsChildren','NumberofPersonsAdult','HHSize','IncomeRange','NumberofIncomes','HeadHHAgeBreak','HeadHHAge','Education','Occupation','LadyPresentFlag','LadyOccupation','WiredCable','PayChannel','CablePlus','AltDelivery','WiredDigitalCable','DBSOwner','DVDOwner','DVR','NumberofTV','TVwithPay','TVwithCable','TVwithPayandCable','NumberofVCR','VideoGame','HHDate','HHOrigin','HHHispanicEthnicity','NumberofDVR','CableviaTelco','NumberofCars','NumberofTrucks','NewCarLast3Years','NewCarLast5Years','NewTruckLast3Years','NewTruckLast5Years','DomesticVehicle','ForeignVehicle','Dog','Cat','LongDistanceCarrier','PCAccess','PCAccessInternet','IncomeRangeDetails','HHIncomeAmount','HHIncomeNonWorking','HHGender','NumberofSmartphone']]
type20=type20.drop_duplicates('HHID') 
type24=type24[['HHID','PersonID','Age','Gender','Age/Gender_Building_Block_Code','Visitor_Status_Code','Principal_Shopper','Working_Women_Full_Time_Flag','Working_Women_Part_Time_Flag','Language_Class_Code','Frequent_Moviegoer_Code','Avid_Moviegoer_Code','Number_of_Working_Hours','Lady_of_Household_Flag','Head_of_Household_Flag','Relationship_to_Head_of_Household_Code','Nielsen_Occupation_Code','Works_Outside_of_Home_Flag','Principal_Moviegoer_Code','Education_Ranges','Internet_Usage-Home','Internet_Usage-_Work']]
type24=type24.drop_duplicates(['HHID','PersonID']) 
alldemo=pd.merge(type20,type24,on='HHID')

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
    
HHIDs=alldemo[['HHID']]
type65v2=type65v2.drop_duplicates('HHID')
type65v2=pd.merge(HHIDs,type65v2,on='HHID',how='outer')
type65v2=type65v2.fillna('Other')
type65v2=type65v2.drop_duplicates('HHID')
alldemo=pd.merge(alldemo,type65v2,on='HHID')
alldemo=alldemo.drop_duplicates(['HHID','PersonID'])

alldemo.to_csv(filepath,index=False,dtype={'HHID':'object'})

del type65
del type65v2
del df
del type20
del type24
del type14
del names2024
del names14
del names65
del df2
del alldemo
del HHIDs
import gc
gc.collect()
logging.info(datetime.datetime.now())
logging.info('Finished')