# -*- coding: utf-8 -*-
"""
Created on Wed Sep  7 15:08:29 2016

@author: jubaplus1
"""
import datetime
print(datetime.datetime.now())

foldername='/Users/Jubaplus1/Desktop/'

###### make sure the directory file is up to date
demofilepath='/Users/jubaplus1/Desktop/Nielsen/demographichhid0811.csv'

####code start here, do not change
import os
os.chdir(foldername)

import pandas as pd


df=pd.read_csv(demofilepath)

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
df.to_csv('DemographicsVariables.csv',index=False)
print(datetime.datetime.now())