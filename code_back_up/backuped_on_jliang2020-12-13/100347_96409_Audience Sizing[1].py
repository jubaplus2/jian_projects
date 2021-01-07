# -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus
"""
import pandas as pd
import numpy as np

###download this file from server
HHIntab=pd.read_csv("/Users/jubaplus1/Desktop/Nielsen/type25all.csv",dtype={'Week':'object','Weeknumber':'int'})
HHIntab['ViewDate2']=pd.to_datetime(HHIntab['Week'])
HHIntab.groupby('Weeknumber').count()
HHIntab['Weeknumber']=HHIntab['Weeknumber']-7

HHIntab['ViewDate']=np.where(HHIntab['Weeknumber']==-1,(HHIntab['ViewDate2']+pd.DateOffset(-1)),
np.where(HHIntab['Weeknumber']==-2,(HHIntab['ViewDate2']+pd.DateOffset(-2)),
np.where(HHIntab['Weeknumber']==-3,(HHIntab['ViewDate2']+pd.DateOffset(-3)),
np.where(HHIntab['Weeknumber']==-4,(HHIntab['ViewDate2']+pd.DateOffset(-4)),
np.where(HHIntab['Weeknumber']==-5,(HHIntab['ViewDate2']+pd.DateOffset(-5)),
np.where(HHIntab['Weeknumber']==-6,(HHIntab['ViewDate2']+pd.DateOffset(-6)),
(HHIntab['ViewDate2'])))))))
HHIntab=HHIntab[['HHID', 'HHIntab','ViewDate']]
HHIntab=HHIntab[HHIntab['HHIntab']>0]
HHIntab=HHIntab.drop_duplicates(['HHID', 'ViewDate'])

##update date to last three month
HHIntab2=HHIntab[(HHIntab['ViewDate']>='2015-12-22')&(HHIntab['ViewDate']<='2016-06-22')]

HHIntab2=HHIntab2.groupby('HHID').mean()
HHIntab2.reset_index(inplace=True)

#HHIntab2.to_csv('/Users/Jubaplus/Desktop/HHIntab2.csv',index=False)

###run demographic file in server and read this file here
df=pd.read_csv("/Users/jubaplus1/Desktop/Nielsen/demographichhid.csv")

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

demo=pd.merge(HHIntab2,df,on='HHID')
df=demo
##quads
Startdate=20151222
Enddate=20160622

names36=[
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_03_20_CBLNM_R0/N16032V0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_03_27_CBLNM_R0/N16033V0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_04_03_CBLNM_R0/N16034V0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_04_10_CBLNM_R0/N16041V0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_04_17_CBLNM_R0/N16042V0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_04_24_CBLNM_R0/N16043V0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_01_CBLNM_R0/N16044V0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_08_CBLNM_R0/N16051V0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_15_CBLNM_R0/N16052V0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_22_CBLNM_R0/N16053V0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_29_CBLNM_R0/N16054V0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_06_05_CBLNM_R0/N16055V0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_06_12_CBLNM_R0/N16061V0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_06_19_CBLNM_R0/N16062V0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_06_26_CBLNM_R0/N16063V0.ECN',

]
names30=[
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_03_20_CBLNM_R0/N16032D0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_03_27_CBLNM_R0/N16033D0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_04_03_CBLNM_R0/N16034D0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_04_10_CBLNM_R0/N16041D0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_04_17_CBLNM_R0/N16042D0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_04_24_CBLNM_R0/N16043D0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_01_CBLNM_R0/N16044D0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_08_CBLNM_R0/N16051D0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_15_CBLNM_R0/N16052D0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_22_CBLNM_R0/N16053D0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_29_CBLNM_R0/N16054D0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_06_05_CBLNM_R0/N16055D0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_06_12_CBLNM_R0/N16061D0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_06_19_CBLNM_R0/N16062D0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_06_26_CBLNM_R0/N16063D0.ECN',

]

df=pd.DataFrame()
type36=pd.DataFrame()
for i in names36:
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='36']
    df['HHID']=df['all'].str[29:36]
    df['CableID']=df['all'].str[3:9]
    df=df[df['CableID']=='007516']
    df['PersonID']=df['all'].str[36:38]
    df=df[df['PersonID']!='00']
    df['Programcode']=df['all'].str[9:16]
    df['TelecastN']=df['all'].str[16:23]
    type36=type36.append(df,ignore_index=True)
 
   
df=pd.DataFrame()
type30=pd.DataFrame()
for i in names30:
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='30']
    df['ProgramDate']=df['all'].str[39:47]
    df['CableID']=df['all'].str[8:14]
    df=df[df['CableID']=='007516']
    type30=type30.append(df,ignore_index=True)
  
type30['Programcode']=type30['all'].str[14:21]
type30['TelecastN']=type30['all'].str[21:28]
type30['StartTime']=type30['all'].str[88:92].astype(int)
type30['EndTime']=type30['all'].str[94:98].astype(int)
type30['Date2']=type30['ProgramDate'].astype('int')
type30=type30[(type30['Date2']>=Startdate)&(type30['Date2']<=Enddate)]
###hash below line if you want all time
type30=type30[(((type30['StartTime']>2000)&(type30['StartTime']<2300))|((type30['EndTime']>2000)&(type30['EndTime']<2300)))]
type30['Weekday']=pd.to_datetime(type30['ProgramDate']).dt.dayofweek
type30=type30[type30.Weekday.isin([3,4])]


WGNviewers=pd.merge(type36,type30,on=['CableID','Programcode','TelecastN'])

WGNviewers=WGNviewers[['HHID']]
WGNviewers['Quads']=1
WGNviewers=WGNviewers.drop_duplicates()
WGNviewers['HHID']=WGNviewers['HHID'].astype(int)

df=demo
df=pd.merge(WGNviewers,df,on='HHID',how='outer')
df['Quads'].fillna(0,inplace=True)

ITshows=pd.read_csv('/Users/jubaplus1/Desktop/WETV MBC S6/showaffinitymbc.csv')
ITshows['segment']=np.where((ITshows['Totalfreq375479']>0|(ITshows['Totalfreq300000']>0)),"A1",
np.where(((ITshows['Totalfreq402099']>0)|(ITshows['Totalfreq397993']>0)|(ITshows['Totalfreq392896']>0)),'A2',
         np.where(((ITshows['Totalfreq300368']>0)|(ITshows['Totalfreq312073']>0)),"A3",'Other'))) 
ITshows.groupby('segment').count()
ITshows=ITshows[['Household Number', 'segment']]
ITshows.columns=['HHID','Segment']

df=pd.merge(ITshows,df,on='HHID',how='outer')

#OTshows=pd.read_csv('/Users/jubaplus1/Desktop/WETV MBC S6/OT/programfreq.csv')
#OTshows2=pd.read_csv('/Users/jubaplus1/Desktop/otshowsp2.csv')
#OTshows=pd.merge(OTshows1,OTshows2,on=['HHID'])
#####update the show names
#OTshows['OT']=np.where(((OTshows['VH1.._DEAR.MAMA']>0)|(OTshows['VH1.._BEHIND.THE.MOVIE']>0)|(OTshows['BET.._SOUL.TRAIN.AWARDS']>0)|(OTshows['VH1.._FAMILY.THERAPY']>0)|(OTshows['BET.._BEING.MARY.JANE.S3']>0)|(OTshows['BRVO._SHAHS.OF.SUNSET']>0)|(OTshows['VH1.._STEVIE.J...JOSELINE']>0)|(OTshows['BET.._BET.HONORS..THE..........']>0)|(OTshows['BRVO._VANDERPUMP.AFTER.SHOW']>0)|(OTshows['ENT.._ROYALS..THE..............']>0)),1,0)
#OTshows=OTshows[OTshows['OT']==1]
#OTshows=OTshows[['HHID','OT']]
#df=pd.merge(OTshows,df,on='HHID',how='outer')


df.to_csv("/Users/jubaplus1/Desktop/WETV MBC S6/demographicsMBC6month.csv")












