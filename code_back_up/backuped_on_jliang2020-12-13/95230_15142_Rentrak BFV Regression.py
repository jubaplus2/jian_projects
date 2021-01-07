# -*- coding: utf-8 -*-
"""
Created on Sat Mar 12 16:15:36 2016

@author: Spencer
"""

import pandas as pd
import numpy as np
import statsmodels.api as sm
import pylab as pl
from sklearn.linear_model import LogisticRegression
import statsmodels.formula.api as smf
import os
os.chdir("/Users/jubaplus1/Desktop/Rentrak/BFV")


###code on Mar 22, Rentrak Demo
combined2=pd.read_csv("BFVRentrak0316.csv",dtype={'Zip':'object','Zip4':'object'})

combined2['Rank1']=np.where(combined2['Segment2']=='A',1,0)
combined2.groupby('Rank1').count()

combined2['Rank2']=np.where(combined2['Segment']=='A',1,0)
combined2.groupby('Rank2').count()

combined2['Rank3']=np.where(combined2['Segment3']=='A',1,0)
combined2.groupby('Rank3').count()

combined2['Rank4']=np.where(combined2['Segment3']=='A',1,0)
combined2.groupby('Rank4').count()

combined2['Rank5']=np.where(combined2['Segment3']=='A',1,0)
combined2.groupby('Rank5').count()


Demo=pd.read_csv("/Users/jubaplus1/Desktop/Rentrak/DemoALL.csv",dtype={'ZIP_CODE':'object','ZIP4':'object'})

combined2=combined2[['Zip', 'Zip4','Rank1','Rank2','Rank3']]
combined2.columns=['ZIP_CODE','ZIP4','Rank1','Rank2','Rank3']
combined2=combined2.drop_duplicates(['ZIP_CODE','ZIP4'])

#combined2=combined2[['Zip', 'Zip4','Rank4','Rank5']]
#combined2.columns=['ZIP_CODE','ZIP4','Rank4','Rank5']


combined2=pd.merge(combined2,Demo,on=['ZIP_CODE','ZIP4'])

combined2=combined2[combined2['HH15']>0]

combined2['randonnum']=np.random.choice(range(0,2),combined2.shape[0])
combined2.head(20)
combined2.describe()
combined2.groupby('randonnum').count()

train=combined2[combined2['randonnum']==0]
Validation=combined2[combined2['randonnum']==1]

train.groupby('Rank1').count()
train.groupby('Rank2').count()
train.groupby('Rank3').count()

train.to_csv("BFV_Train.csv",index=False,dtype={'ZIP_CODE':'object','ZIP4':'object'})
train=pd.read_csv("BFV_Train.csv",dtype={'ZIP_CODE':'object','ZIP4':'object'})
train=pd.merge(train,combined2,on=['ZIP_CODE','ZIP4'])

Validation.groupby('Rank1').count()
Validation.to_csv("BFV_Validation.csv",index=False,dtype={'ZIP_CODE':'object','ZIP4':'object'})
Validation=pd.read_csv("BFV_Validation.csv",dtype={'ZIP_CODE':'object','ZIP4':'object'})
Validation=pd.merge(Validation,combined2,on=['ZIP_CODE','ZIP4'])

del combined2
del Demo
import gc
gc.collect()

train.columns
train['const']=1
train['newAVGHHSIZE']=train['AVGHHSIZE']
train['newMEDAGHHER']=train['MEDAGHHER']
train['newMEDRENT']=np.log10(train['MEDRENT']+500)
train['newMEDVALOCC']=np.log10(train['MEDVALOCC']+8000)
train['newMEDVEHICLE']=train['MEDVEHICLE']
train['newMEDHHINC']=train['MEDHHINC']
train['newAVGHHINC']=np.log10(train['AVGHHINC']+10000)
train['newPERCAPINC']=np.log10(train['PERCAPINC']+10000)
train['newMEDAGE']=train['MEDAGE']
train['newPCTHH200P']=np.log(train['PCTHH200P']+0.1)
train['newHHHINCAVG']=np.log10(train['HHHINCAVG']+500000)
train['newPCTOOHH']=np.sqrt(train['PCTOOHH']+500)
train['newPCTROOCCHH']=np.log10(train['PCTROOCCHH']+10)
train['newPCTWHPOP']=train['PCTWHPOP']
train['newPCTBLPOP']=np.log(train['PCTBLPOP']+0.05)
train['newPCTASPOP']=np.log(train['PCTASPOP']+0.05)
train['newPCTAMPOP']=np.log(train['PCTAMPOP']+0.03)
train['newPCTTMPOP']=np.log(train['PCTTMPOP']+0.05)
train['newPCTHISPOP']=np.log(train['PCTHISPOP']+0.08)
train['newPCTGRADDEG']=np.log(train['PCTGRADDEG']+2)
train['newPCTMARC']=np.log(train['PCTMARC']+5)
train['newPCTMARNOC']=train['PCTMARNOC']
train['newPCTWHCOL']=train['PCTWHCOL']
train['newPCTSPARC']=np.log(train['PCTSPARC']+2)
train['newPCTSING']=train['PCTSING']
train['newPCTHHL25']=np.log(train['PCTHHL25']+0.2)
train['newPCTHH25_34']=np.log(train['PCTHH25_34']+2)
train['newPCTHH35_44']=np.log(train['PCTHH35_44']+3)
train['newPCTHH45_54']=np.log(train['PCTHH45_54']+5)
train['newPCTHH55_64']=np.log(train['PCTHH55_64']+20)
train['newPCTHH65_74']=np.log(train['PCTHH65_74']+10)
train['newPCT35_100K']=train['PCT35_100K']
train['newPCT100KP']=np.log(train['PCT100KP']+8)


a=['newAVGHHSIZE',
       'newMEDAGHHER', 'newMEDRENT', 'newMEDVALOCC', 'newMEDVEHICLE',
       'newMEDHHINC', 'newAVGHHINC', 'newPERCAPINC', 'newMEDAGE',
       'newPCTHH200P', 'newHHHINCAVG', 'newPCTOOHH', 'newPCTROOCCHH',
       'newPCTWHPOP', 'newPCTBLPOP', 'newPCTASPOP', 'newPCTAMPOP',
       'newPCTTMPOP', 'newPCTHISPOP', 'newPCTGRADDEG', 'newPCTMARC',
       'newPCTMARNOC', 'newPCTWHCOL', 'newPCTSPARC', 'newPCTSING',
       'newPCTHHL25', 'newPCTHH25_34', 'newPCTHH35_44', 'newPCTHH45_54',
       'newPCTHH55_64', 'newPCTHH65_74', 'newPCT35_100K', 'newPCT100KP']
for i in a:
    print(train.hist(i))
    
train.hist('PCTWHPOP')


train['test']=np.sqrt(train['PCTWHPOP']+5000)
train.hist('test')

train['test']=np.log10(train['PCTWHPOP']+200)
train.hist('test')



pl.show()
train.describe()
train['const']=1
print(train)


train_cols=['newAVGHHSIZE',
       'newMEDAGHHER', 'newMEDRENT', 'newMEDVALOCC', 'newMEDVEHICLE',
       'newMEDHHINC', 'newAVGHHINC', 'newPERCAPINC', 'newMEDAGE',
       'newPCTHH200P', 'newHHHINCAVG', 'newPCTOOHH', 
       'newPCTWHPOP', 'newPCTBLPOP', 'newPCTASPOP', 'newPCTAMPOP',
       'newPCTTMPOP', 'newPCTHISPOP', 'newPCTMARC',
       'newPCTMARNOC', 'newPCTWHCOL', 'newPCTSPARC', 'newPCTSING',
       'newPCTHHL25', 'newPCTHH25_34', 'newPCTHH35_44', 'newPCTHH45_54',
       'newPCTHH55_64', 'newPCTHH65_74', 'newPCT35_100K', 'newPCT100KP', 'const']


#fitthemodel
logit=sm.Logit(train['Rank2'],train[train_cols]) 
result=logit.fit()
result.summary()

train=train.reset_index()
yhat=result.predict()
print(yhat)
yhat=pd.DataFrame(yhat, columns=['yhat'])
yhat.describe()

train=train.join(yhat)
train.describe()

train['decile']=train['yhat'].rank(ascending=False)
train['decile2']=np.where(train['decile']<=157385,1,
np.where(train['decile']<=314770,2,
np.where(train['decile']<=472155,3,
np.where(train['decile']<=629540,4,
np.where(train['decile']<=786925,5,
np.where(train['decile']<=944310,6,
np.where(train['decile']<=1101695,7,
np.where(train['decile']<=1259080,8,
np.where(train['decile']<=1416465,9,10)))))))))

sum1=train[['Rank2','decile2','const']]
sum1.groupby('decile2').sum()
sum1=train.groupby('decile2').mean()
sum1.to_csv("sum.csv")



Validation['const']=1
Validation['newAVGHHSIZE']=Validation['AVGHHSIZE']
Validation['newMEDAGHHER']=Validation['MEDAGHHER']
Validation['newMEDRENT']=np.log10(Validation['MEDRENT']+500)
Validation['newMEDVALOCC']=np.log10(Validation['MEDVALOCC']+8000)
Validation['newMEDVEHICLE']=Validation['MEDVEHICLE']
Validation['newMEDHHINC']=Validation['MEDHHINC']
Validation['newAVGHHINC']=np.log10(Validation['AVGHHINC']+10000)
Validation['newPERCAPINC']=np.log10(Validation['PERCAPINC']+10000)
Validation['newMEDAGE']=Validation['MEDAGE']
Validation['newPCTHH200P']=np.log(Validation['PCTHH200P']+0.1)
Validation['newHHHINCAVG']=np.log10(Validation['HHHINCAVG']+500000)
Validation['newPCTOOHH']=np.sqrt(Validation['PCTOOHH']+500)
Validation['newPCTROOCCHH']=np.log10(Validation['PCTROOCCHH']+10)
Validation['newPCTWHPOP']=Validation['PCTWHPOP']
Validation['newPCTBLPOP']=np.log(Validation['PCTBLPOP']+0.05)
Validation['newPCTASPOP']=np.log(Validation['PCTASPOP']+0.05)
Validation['newPCTAMPOP']=np.log(Validation['PCTAMPOP']+0.03)
Validation['newPCTTMPOP']=np.log(Validation['PCTTMPOP']+0.05)
Validation['newPCTHISPOP']=np.log(Validation['PCTHISPOP']+0.08)
Validation['newPCTGRADDEG']=np.log(Validation['PCTGRADDEG']+2)
Validation['newPCTMARC']=np.log(Validation['PCTMARC']+5)
Validation['newPCTMARNOC']=Validation['PCTMARNOC']
Validation['newPCTWHCOL']=Validation['PCTWHCOL']
Validation['newPCTSPARC']=np.log(Validation['PCTSPARC']+2)
Validation['newPCTSING']=Validation['PCTSING']
Validation['newPCTHHL25']=np.log(Validation['PCTHHL25']+0.2)
Validation['newPCTHH25_34']=np.log(Validation['PCTHH25_34']+2)
Validation['newPCTHH35_44']=np.log(Validation['PCTHH35_44']+3)
Validation['newPCTHH45_54']=np.log(Validation['PCTHH45_54']+5)
Validation['newPCTHH55_64']=np.log(Validation['PCTHH55_64']+20)
Validation['newPCTHH65_74']=np.log(Validation['PCTHH65_74']+10)
Validation['newPCT35_100K']=Validation['PCT35_100K']
Validation['newPCT100KP']=np.log(Validation['PCT100KP']+8)

yhat2=result.predict(Validation[train_cols])
yhat2=pd.DataFrame(yhat2, columns=['yhat'])
yhat.describe()
Validation=Validation.reset_index()
Validation=Validation.join(yhat2)
print(train)
Validation.describe()

Validation['decile']=Validation['yhat'].rank(ascending=False)
Validation['decile2']=np.where(Validation['decile']<=157432,1,
np.where(Validation['decile']<=314864,2,
np.where(Validation['decile']<=472296,3,
np.where(Validation['decile']<=629728,4,
np.where(Validation['decile']<=787160,5,
np.where(Validation['decile']<=944592,6,
np.where(Validation['decile']<=1102024,7,
np.where(Validation['decile']<=1259456,8,
np.where(Validation['decile']<=1416888,9,10)))))))))

sum=Validation.groupby('decile2').mean()
sum.to_csv("sum2.csv")
sum1=Validation[['Rank2','decile2','const']]
sum1.groupby('decile2').sum()


result=logit.fit()

result.summary(Validation[train_cols])

train.to_csv("train.csv",index=False)
Validation.to_csv("Validation.csv",index=False)

del Validation
del sum
del sum1
del a
del i
del yhat
del yhat2
import gc
gc.collect()

Demo=pd.read_csv("DemoALL.csv",dtype={'ZIP_CODE':'object','ZIP4':'object'})

Demo['const']=1
Demo['newAVGHHSIZE']=Demo['AVGHHSIZE']
Demo['newMEDAGHHER']=Demo['MEDAGHHER']
Demo['newMEDRENT']=np.log10(Demo['MEDRENT']+500)
Demo['newMEDVALOCC']=np.log10(Demo['MEDVALOCC']+8000)
Demo['newMEDVEHICLE']=Demo['MEDVEHICLE']
Demo['newMEDHHINC']=Demo['MEDHHINC']
Demo['newAVGHHINC']=np.log10(Demo['AVGHHINC']+10000)
Demo['newPERCAPINC']=np.log10(Demo['PERCAPINC']+10000)
Demo['newMEDAGE']=Demo['MEDAGE']
Demo['newPCTHH200P']=np.log(Demo['PCTHH200P']+0.1)
Demo['newHHHINCAVG']=np.log10(Demo['HHHINCAVG']+500000)
Demo['newPCTOOHH']=np.sqrt(Demo['PCTOOHH']+500)
Demo['newPCTROOCCHH']=np.log10(Demo['PCTROOCCHH']+10)
Demo['newPCTWHPOP']=Demo['PCTWHPOP']
Demo['newPCTBLPOP']=np.log(Demo['PCTBLPOP']+0.05)
Demo['newPCTASPOP']=np.log(Demo['PCTASPOP']+0.05)
Demo['newPCTAMPOP']=np.log(Demo['PCTAMPOP']+0.03)
Demo['newPCTTMPOP']=np.log(Demo['PCTTMPOP']+0.05)
Demo['newPCTHISPOP']=np.log(Demo['PCTHISPOP']+0.08)
Demo['newPCTGRADDEG']=np.log(Demo['PCTGRADDEG']+2)
Demo['newPCTMARC']=np.log(Demo['PCTMARC']+5)
Demo['newPCTMARNOC']=Demo['PCTMARNOC']
Demo['newPCTWHCOL']=Demo['PCTWHCOL']
Demo['newPCTSPARC']=np.log(Demo['PCTSPARC']+2)
Demo['newPCTSING']=Demo['PCTSING']
Demo['newPCTHHL25']=np.log(Demo['PCTHHL25']+0.2)
Demo['newPCTHH25_34']=np.log(Demo['PCTHH25_34']+2)
Demo['newPCTHH35_44']=np.log(Demo['PCTHH35_44']+3)
Demo['newPCTHH45_54']=np.log(Demo['PCTHH45_54']+5)
Demo['newPCTHH55_64']=np.log(Demo['PCTHH55_64']+20)
Demo['newPCTHH65_74']=np.log(Demo['PCTHH65_74']+10)
Demo['newPCT35_100K']=Demo['PCT35_100K']
Demo['newPCT100KP']=np.log(Demo['PCT100KP']+8)


yhat2=result.predict(Demo[train_cols])
yhat2=pd.DataFrame(yhat2, columns=['yhat'])
yhat2.to_csv("yhat.csv",index=False)
yhat2.describe()
Demo=Demo.reset_index()
Demo=Demo.join(yhat2)
Demo.to_csv("DemoALL0406.csv",index=False,dtype={'ZIP_CODE':'object','ZIP4':'object'})
Demo=pd.read_csv("DemoALL0406.csv",dtype={'ZIP_CODE':'object','ZIP4':'object'})
print(train)

Demo.head()
yhat2.head()

test=Demo['yhat']
test.describe()
Demo.describe()

Demo['decile']=Demo['yhat'].rank(ascending=False)
Demo['decile2']=np.where(Demo['decile']<=3914500,'1',
np.where(Demo['decile']<=7829000,'2',
np.where(Demo['decile']<=11743500,'3',
np.where(Demo['decile']<=15658000,'4',
np.where(Demo['decile']<=19572500,'5',
np.where(Demo['decile']<=23487000,'6',
np.where(Demo['decile']<=27401500,'7',
np.where(Demo['decile']<=31316000,'8',
np.where(Demo['decile']<=35230500,'9','10')))))))))

Demo.columns
Demo=Demo[['ZIP_CODE', 'ZIP4', 'POP15', 'HH15', 'POPGROW15', 'AVGHHSIZE',
       'MEDAGHHER', 'MEDRENT', 'MEDVALOCC', 'MEDVEHICLE', 'MEDHHINC',
       'AVGHHINC', 'PERCAPINC', 'MEDAGE', 'PCTHH200P', 'HHHINCAVG', 'PCTOOHH',
       'PCTROOCCHH', 'PCTWHPOP', 'PCTBLPOP', 'PCTASPOP', 'PCTAMPOP',
       'PCTORPOP', 'PCTTMPOP', 'PCTHISPOP', 'PCTWNHPOP', 'PCTGRADDEG',
       'PCTWHCOL', 'PCTMARNOC', 'PCTMARC', 'PCTSPARC', 'PCTSING', 'PCTHHL25',
       'PCTHH25_34', 'PCTHH35_44', 'PCTHH45_54', 'PCTHH55_64', 'PCTHH65_74',
       'PCTHH75P', 'PCTL35K', 'PCT35_100K', 'PCT100KP','yhat', 'decile', 'decile2']]

sum=Demo.groupby('decile2').mean()
sum2=Demo.groupby('decile2').sum()
sum.to_csv("sum.csv")
sum2.to_csv("sum2.csv")

Demo.columns

del Demo
import gc
gc.collect()


Demo2=Demo[['ZIP_CODE', 'ZIP4','decile2','PCTBLPOP','HH15']]

Demo2=Demo2[Demo2['HH15']>0]
Demo2=Demo2[Demo2['decile2']==1]

combined2.columns=['ZIP_CODE','ZIP4','Segment']

Demo2=pd.merge(combined2,Demo2,on=['ZIP_CODE','ZIP4'],how='right')
Demo2=Demo.fillna('0')
Demo2=Demo2[Demo2['Segment']=='0']
Demo2=Demo2.drop_duplicates(['ZIP_CODE','ZIP4'])

Demo2BL=Demo2[Demo2['PCTBLPOP']>=10]
Demo2OT=Demo2[Demo2['PCTBLPOP']<10]
Demo2BL=Demo2BL[['ZIP_CODE', 'ZIP4']]
Demo2OT=Demo2OT[['ZIP_CODE', 'ZIP4']]
Demo2BL.to_csv("BFVBL.csv",index=False,dtype={'ZIP_CODE':'object','ZIP4':'object'})
Demo2OT.to_csv("BFVOT.csv",index=False,dtype={'ZIP_CODE':'object','ZIP4':'object'})
Demo2.to_csv("DemoZip40323.csv",index=False,dtype={'ZIP_CODE':'object','ZIP4':'object'})
Demo2=pd.read_csv("DemoZip40323.csv",dtype={'ZIP_CODE':'object','ZIP4':'object'})
