# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 17:52:25 2016

@author: Jubaplus
"""

import pandas as pd
import numpy as np

df=pd.read_csv("/Users/jubaplus1/Downloads/IT-HH-ViewershipDayPartsNew.csv",sep="\t")

totalfre=df[['Cable Network ID', 'Program Code', 'Program Name','Telecast Number','Household Number']]
totalfre=totalfre.drop_duplicates()
totalfre=totalfre.groupby(['Household Number','Program Code', 'Program Name']).count()
totalfre.reset_index(inplace=True)  
totalfre=totalfre[['Household Number', 'Program Code', 'Program Name', 'Telecast Number']]
totalfre.columns=['Household Number', 'Program Code', 'Program Name', 'Totalfreq']

df['mins']=df['End Minute of Program Viewing']-df['Start Minute of Program Viewing']
totalmins=df[['Program Code', 'Program Name','mins','Household Number']]
totalmins=totalmins.groupby(['Household Number','Program Code', 'Program Name']).sum()
totalmins.reset_index(inplace=True)  
totalmins.columns=['Household Number', 'Program Code', 'Program Name', 'Totalmins']


hhintab=df[['HH In-tab Weight','Household Number']]
hhintab=hhintab.groupby('Household Number').mean()
hhintab.reset_index(inplace=True)  

totalfre2=df[['Cable Network ID', 'Program Code', 'Program Name','Telecast Number','Household Number']]
totalfre2=totalfre2.drop_duplicates()
totalfre2=totalfre2[['Household Number', 'Telecast Number']]
totalfre2=totalfre2.groupby(['Household Number']).count()
totalfre2.reset_index(inplace=True)  
totalfre2.columns=['Household Number', 'Totalfreq_Allprogram']

totalmins2=df[['mins','Household Number']]
totalmins2=totalmins2.groupby(['Household Number']).sum()
totalmins2.reset_index(inplace=True)  
totalmins2.columns=['Household Number','Totalmins_Allprogram']

hhintab=pd.merge(hhintab,totalfre2,on='Household Number',how='outer')
hhintab=pd.merge(hhintab,totalmins2,on='Household Number',how='outer')
hhintab=hhintab.fillna(0)


df['Time Shifted Viewing Code']=np.where(df['Time Shifted Viewing Code']==1,1,0)
totalfre2=df[['Program Code', 'Program Name','Telecast Number','Household Number','Time Shifted Viewing Code']]
totalfre2=totalfre2.groupby(['Program Code', 'Program Name','Telecast Number','Household Number']).mean()
totalfre2.reset_index(inplace=True)  
totalfre2['Time Shifted Viewing Code']=np.where(totalfre2['Time Shifted Viewing Code']>=0.5,1,0)
totalfre2.columns=['Program Code', 'Program Name','Telecast Number','Household Number','NewShiftedCode']
df=pd.merge(df,totalfre2,on=['Program Code', 'Program Name','Telecast Number','Household Number'])
totalfre2=totalfre2[['Household Number','NewShiftedCode','Telecast Number']]
totalfre2=totalfre2.groupby(['Household Number','NewShiftedCode']).count()
totalfre2.reset_index(inplace=True)  
totalfre2p1=totalfre2[totalfre2['NewShiftedCode']==1]
totalfre2p1=totalfre2p1[['Household Number', 'Telecast Number']]
totalfre2p1.columns=['Household Number', 'Totalfreq_Live']
totalfre2p2=totalfre2[totalfre2['NewShiftedCode']!=1]
totalfre2p2=totalfre2p2[['Household Number', 'Telecast Number']]
totalfre2p2.columns=['Household Number', 'Totalfreq_Shifted']
totalfre2=pd.merge(totalfre2p1,totalfre2p2,on='Household Number',how='outer')
totalfre2=totalfre2.fillna(0)

totalmins2=df[['mins','Household Number','NewShiftedCode']]
totalmins2=totalmins2.groupby(['Household Number','NewShiftedCode']).sum()
totalmins2.reset_index(inplace=True)
totalmins2p1=totalmins2[totalmins2['NewShiftedCode']==1]
totalmins2p1=totalmins2p1[['Household Number', 'mins']]
totalmins2p1.columns=['Household Number', 'Totalmins_Live']
totalmins2p2=totalmins2[totalmins2['NewShiftedCode']!=1]
totalmins2p2=totalmins2p2[['Household Number', 'mins']]
totalmins2p2.columns=['Household Number', 'Totalmins_Shifted']
totalmins2=pd.merge(totalmins2p1,totalmins2p2,on='Household Number',how='outer')
totalmins2=totalmins2.fillna(0)
hhintab=pd.merge(hhintab,totalfre2,on='Household Number',how='outer')
hhintab=pd.merge(hhintab,totalmins2,on='Household Number',how='outer')
hhintab=hhintab.fillna(0)


shiftview=df[['Program Code', 'Program Name','Household Number','NewShiftedCode']]
shiftview=shiftview.drop_duplicates()
shiftview=shiftview.groupby(['Household Number','Program Code', 'Program Name']).mean()
shiftview.reset_index(inplace=True)  
shiftview['NewShiftedCode']=np.where(shiftview['NewShiftedCode']==1,1,
np.where(shiftview['NewShiftedCode']==0,2,3))
shiftview.columns=['Household Number', 'Program Code', 'Program Name', 'ShiftedViewindicator']

shiftview2=df[['Household Number','NewShiftedCode']]
shiftview2=shiftview2.drop_duplicates()
shiftview2=shiftview2.groupby(['Household Number']).mean()
shiftview2.reset_index(inplace=True)  
shiftview2['NewShiftedCode']=np.where(shiftview2['NewShiftedCode']==1,1,
np.where(shiftview2['NewShiftedCode']==0,2,3))
shiftview2.columns=['Household Number','ShiftedViewindicatorallprogram']
hhintab=pd.merge(hhintab,shiftview2,on='Household Number',how='outer')

shiftview2=df[['Household Number','NewShiftedCode']]
shiftview2=shiftview2[shiftview2['NewShiftedCode']==0]
shiftview2=shiftview2.groupby(['Household Number']).count()
shiftview2.reset_index(inplace=True)  
shiftview2.columns=['Household Number','Countshiftedview']
hhintab=pd.merge(hhintab,shiftview2,on='Household Number',how='outer')
hhintab=hhintab.fillna(0)

shiftview2=df[['Household Number','NewShiftedCode']]
shiftview2=shiftview2[shiftview2['NewShiftedCode']==1]
shiftview2=shiftview2.groupby(['Household Number']).count()
shiftview2.reset_index(inplace=True)  
shiftview2.columns=['Household Number','Countliveview']
hhintab=pd.merge(hhintab,shiftview2,on='Household Number',how='outer')
hhintab=hhintab.fillna(0)


allfile=pd.merge(totalfre,totalmins,on=['Household Number', 'Program Code', 'Program Name'])
allfile=pd.merge(allfile,shiftview,on=['Household Number', 'Program Code', 'Program Name'])

df2=pd.DataFrame()
program=df[['Program Code','Program Name']]
program=program.drop_duplicates()
for i in program['Program Code']:
    df2=allfile[allfile['Program Code']==i]
    df2.columns=['Household Number', 'Program Code'+str(i), 'Program Name'+str(i), 'Totalfreq'+str(i),
       'Totalmins'+str(i), 'ShiftedViewindicator'+str(i)]
    hhintab=pd.merge(hhintab,df2,on='Household Number',how='outer')
hhintab=hhintab.fillna(0)

for i in program['Program Code']:
    hhintab['Program Code'+str(i)]=str(i)

for i,j in zip(program['Program Code'],program['Program Name']):
    hhintab['Program Name'+str(i)]=j

hhintab.to_csv("/Users/jubaplus1/Downloads/showaffinitysund.csv",index=False)

print(program)

