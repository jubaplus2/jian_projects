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

outputpath = '/home/nielsen/MicroSegment/Engine/Client/WGNA/Chicago Fire/'
network = 'WGNA '
startdate = 20170213
enddate = 20180211
minage = 25
maxage = 54
gender = ['F','M']
show_dic={'ProgramID':[' 822246'],    
         'startdate':['2017-02-13'],
         'enddate':['2018-02-11']}

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
PersonIntab.to_csv(outputpath + 'personintab.csv',index = False)

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

df_ct = df[['HHID_PersonID','ProgramName']].drop_duplicates()
df_ct2 = df_ct.copy()
df_ct = pd.merge(df_ct,df_ct2,on = ['HHID_PersonID'])
df_ct = pd.merge(df_ct,PersonIntab,on = ['HHID_PersonID'])
df_ct2 = df_ct[df_ct['ProgramName_x']==df_ct['ProgramName_y']]
df_ct2 = df_ct2.groupby(['ProgramName_x']).sum()
df_ct2.reset_index(inplace = True)
df_ct = df_ct.groupby(['ProgramName_x','ProgramName_y']).sum().unstack('ProgramName_y')
df_ct.columns = df_ct.columns.get_level_values(1)
df_ct.reset_index(inplace = True)
df_ct = pd.merge(df_ct,df_ct2,on = ['ProgramName_x'])
for i in df_ct.columns[1:(len(df_ct.columns)-1)]:
    df_ct[i+'_pct'] = df_ct[i]/df_ct['PersonIntab']
df_ct.to_csv(outputpath + 'crosstab.csv',index = False)
del df_ct,df_ct2

df=df[['HHID_PersonID','flipfreq','mins']]
df=df.groupby('HHID_PersonID').sum()
df.reset_index(inplace=True)
df=pd.merge(df,PersonIntab[['HHID_PersonID','PersonIntab']],on='HHID_PersonID')
df=df.sort_values('flipfreq',ascending=0)
df=df.reset_index(drop=True)
df=df.reset_index()
df.columns=['freqrank','HHID_PersonID','flipfreq', 'mins','PersonIntab']
df=df.sort_values('mins',ascending=0)
df=df.reset_index(drop=True)
df=df.reset_index()
df.columns=['minsrank','freqrank','HHID_PersonID',  'flipfreq', 'mins','PersonIntab']
rows=len(df.index)
df['Cluster']=np.where(((df['freqrank']<=rows/10)|(df['minsrank']<=rows/10)),'Core',np.where(((df['freqrank']<=rows/(10/3))|(df['minsrank']<=rows/(10/3))),'Occasional','Balance'))
df3 = df[['Cluster','flipfreq', 'mins']].groupby('Cluster').mean()
df=df[['HHID_PersonID','Cluster','PersonIntab']]
df.to_csv(outputpath + 'IDlist.csv',index = False)
df2=df[['PersonIntab','Cluster']].groupby('Cluster').sum()
df2.columns=['core_table']
df3 = pd.concat([df2, df3], axis=1)
df3.to_csv(outputpath + 'corepct.csv')
df2=df2.to_dict(orient='dict')
df['Cluster']=np.where(df['Cluster']=='Occasional','Core',df['Cluster'])
hhidlist='('
for i in df['HHID_PersonID']:
    hhidlist=hhidlist+'"'+i+'",'
hhidlist=hhidlist[0:(len(hhidlist)-1)]  
hhidlist=hhidlist+')'

type30=pd.read_csv(outputpath + 'allprograms.csv',
                   dtype={'NetworkID':'str','ProgramID':'str','TelecastN':'int','EpisodeName':'str'})




dfprogram=pd.read_sql(("select HHID_PersonID,NetworkName,ProgramName,est_season,mins,flipfreq,shareoffreq, shareofmins from programs where HHID_PersonID in %s"% hhidlist),con=engine)
dfmovie=pd.read_sql(("select HHID_PersonID,NetworkName,ProgramName,est_season,mins,flipfreq,shareoffreq, shareofmins from movies where HHID_PersonID in %s"% hhidlist),con=engine)


programdetails=pd.read_csv(outputpath + 'allprograms.csv',dtype = 'str')
programdetails = programdetails[['NetworkName','ProgramName','est_season','ProgramDate','BroadcastStartTime','RepeatIndicator','Programtypesummarycode','EpisodeName']]

programdetails['ProgramName'] = np.where(programdetails['Programtypesummarycode'] =='FF',
                                        'MOVIE:'+programdetails['EpisodeName'],
                                        programdetails['ProgramName'])

programdetails = programdetails[programdetails['RepeatIndicator']=='0']
programdetails['dayofweek'] = pd.to_datetime(programdetails['ProgramDate'])
days = {0:'Mon',1:'Tue',2:'Wed',3:'Thu',4:'Fri',5:'Sat',6:'Sun'}
programdetails['dayofweek'] = programdetails['dayofweek'].dt.dayofweek
programdetails['dayofweek'] = programdetails['dayofweek'].apply(lambda x: days[x])

programdetails = programdetails.sort_values(['ProgramDate',
                                             'BroadcastStartTime'],
                                             ascending = True)
programdetails = programdetails.drop_duplicates(['NetworkName',
                                                 'ProgramName','est_season'])
programdetails['BroadcastDayTime'] = programdetails['dayofweek'] + ' ' + programdetails['BroadcastStartTime'].str[2:4]+':' + programdetails['BroadcastStartTime'].str[4:6]
programdetails['programfinal']=programdetails['NetworkName']+'_'+programdetails['ProgramName']+'_'+programdetails['est_season'].astype('str')
programdetails['moviefinal']=programdetails['NetworkName']+'_'+programdetails['ProgramName']
programdetails = programdetails[['programfinal','moviefinal','BroadcastDayTime']]

dfprogram['programfinal']=dfprogram['NetworkName']+'_'+dfprogram['ProgramName']+'_'+dfprogram['est_season'].astype('str')
dfmovie['moviefinal']=dfmovie['NetworkName']+'_'+dfmovie['ProgramName']


df['Cluster']=np.where(df['Cluster']=='Core','ACore',df['Cluster'])
df2=df[['PersonIntab','Cluster']].groupby('Cluster').sum()
df2.columns=['totalintab']
df2.reset_index(inplace=True)

dmamap={'106' : 'Boston', '101' : 'New York', '202' : 'Chicago', '403' : 'Los Angeles', '407' : 'San Francisco', '104' : 'Philadelphia', '111' : 'Washington D.C.', '105' : 'Detroit', '223' : 'Dallas', '124' : 'Atlanta', '218' : 'Houston', '419' : 'Seattle', '139' : 'Tampa', '353' : 'Phoenix', '110' : 'Cleveland', '213' : 'Minneapolis', '128' : 'Miami-Ft.Lauderdale', '134' : 'Orlando', '462' : 'Sacramento', '209' : 'St.Louis', '112' : 'Baltimore', '108' : 'Pittsburgh', '420' : 'Portland', '117' : 'Charlotte', '351' : 'Denver', '   ' : 'Non Top 25'}


weekrank = {'Sunday': 0,'Monday': 1,'Tuesday': 2,'Wednesday': 3,'Thursday': 4,'Friday': 5,'Saturday': 6}
colheaders = ['1flipfreq_c', '2mins_c', '3shareoffreq_c','4shareofmins_c', '5PersonIntab_c',  '6count_c',
              '1flipfreq_b', '2mins_b', '3shareoffreq_b','4shareofmins_b', '5PersonIntab_b',  '6count_b']
#
varlist=['Network','Dayofweek','Hour','MVPD','DMA','programfinal','moviefinal']
blist=['BOU  ','ESC  ','CW   ','GRT  ','FOX  ','ION  ','COZ  ','ABC  ','MET  ','CBS  ','NBC  ','ETV  ','AZA  ','TEL  ','UNI  ','UMA  ','MMX  ']

for i in varlist:
    j=outputpath+i+'.csv'
    if i=='Network':
        dffile=pd.read_sql(("select HHID_PersonID,Network,mins,flipfreq,shareoffreq, shareofmins from network_dayofweek where HHID_PersonID in %s"% hhidlist),con=engine)
    elif (i=='Dayofweek')|(i=='Hour'):
        dffile=pd.read_sql(("select HHID_PersonID,Dayofweek,Hour,mins,flipfreq,shareoffreq, shareofmins from dayofweek_hour where HHID_PersonID in %s"% hhidlist),con=engine)
        dffile['Hour'] = dffile['Hour'].astype('int')
    elif i=='programfinal':
        dffile=dfprogram.copy()
    elif (i=='MVPD')|(i=='DMA'):
        dffile=pd.read_sql(("select HHID_PersonID,MVPD,DMA,mins,flipfreq,shareoffreq, shareofmins from mvpd_dma where HHID_PersonID in %s"% hhidlist),con=engine)
        dffile['DMA']= dffile['DMA'].apply(lambda x: dmamap[x])
    elif i=='moviefinal':
        dffile=dfmovie.copy()
    print(i)
    dfhhid=pd.merge(dffile,df[['HHID_PersonID','Cluster']],on='HHID_PersonID')
    dfhhid=dfhhid.groupby([i,'HHID_PersonID','Cluster']).sum()
    dfhhid.reset_index(inplace=True)
    dfhhid=dfhhid[[i,'HHID_PersonID','Cluster','flipfreq', 'mins','shareoffreq', 'shareofmins']]
    dfhhid=pd.merge(dfhhid,df[['HHID_PersonID','PersonIntab']],on='HHID_PersonID')
    dfhhid=dfhhid[[i,'Cluster','flipfreq', 'mins','shareoffreq', 'shareofmins','PersonIntab']]
    dfhhid['flipfreq']=dfhhid['flipfreq']*dfhhid['PersonIntab']
    dfhhid['mins']=dfhhid['mins']*dfhhid['PersonIntab']
    dfhhid['shareoffreq']=dfhhid['shareoffreq']*dfhhid['PersonIntab']
    dfhhid['shareofmins']=dfhhid['shareofmins']*dfhhid['PersonIntab']
    dfhhid=dfhhid.groupby([i,'Cluster']).sum()
    dfhhid.reset_index(inplace=True)
    dfhhid=pd.merge(dfhhid,df2,on=['Cluster'])
    dfhhid['count']=dfhhid['PersonIntab']/dfhhid['totalintab']
    dfhhid['flipfreq']=dfhhid['flipfreq']/dfhhid['PersonIntab']
    dfhhid['mins']=dfhhid['mins']/dfhhid['PersonIntab']
    dfhhid['shareoffreq']=dfhhid['shareoffreq']/dfhhid['PersonIntab']
    dfhhid['shareofmins']=dfhhid['shareofmins']/dfhhid['PersonIntab']
    dfhhid.columns=[i, 'Cluster', '1flipfreq', '2mins', '3shareoffreq','4shareofmins', '5PersonIntab', 'totalintab', '6count']
    dfhhidall=pd.pivot_table(dfhhid,index=[i], columns=['Cluster'], values=['1flipfreq', '2mins', '3shareoffreq','4shareofmins', '5PersonIntab', '6count'],aggfunc=np.sum)
    dfhhidall.columns = dfhhidall.columns.swaplevel(0,1)
    dfhhidall = dfhhidall.sort_index(axis=1)
    dfhhidall.columns=dfhhidall.columns.get_level_values(1)
    dfhhidall.reset_index(inplace = True)
    dfhhidall.columns = [i] + colheaders
    dfhhidall['freqindex'] = dfhhidall['1flipfreq_c']/dfhhidall['1flipfreq_b']
    dfhhidall['minsindex'] = dfhhidall['2mins_c']/dfhhidall['2mins_b']
    if i=='programfinal':
        programdf=dfhhidall.copy()
        dfhhidall = pd.merge(dfhhidall,
                             programdetails,
                             on = 'programfinal',how = 'left')
        dfhhidall['Network'] = dfhhidall['programfinal'].apply(lambda x: x.split('_')[0])
        dfhhidall['program'] = dfhhidall['programfinal'].apply(lambda x: x.split('_')[1])
        dfhhidall['season'] = dfhhidall['programfinal'].apply(lambda x: x.split('_')[2])
        dfhhidall['broadcast']=np.where(dfhhidall['Network'].isin(blist),'B','C')
        dfhhidall = dfhhidall.sort_values('freqindex' , ascending = False)
        dfhhidall = dfhhidall[['Network','program','season','broadcast','BroadcastDayTime','freqindex','minsindex'] + colheaders]
    elif i=='moviefinal':
        moviedf=dfhhidall.copy()
        dfhhidall = pd.merge(dfhhidall,
                             programdetails,
                             on = 'moviefinal',how = 'left')
        dfhhidall['Network'] = dfhhidall['moviefinal'].apply(lambda x: x.split('_')[0])
        dfhhidall['program'] = dfhhidall['moviefinal'].apply(lambda x: x.split('_')[1])
        dfhhidall['broadcast']=np.where(dfhhidall['Network'].isin(blist),'B','C')
        dfhhidall = dfhhidall.sort_values('minsindex' , ascending = False)
        dfhhidall = dfhhidall[['Network','program','broadcast','BroadcastDayTime','freqindex','minsindex'] + colheaders]
    elif i =='Dayofweek':
        dfhhidall['rank']= dfhhidall['Dayofweek'].apply(lambda x: weekrank[x])
        dfhhidall = dfhhidall.sort_values('rank' , ascending = True)
        dfhhidall = dfhhidall[[i,'freqindex','minsindex'] + colheaders]
    elif i =='Hour':
        dfhhidall = dfhhidall.sort_values('Hour' , ascending = True)
        dfhhidall = dfhhidall[[i,'freqindex','minsindex'] + colheaders]
    elif i =='Network':
        dfhhidall['broadcast']=np.where(dfhhidall['Network'].isin(blist),'B','C')
        dfhhidall = dfhhidall.sort_values(['broadcast','freqindex'] , ascending = [1,0])
        dfhhidall = dfhhidall[[i,'broadcast','freqindex','minsindex'] + colheaders]
    else:
        dfhhidall = dfhhidall.sort_values('freqindex' , ascending = False)
        dfhhidall = dfhhidall[[i,'freqindex','minsindex'] + colheaders]
    dfhhidall.to_csv(j)

programdf.columns=['programfinal','11flipfreq', '12mins', '13shareoffreq', '14shareofmins', '15PersonIntab','16count','21flipfreq', '22mins', '23shareoffreq', '24shareofmins', '25PersonIntab', '26count',
                   'freqindex','minsindex']
#programdf.reset_index(inplace=True)     
programdf['freqindex']=programdf['11flipfreq']/programdf['21flipfreq']
programdf['minindex']=programdf['12mins']/programdf['22mins']
programdf['network']=programdf['programfinal'].apply(lambda x: x.split('_')[0])
programdf['netgroup']=np.where(programdf['network']==network,1,2)
programdf.groupby('netgroup').count()
programdf=programdf.fillna(0)
programdf=programdf.sort_values(['netgroup','freqindex'],ascending=[1,0])

group=2
dfpall=pd.DataFrame()
for i in range(group):
    print(i)
    dfp=programdf[programdf['netgroup']==(i+1)]
    if i<group-1:
        dfp=dfp.head(25)
        dfp=dfp[['netgroup','programfinal','15PersonIntab']]
        dfp=dfp.reset_index(drop=True)
        dfp=dfp.reset_index()
    else:
        dfp2=dfp[((dfp['freqindex']>=1.5)|(dfp['freqindex']>=1.5))&(dfp['15PersonIntab']>=200000)]
        dfp2['netgroup']=group+1
        dfp2=dfp2[['netgroup','programfinal','15PersonIntab']]
        dfp2=dfp2.reset_index(drop=True)
        dfp2=dfp2.reset_index()
        dfp=dfp.head(25)
        dfp=dfp[['netgroup','programfinal','15PersonIntab']]
        dfp=dfp.reset_index(drop=True)
        dfp=dfp.reset_index()
        dfp=dfp.append(dfp2,ignore_index=True)
    dfpall=dfpall.append(dfp,ignore_index=True)

dfpall=dfpall.sort_values(['netgroup','15PersonIntab'],ascending=[1,0])
dfpall=dfpall[['netgroup','programfinal']]
dfpall=dfpall.reset_index(drop=True)
dfpall=dfpall.reset_index()

HHIDs=df[df['Cluster']=='ACore']
dfhhid=pd.merge(dfprogram,HHIDs,on=['HHID_PersonID'])
dfhhid=dfhhid[['HHID_PersonID','programfinal','PersonIntab']]
dfhhid=dfhhid.drop_duplicates()

dfhhid=pd.merge(dfhhid,dfpall,on=['programfinal'])
dfhhid=dfhhid.sort_values(['netgroup','index'])
dfhhid=dfhhid.drop_duplicates(['netgroup','HHID_PersonID'])
dfhhid=dfhhid[['netgroup','index','programfinal',  'PersonIntab']]
dfhhid=dfhhid.groupby(['netgroup','index','programfinal']).sum()
dfhhid.reset_index(inplace=True)
dfhhid=pd.merge(dfhhid[['programfinal',  'PersonIntab','netgroup']],dfpall,on=['netgroup','programfinal'],how='right')
dfhhid=pd.merge(dfhhid,programdf,on=['programfinal'])
dfhhid=dfhhid.fillna(0)
dfhhid = pd.merge(dfhhid,
                  programdetails,
                  on = 'programfinal',how = 'left')
dfhhid['Network'] = dfhhid['programfinal'].apply(lambda x: x.split('_')[0])
dfhhid['program'] = dfhhid['programfinal'].apply(lambda x: x.split('_')[1])
dfhhid['season'] = dfhhid['programfinal'].apply(lambda x: x.split('_')[2])
dfhhid['broadcast']=np.where(dfhhid['Network'].isin(blist),'B','C')
programlist_id=pd.read_sql(("select ProgramID,ProgramName,NetworkName from programdetails "),con=engine)
programlist_id.columns = ['ProgramID','program','Network']
programlist_id = programlist_id.drop_duplicates(['program','Network'])
dfhhid = pd.merge(dfhhid,programlist_id,on = ['program','Network'],how='left')
dfhhid = dfhhid[['Network','program','season','broadcast','BroadcastDayTime','netgroup_x','index','freqindex','minsindex',
                 '11flipfreq', '12mins', '13shareoffreq', '14shareofmins', '15PersonIntab','16count',
                 'PersonIntab','21flipfreq', '22mins', '23shareoffreq', '24shareofmins', '25PersonIntab', '26count',
                 'programfinal','ProgramID']]
dfhhid = dfhhid.sort_values(['netgroup_x','index'], ascending = True)
dfhhid.to_csv(outputpath+'programcounts.csv',index=False)

###removebroadcast
blist=['BOU  ','ESC  ','CW   ','GRT  ','FOX  ','ION  ','COZ  ','ABC  ','MET  ','CBS  ','NBC  ','ETV  ','AZA  ','TEL  ','UNI  ','UMA  ','MMX  ']
programdf['Network']=np.where(programdf['network'].isin(blist),'B','C')
programdf=programdf[programdf['Network']=='C']
programdf=programdf[programdf.columns[0:17]]

list2=dfhhid[(dfhhid['index']<=5)|((dfhhid['index']<=55)&(dfhhid['index']>=50))]
list2=list2[['programfinal']].drop_duplicates()

###remove all the broadcastshows
#dfprogram=dfprogram[~dfprogram['NetworkName'].isin(blist)]

programdetail=pd.read_sql(("select * from programdetails"),con=engine)
excludeprogram=programdetail[programdetail['Programtypesummarycode'].isin(['SC','SE','GV'])]
excludeprogram=excludeprogram[['ProgramName','Programtypesummarycode']]
excludeprogram=excludeprogram.drop_duplicates()

programlist=pd.merge(dfprogram,HHIDs,on=['HHID_PersonID'])
programlist=pd.merge(programlist,excludeprogram,on=['ProgramName'],how='left')
programlist['Programtypesummarycode'].fillna('Other',inplace=True)
programlist=programlist[programlist['Programtypesummarycode']=='Other']
programlist=programlist[['flipfreq','programfinal']]
programlist=programlist.groupby(['programfinal']).sum()
programlist.reset_index(inplace=True)
programlist=programlist.sort_values('flipfreq',ascending=0)
programlist=programlist.reset_index(drop=True)
programlist=programlist.reset_index()
programlist=programlist.head(40)
programlist=programlist[['programfinal']]
programlist=programlist.append(list2,ignore_index=True)
programlist=programlist.drop_duplicates()
programlist1=programlist['programfinal']
programlist=pd.merge(dfprogram,programlist,on=['programfinal'])
programlist=pd.merge(programlist,HHIDs,on=['HHID_PersonID'])
programlist=programlist.drop_duplicates()
programlist.to_csv(outputpath+'programlist.csv',index=False)
programlist
###get movies here
#moviedf.columns=moviedf.columns.get_level_values(1)
moviedf.columns=['moviefinal','11flipfreq', '12mins', '13shareoffreq', '14shareofmins', '15PersonIntab','16count','21flipfreq', '22mins', '23shareoffreq', '24shareofmins', '25PersonIntab', '26count',
                'freqindex','minsindex']
#moviedf.reset_index(inplace=True)     
moviedf['freqindex']=moviedf['11flipfreq']/moviedf['21flipfreq']
moviedf['minindex']=moviedf['12mins']/moviedf['22mins']
moviedf['network']=moviedf['moviefinal'].apply(lambda x: x.split('_')[0])
moviedf['netgroup']=np.where(moviedf['network']==network,1,2)
moviedf.groupby('netgroup').count()
moviedf=moviedf.fillna(0)

moviedf=moviedf.sort_values(['netgroup','minindex'],ascending=[1,0])
group=2
dfpall=pd.DataFrame()
for i in range(group):
    print(i)
    dfp=moviedf[moviedf['netgroup']==(i+1)]
    if i<group-1:
        dfp=dfp.head(25)
        dfp=dfp[['netgroup','moviefinal','15PersonIntab']]
        dfp=dfp.reset_index(drop=True)
        dfp=dfp.reset_index()
    else:
        dfp2=dfp[((dfp['freqindex']>=1.5)|(dfp['freqindex']>=1.5))&(dfp['15PersonIntab']>=200000)]
        dfp2['netgroup']=group+1
        dfp2=dfp2[['netgroup','moviefinal','15PersonIntab']]
        dfp2=dfp2.reset_index(drop=True)
        dfp2=dfp2.reset_index()
        dfp=dfp.head(25)
        dfp=dfp[['netgroup','moviefinal','15PersonIntab']]
        dfp=dfp.reset_index(drop=True)
        dfp=dfp.reset_index()
        dfp=dfp.append(dfp2,ignore_index=True)
    dfpall=dfpall.append(dfp,ignore_index=True)

dfpall=dfpall.sort_values(['netgroup','15PersonIntab'],ascending=[1,0])
dfpall=dfpall[['netgroup','moviefinal']]
dfpall=dfpall.reset_index(drop=True)
dfpall=dfpall.reset_index()

HHIDs=df[df['Cluster']=='ACore']
dfhhid=pd.merge(dfmovie,HHIDs,on=['HHID_PersonID'])
dfhhid=dfhhid[['HHID_PersonID','moviefinal','PersonIntab']]
dfhhid=dfhhid.drop_duplicates()

dfhhid=pd.merge(dfhhid,dfpall,on=['moviefinal'])
dfhhid=dfhhid.sort_values(['netgroup','index'])
dfhhid=dfhhid.drop_duplicates(['netgroup','HHID_PersonID'])
dfhhid=dfhhid[['netgroup','index','moviefinal',  'PersonIntab']]
dfhhid=dfhhid.groupby(['netgroup','index','moviefinal']).sum()
dfhhid.reset_index(inplace=True)
dfhhid=pd.merge(dfhhid[['moviefinal',  'PersonIntab','netgroup']],dfpall,on=['netgroup','moviefinal'],how='right')
dfhhid=pd.merge(dfhhid,moviedf,on=['moviefinal'])
dfhhid=dfhhid.fillna(0)
dfhhid = pd.merge(dfhhid,
                  programdetails,
                  on = 'moviefinal',how = 'left')
dfhhid['Network'] = dfhhid['moviefinal'].apply(lambda x: x.split('_')[0])
dfhhid['program'] = dfhhid['moviefinal'].apply(lambda x: x.split('_')[1])
dfhhid['broadcast']=np.where(dfhhid['Network'].isin(blist),'B','C')

programlist_id=pd.read_sql(("select ProgramID,EpisodeName,NetworkName from programdetails where Programtypesummarycode='FF'"),con=engine)
programlist_id.columns = ['ProgramID','program','Network']
programlist_id['program'] = "MOVIE:" + programlist_id['program']
programlist_id = programlist_id.drop_duplicates(['program','Network'])
dfhhid = pd.merge(dfhhid,programlist_id,on = ['program','Network'],how='left')

dfhhid = dfhhid[['Network','program','broadcast','BroadcastDayTime','netgroup_x','index','freqindex','minsindex',
                 '11flipfreq', '12mins', '13shareoffreq', '14shareofmins', '15PersonIntab','16count',
                 'PersonIntab','21flipfreq', '22mins', '23shareoffreq', '24shareofmins', '25PersonIntab', '26count','ProgramID']]
dfhhid = dfhhid.sort_values(['netgroup_x','index'], ascending = True)
dfhhid.to_csv(outputpath+'moviecounts.csv',index=False)

pdemo = pd.merge(dfprogram,df[['HHID_PersonID','Cluster','PersonIntab']],on='HHID_PersonID')
dfprogramcount = pd.read_csv(outputpath+'programcounts.csv',dtype = 'str')
for i in ['Gender','SVODSubscription',
          'AmazonPrimeSubscription', 'HuluSubscription',
          'NetflixSubscription']:
    pdemo2 = pd.merge(pdemo,persons[['HHID_PersonID',i]],on = 'HHID_PersonID')
    pdemo2['Cluster'] = pdemo2['Cluster']+'_' + i +'_'+ pdemo2[i]
    pdemo2['Network'] = pdemo2['programfinal'].apply(lambda x: x.split('_')[0])
    pdemo2['program'] = pdemo2['programfinal'].apply(lambda x: x.split('_')[1])
    pdemo2['season'] = pdemo2['programfinal'].apply(lambda x: x.split('_')[2])
    pdemo2 = pdemo2[['Network','program','season','Cluster','PersonIntab']].groupby(['Network','program','season','Cluster']).sum().unstack('Cluster')
    pdemo2.columns = pdemo2.columns.get_level_values(1)
    pdemo2.reset_index(inplace = True)
    dfprogramcount = pd.merge(dfprogramcount,pdemo2,
                              on = ['Network','program','season'],how='left')
dfprogramcount['index'] =  dfprogramcount['index'].astype('int')
dfprogramcount = dfprogramcount.sort_values(['netgroup_x','index'], ascending = True)
dfprogramcount.to_csv(outputpath+'programcounts2.csv',index = False)

pdemo = pd.merge(dfmovie,df[['HHID_PersonID','Cluster','PersonIntab']],on='HHID_PersonID')
pdemo = pdemo.drop_duplicates(['HHID_PersonID','moviefinal','Cluster'])
dfprogramcount = pd.read_csv(outputpath+'moviecounts.csv',dtype = 'str')
for i in ['Gender','SVODSubscription',
          'AmazonPrimeSubscription', 'HuluSubscription',
          'NetflixSubscription']:
    pdemo2 = pd.merge(pdemo,persons[['HHID_PersonID',i]],on = 'HHID_PersonID')
    pdemo2['Cluster'] = pdemo2['Cluster']+'_' + i +'_'+ pdemo2[i]
    pdemo2['Network'] = pdemo2['moviefinal'].apply(lambda x: x.split('_')[0])
    pdemo2['program'] = pdemo2['moviefinal'].apply(lambda x: x.split('_')[1])
    pdemo2 = pdemo2[['Network','program','Cluster','PersonIntab']].groupby(['Network','program','Cluster']).sum().unstack('Cluster')
    pdemo2.columns = pdemo2.columns.get_level_values(1)
    pdemo2.reset_index(inplace = True)
    dfprogramcount = pd.merge(dfprogramcount,pdemo2,
                              on = ['Network','program'],how='left')
dfprogramcount['index'] =  dfprogramcount['index'].astype('int')
dfprogramcount = dfprogramcount.sort_values(['netgroup_x','index'], ascending = True)                             
dfprogramcount.to_csv(outputpath+'moviecounts2.csv',index=False)






















                                                                
