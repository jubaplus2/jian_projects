# -*- coding: utf-8 -*-
"""
Created on Fri Jan  5 10:55:57 2018

@author: Jubaplus
"""
import pandas as pd
import numpy as np
import gc
from scipy import stats

projectname = 'Campbells'
minage = 25
maxage = 54
gender = ['F','M']
svodfilter = ['Y','N']
cluster_core = [2,4,5,6]
cluster_balance = [3]
folderpath = '/home/nielsen/MicroSegment/Code/Crosstab/TV1/Campbells/'
pathdemo = '/home/nielsen/MicroSegment/Outputs/Demo/Demographicpersonlevel_160101_180114.csv'
pathcluster = '/home/nielsen/MicroSegment/Code/Crosstab/TV1/Campbells/Campbells_cluster.csv'

###code start here do not change

df_demo = pd.read_csv(pathdemo,dtype = 'str')
df_demo['HHID_PersonID'] = df_demo['HHID'] + '_' + df_demo['PersonID']
df_cluster = pd.read_csv(pathcluster)

df_cluster = df_cluster[df_cluster['Cluster'].isin(cluster_core+cluster_balance)]
df_cluster['FinalCluster'] = np.where(df_cluster['Cluster'].isin(cluster_core),'Core','Balance')
df_cluster['FinalCluster2'] = np.where(df_cluster['Cluster'].isin(cluster_core),1,0)

df_cluster = pd.merge(df_cluster[['HHID_PersonID','FinalCluster','FinalCluster2']],
                      df_demo,on = 'HHID_PersonID')

df_cluster['Age'] = df_cluster['Age'].astype('int')
df_cluster = df_cluster[df_cluster['Age'].isin(range(minage,(maxage+1)))]
df_cluster = df_cluster[df_cluster['Gender'].isin(gender)]
df_cluster['Age'] = df_cluster['Age'].astype('str')
df_cluster['Age1'] = df_cluster['Age'].str[0:1]
df_cluster['Age2'] = df_cluster['Age'].str[1:2]
df_cluster['Age2'] = df_cluster['Age2'].astype('int')
df_cluster['Age'] = df_cluster['Age'].astype('int')
df_cluster['AgeCategory'] = np.where(df_cluster['Age']<10,'2-9',
                                    np.where(df_cluster['Age2']<=4,df_cluster['Age1'] +'0-'+df_cluster['Age1']+'4',
                                            df_cluster['Age1'] +'5-'+df_cluster['Age1']+'9'))

del df_cluster['Age1']
del df_cluster['Age2']
del df_demo
gc.collect()
df_cluster = df_cluster[df_cluster['SVODSubscription'].isin(svodfilter)]

cols_hhid = ['GeoCode', 'TimeZone',
       'CountySize', 'Race', 'Language', 'PresenceofChildren',
       'Children<2', 'Children<3', 'Children2to5', 'Children<6',
       'Children6to11', 'Children12to17', 'NumberofPersonsKids<3',
       'NumberofPersonsChildren', 'NumberofPersonsAdult', 'HHSize',
       'IncomeRange', 'NumberofIncomes', 'HeadHHAgeBreak', 'HeadHHAge',
       'Education', 'Occupation', 'LadyPresentFlag', 'LadyOccupation',
       'WiredCable', 'PayChannel', 'CablePlus', 'AltDelivery',
       'WiredDigitalCable', 'DBSOwner', 'DVDOwner', 'DVR', 'NumberofTV',
       'TVwithPay', 'TVwithCable', 'TVwithPayandCable', 'NumberofVCR',
       'VideoGame',  'HHOrigin', 'HHHispanicEthnicity',
       'NumberofDVR', 'CableviaTelco', 'NumberofCars', 'NumberofTrucks',
       'NewCarLast3Years', 'NewCarLast5Years', 'NewTruckLast3Years',
       'NewTruckLast5Years', 'DomesticVehicle', 'ForeignVehicle',
       'LongDistanceCarrier', 'PCAccessInternet', 'IncomeRangeDetails',
       'HHIncomeAmount', 'HHIncomeNonWorking', 'HHGender',
       'NumberofSmartphone', 'BroadbandOnlyHousehold',
       'InternetCapableDigitalDevice', 'OTTCapableHousehold',
       'HouseholdwithInternetEnabledSmartTV',
       'HouseholdwithEnabledInternetConnectedDevice',
       'HouseholdwithInternetConnectedDevice',
        'LPMFlag', 'LPMCode',
       'SVODSubscription', 'AmazonPrimeSubscription', 'HuluSubscription',
       'NetflixSubscription']

cols_personid = ['Gender', 'Visitor_Status_Code',
       'Principal_Shopper', 'Working_Women_Full_Time_Flag',
       'Working_Women_Part_Time_Flag', 'Language_Class_Code',
       'Frequent_Moviegoer_Code', 'Avid_Moviegoer_Code',
       'Lady_of_Household_Flag', 'Head_of_Household_Flag',
       'Relationship_to_Head_of_Household_Code', 'Nielsen_Occupation_Code',
       'Works_Outside_of_Home_Flag', 'Principal_Moviegoer_Code',
       'Education_Ranges', 'AgeCategory','Age',
       'Age/Gender_Building_Block_Code', 'MVPD']

df_hhid = df_cluster[['HHID','FinalCluster','FinalCluster2']+cols_hhid].drop_duplicates('HHID')
df_personid = df_cluster[['HHID_PersonID','FinalCluster','FinalCluster2']+cols_personid].drop_duplicates('HHID_PersonID')

df_hhid.reset_index(drop = True, inplace = True)
df_personid.reset_index(drop = True, inplace = True)
sumbalance_personid = len(df_personid['HHID_PersonID'][df_personid['FinalCluster']=='Balance'])
sumcore_personid = len(df_personid['HHID_PersonID'][df_personid['FinalCluster']!='Balance'])
sumbalance_hhid = len(df_hhid['HHID'][df_hhid['FinalCluster']=='Balance'])
sumcore_hhid = len(df_hhid['HHID'][df_hhid['FinalCluster']!='Balance'])

df_map = pd.read_csv('/home/nielsen/MicroSegment/Code/Crosstab/demomapv4.csv',dtype = 'str')
df_map['key'] = np.where(df_map['group'].isin(['IncomeRangeDetails','LadyOccupation','IncomeRange',
                                              'Relationship_to_Head_of_Household_Code',
                                              'Nielsen_Occupation_Code']),
                        df_map['key'].str.zfill(2),df_map['key'])

map_groups = list(df_map['group'].unique())
writer = pd.ExcelWriter(folderpath+'crosstab_'+projectname+'.xlsx',
                        engine='xlsxwriter',
                        datetime_format='yyyy-mm-dd',
                        date_format='yyyy-mm-dd')

workbook  = writer.book

def get_colchart(i,tabname,row_s,rot_e,maxpct,numgroup):
    if i =='Gender':
        chart1 = workbook.add_chart({'type': 'column'})
        chart1.add_series({
                    'name':       'Female',
                    'categories': [tabname, row_s-1, 2, row_s-1, 3],
                    'values':     [tabname, row_s, 4, row_s, 5],
                    'data_labels': {'value': True,'font':  {'name': 'Century Gothic'}},
                    'fill':   {'color': '#FF0000'},
                     })
        chart1.add_series({
                    'name':       'Male',
                    'categories': [tabname, row_s-1, 2, row_s-1, 3],
                    'values':     [tabname, rot_e, 4, rot_e, 5],
                    'data_labels': {'value': True,'font':  {'name': 'Century Gothic'}},
                    'fill':   {'color': '#4472C4'}})
        chart1.set_legend({'position': 'bottom',
                        'font':  {'name': 'Century Gothic','bold':False}})
        chart1.set_x_axis({'num_font':  {'name': 'Century Gothic','bold':False}})
    elif i =='AgeCategory':
        chart1 = workbook.add_chart({'type': 'line'})
        chart1.add_series({
                        'name':       'Core',
                        'categories': [tabname, row_s, 11, rot_e, 11],
                        'values':     [tabname, row_s, 4, rot_e, 4],
                        'line':   {'color': '#4472C4'},
                         })
        chart1.add_series({
                        'name':       'Balance',
                        'categories': [tabname, row_s, 11, rot_e, 11],
                        'values':     [tabname, row_s, 5, rot_e, 5],
                        'line':   {'color': '#DAE3F3'}})
        chart1.set_legend({'position': 'bottom',
                            'font':  {'name': 'Century Gothic','bold':False}})
        chart1.set_x_axis({'num_font':  {'name': 'Century Gothic','bold':False}})
    elif i =='MVPD':
        chart1 = workbook.add_chart({'type': 'pie'})
        chart1.add_series({
                        'name':       'Core',
                        'categories': [tabname, row_s, 11, rot_e, 11],
                        'values':     [tabname, row_s, 4, rot_e, 4],
                        'data_labels': {'value': True,'font':  {'name': 'Century Gothic'}},
                         })
    elif numgroup>1:
        chart1 = workbook.add_chart({'type': 'column'})
        chart1.add_series({
                        'name':       'Core',
                        'categories': [tabname, row_s, 11, rot_e, 11],
                        'values':     [tabname, row_s, 4, rot_e, 4],
                        'data_labels': {'value': True,'font':  {'name': 'Century Gothic'}},
                        'fill':   {'color': '#4472C4'},
                         })
        chart1.add_series({
                        'name':       'Balance',
                        'categories': [tabname, row_s, 11, rot_e, 11],
                        'values':     [tabname, row_s, 5, rot_e, 5],
                        'data_labels': {'value': True,'font':  {'name': 'Century Gothic'}},
                        'fill':   {'color': '#DAE3F3'}})
        chart1.set_legend({'position': 'bottom',
                            'font':  {'name': 'Century Gothic','bold':False}})
        chart1.set_x_axis({'num_font':  {'name': 'Century Gothic','bold':False}})
    else:
        chart1 = workbook.add_chart({'type': 'column'})
        chart1.add_series({
                    'name':       'Core',
                    'categories': [tabname, row_s, 11, rot_e, 11],
                    'values':     [tabname, row_s, 4, rot_e, 4],
                    'overlap':    -100,
                    'data_labels': {'value': True,'font':  {'name': 'Century Gothic'}},
                    'fill':   {'color': '#4472C4'},
                     })
        chart1.add_series({
                    'name':       'Balance',
                    'categories': [tabname, row_s, 11, rot_e, 11],
                    'values':     [tabname, row_s, 5, rot_e, 5],
                    'overlap':    -100,
                    'data_labels': {'value': True,'font':  {'name': 'Century Gothic'}},
                    'fill':   {'color': '#DAE3F3'}})
        chart1.set_legend({'position': 'bottom',
                        'font':  {'name': 'Century Gothic','bold':False}})
        chart1.set_x_axis({'label_position':  'none'})
    worksheet = writer.sheets[tabname]
    chart_name = i.replace('_',' ')
    chart_name = chart_name.replace('Flag','')
    chart_name = chart_name.replace('Code','')
    if i =='AgeCategory':
        chart1.set_title ({'name': chart_name,
                           'name_font':  {'name': 'Century Gothic',
                           'bold':False,'size':14}})
        chart1.set_y_axis({'min': 0,'max':maxpct,
                           'major_gridlines' :{'visible': False},
                           'num_format': '0%'})
    else:
        chart1.set_title ({'name': chart_name,
                           'name_font':  {'name': 'Century Gothic',
                           'bold':False,'size':14}})
        chart1.set_y_axis({'min': 0,'max':maxpct,
                            'visible': False,
                            'major_gridlines' :{'visible': False}})
    chart1.set_size({'width': 560, 'height': 400}) 
    return chart1

###personlevel
startrow = 1
startrow2 = 1
tabname = 'crosstab_personlevel'
for i in cols_personid:
    uni_values = list(df_personid[i].unique())
    list.sort(uni_values)
    count_values = len(uni_values)
    uni_valuedict = {}
    vs_count = 0
    for vs in uni_values:
         uni_valuedict[vs] = vs_count
         vs_count = vs_count+1
    if count_values > 1:
        df = df_personid[['HHID_PersonID','FinalCluster','FinalCluster2',i]]
        dfsum = df[['HHID_PersonID',i,'FinalCluster']].groupby([i,'FinalCluster']).count().unstack('FinalCluster')
        dfsum.columns = dfsum.columns.get_level_values(1)
        dfsum.reset_index(inplace = True)
        dfsum = dfsum[[i,'Core','Balance']]
        dfsum.fillna(0,inplace = True)
        dfsum['%Core'] = dfsum['Core']/sumcore_personid
        dfsum['%Balance'] = dfsum['Balance']/sumbalance_personid
        dfsum['Diff A=(%Core-%Balance)/%Core'] = (dfsum['%Core'] - dfsum['%Balance'])/dfsum['%Core']
        dfsum['Diff B= %Core - %Balance'] = (dfsum['%Core'] - dfsum['%Balance'])
        dfsum['|Diff A|>10% & |Diff B|>8% Index'] = np.where((dfsum['Diff A=(%Core-%Balance)/%Core'].abs()>0.1)&\
                                                            (dfsum['Diff B= %Core - %Balance'].abs()>0.08),1,0)
        df['values'] = df[i].apply(lambda x:uni_valuedict[x])
        list1 = list(df['values'][df['FinalCluster']=='Balance'])
        list2 = list(df['values'][df['FinalCluster']=='Core'])
        listdict = {}
        for vs in uni_values:
            listdict[vs] = list(df['FinalCluster2'][df[i]== vs])
        p1 = stats.f_oneway(list1,list2).pvalue
        p2 = stats.f_oneway(*listdict.values()).pvalue
        dfsum['pvalue_groupbybalancecore'] = p1
        dfsum['pvalue_groupbyvalue'] =  p2
        if i in map_groups:
            df_map1 = df_map[df_map['group'] == i]
            df_map1 = df_map1[['key','value']]
            df_map1.columns = [i,'describe']
            dfsum = pd.merge(dfsum,df_map1,on =[i],how = 'left')
        else:
            dfsum['describe'] = dfsum[i]
        if (('Y' in uni_values)&(count_values==2))|((i in ['AgeCategory','Gender'])):
            dfsum = dfsum.copy()
        else:
            dfsum = dfsum.sort_values('%Core',ascending = False)
            dfsum.reset_index(drop = True, inplace = True)
        dfsum['pvalue'] = None
        for index_1 in range(count_values):
            dfsum['pvalue'][index_1] = stats.t.sf(np.abs((dfsum['%Core'][index_1]-dfsum['%Balance'][index_1])/np.sqrt(1.0/dfsum['Core'][index_1]+1.0/dfsum['Balance'][index_1])), dfsum['Core'][index_1]+dfsum['Balance'][index_1]-2)*2 
        dfsum.to_excel(writer,tabname, index=False, startrow=startrow,startcol = 1)        
        ###add chart here
        if i!='MVPD':
            count_values2 = len(list(dfsum['%Core'][dfsum['%Core']>=0.05]))
        else:
            count_values2 = count_values
        if ('Y' in uni_values)&(count_values==2):
            row_s = startrow+2
            rot_e = startrow+count_values
        else:
            row_s = startrow+1
            rot_e = startrow+count_values2
        worksheet = writer.sheets[tabname]
        if (p1<=0.05)|(p2<=0.05):
            maxpct = round(max(max(dfsum['%Core']),max(dfsum['%Balance'])) +0.1,1)
            if ('Y' in uni_values)&(count_values==2):
                chart1 = get_colchart(i,tabname,row_s,rot_e,maxpct,1)       
                worksheet.insert_chart('N'+str(startrow2), chart1)
                startrow2 = startrow2 +20
            elif i in ['Language_Class_Code',
                       'Relationship_to_Head_of_Household_Code', 'Nielsen_Occupation_Code',
                       'Education_Ranges']:
                for ncharts in range(count_values2):
                    chart1 = get_colchart((i +' ' + dfsum['describe'][ncharts]),tabname,row_s,row_s,maxpct,1)
                    row_s = row_s+1
                    worksheet.insert_chart('N'+str(startrow2), chart1)
                    startrow2 = startrow2 +20                
            else:
                chart1 = get_colchart(i,tabname,row_s,rot_e,maxpct,count_values2)      
                worksheet.insert_chart('N'+str(startrow2), chart1)
                startrow2 = startrow2 +20
        startrow = startrow+count_values+2

format1 = workbook.add_format({'num_format': '0%'})
worksheet.set_column('E:H', 10, format1)
worksheet.set_column('J:K', 10, format1)

###HHlevel
startrow = 1
startrow2 = 1
tabname = 'crosstab_hhlevel'
for i in cols_hhid:
    uni_values = list(df_hhid[i].unique())
    list.sort(uni_values)
    count_values = len(uni_values)
    uni_valuedict = {}
    vs_count = 0
    for vs in uni_values:
         uni_valuedict[vs] = vs_count
         vs_count = vs_count+1
    if count_values > 1:
        df = df_hhid[['HHID','FinalCluster','FinalCluster2',i]]
        dfsum = df[['HHID',i,'FinalCluster']].groupby([i,'FinalCluster']).count().unstack('FinalCluster')
        dfsum.columns = dfsum.columns.get_level_values(1)
        dfsum.reset_index(inplace = True)
        dfsum = dfsum[[i,'Core','Balance']]
        dfsum.fillna(0,inplace = True)
        dfsum['%Core'] = dfsum['Core']/sumcore_hhid
        dfsum['%Balance'] = dfsum['Balance']/sumbalance_hhid
        dfsum['Diff A=(%Core-%Balance)/%Core'] = (dfsum['%Core'] - dfsum['%Balance'])/dfsum['%Core']
        dfsum['Diff B= %Core - %Balance'] = (dfsum['%Core'] - dfsum['%Balance'])
        dfsum['|Diff A|>10% & |Diff B|>8% Index'] = np.where((dfsum['Diff A=(%Core-%Balance)/%Core'].abs()>0.1)&\
                                                            (dfsum['Diff B= %Core - %Balance'].abs()>0.08),1,0)
        df['values'] = df[i].apply(lambda x:uni_valuedict[x])
        list1 = list(df['values'][df['FinalCluster']=='Balance'])
        list2 = list(df['values'][df['FinalCluster']=='Core'])
        listdict = {}
        for vs in uni_values:
            listdict[vs] = list(df['FinalCluster2'][df[i]== vs])
        p1 = stats.f_oneway(list1,list2).pvalue
        p2 = stats.f_oneway(*listdict.values()).pvalue
        dfsum['pvalue_groupbybalancecore'] = p1
        dfsum['pvalue_groupbyvalue'] =  p2
        if i in map_groups:
            df_map1 = df_map[df_map['group'] == i]
            df_map1 = df_map1[['key','value']]
            df_map1.columns = [i,'describe']
            dfsum = pd.merge(dfsum,df_map1,on =[i],how = 'left')
        else:
            dfsum['describe'] = dfsum[i]
        if (('Y' in uni_values)&(count_values==2))|('Numberof' in i)|('TVwith' in i)|('HHSize' in i):
            dfsum = dfsum.copy()
        else:
            dfsum = dfsum.sort_values('%Core',ascending = False)
            dfsum.reset_index(drop = True, inplace = True)
        dfsum['pvalue'] = None
        for index_1 in range(count_values):
            dfsum['pvalue'][index_1] = stats.t.sf(np.abs((dfsum['%Core'][index_1]-dfsum['%Balance'][index_1])/np.sqrt(1.0/dfsum['Core'][index_1]+1.0/dfsum['Balance'][index_1])), dfsum['Core'][index_1]+dfsum['Balance'][index_1]-2)*2 
        dfsum.to_excel(writer,tabname, index=False, startrow=startrow,startcol = 1)        
        ###add chart here
        if ('Numberof' in i)|('TVwith' in i)|('HHSize' in i):
            count_values2 = count_values
        elif i!='LPMCode':
            count_values2 = len(list(dfsum['%Core'][dfsum['%Core']>=0.05]))
        else:
            count_values2 = len(list(dfsum['%Core'][dfsum['%Core']>=0.02]))
        if (('Y' in uni_values)&(count_values==2)):
            row_s = startrow+2
            rot_e = startrow+count_values
        elif (i=='LPMCode'):
            row_s = startrow+2
            rot_e = startrow+count_values2
        else:
            row_s = startrow+1
            rot_e = startrow+count_values2
        worksheet = writer.sheets[tabname]
        if (p1<=0.05)|(p2<=0.05)|(i=='LPMCode'):
            if i=='LMPCode':
                maxpct = 0.2
            else:
                maxpct = round(max(max(dfsum['%Core']),max(dfsum['%Balance'])) +0.1,1)
            if ('Y' in uni_values)&(count_values==2):
                chart1 = get_colchart(i,tabname,row_s,rot_e,maxpct,1)       
                worksheet.insert_chart('N'+str(startrow2), chart1)
                startrow2 = startrow2 +20            
            elif ('Numberof' in i)|('TVwith' in i)|('HHSize' in i)|(i=='LPMCode')|(i=='Race'):
                chart1 = get_colchart(i,tabname,row_s,rot_e,maxpct,count_values2)      
                worksheet.insert_chart('N'+str(startrow2), chart1)
                startrow2 = startrow2 +20
            else:
                for ncharts in range(count_values2):
                    chart1 = get_colchart((i +' ' + dfsum['describe'][ncharts]),tabname,row_s,row_s,maxpct,1)
                    row_s = row_s+1
                    worksheet.insert_chart('N'+str(startrow2), chart1)
                    startrow2 = startrow2 +20    
        startrow = startrow+count_values+2

format1 = workbook.add_format({'num_format': '0%'})
worksheet.set_column('E:H', 10, format1)
worksheet.set_column('J:K', 10, format1)

df_cluster.to_excel(writer,'demo', index=False)
writer.save()
