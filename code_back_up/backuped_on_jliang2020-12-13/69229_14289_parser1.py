from os import listdir
import pandas as pd
import math
from db import *

path = '/disk2/Projects/Rentrak/20150907-20150920/'
llpath = '/disk2/Projects/Rentrak/zip4_lat_lon_usng.csv'


def getusngzip4dict(llpath):
	usngzip4dict = {}
	with open(llpath) as f:
		next(f)
		for line in f:
			v = line.split('|')
			key = v[-1].replace('\r\n','')
			if  not usngzip4dict.get(key):
				usngzip4dict[key] = []
				#"{0:0>4}".format('123')
			usngzip4dict[key].append((v[0],v[1]))
	return usngzip4dict

def getShowIDToUsngAndRank(showidToFileName):
	v ={} 
	for keys in showidToFileName:
		with open(path+showidToFileName[keys]) as f:
			next(f)
			for line in f:
				vv = line.split(',')
				if not v.get(keys):
					v[keys] = []
				v[keys].append((vv[0],vv[1].replace('\r\n','')))
	return v
		


def getShowidToUsngFileNameAndrefFile():
	showidToFileName = {}
	refFile = None
	result = listdir(path)
	for elements in result:
		if 'txt' in elements:
			v = elements.split('-')[-1].split('.txt')[0]
			showidToFileName[v]  = elements
		elif 'xlsx' in elements:
			refFile = elements
	return showidToFileName, refFile


def getShowidToName (refFile):
	v = pd.read_excel(path+refFile, sheetname='Sheet1')
	showidToName = {}
	c = 0
	while c<len(v):
		#if isinstance(v['series_no'][c], str)
		if not math.isnan(v['series_no'][c]):
			showidToName[str(int(v['series_no'][c]))] = (str(v['network_no'][c]),str(v['title'][c]))
		c+=1
	return showidToName

def creatoutput():
	missing = set()
	showmissing = set()
	showidToFileName, refFile = getShowidToUsngFileNameAndrefFile()
        vv = getShowIDToUsngAndRank(showidToFileName)
	showidToName = getShowidToName(refFile)
	tt = getusngzip4dict(llpath)
	f = open('output.csv','w')
	header = 'ShowID\tNetwork_no\tShowName\tUSNG_100\tZip\tZip4\tRank'+path.split('/')[-2]+'\n'
	f.write(header)
	for key,value in showidToName.iteritems():
		if vv.get(key):
			showIDToUsngAndRank = vv[key]
		
			for elements1 in showIDToUsngAndRank:
				if tt.get(elements1[0]):
					usngzip4list = tt[elements1[0]]
					for elements in usngzip4list:
						#result=key+'\t'+value[0]+'\t'+value[1]+'\t'+elements1[0]+'\t'+elements[0]+'\t'+elements[1]+'\t'+elements1[1]+'\n'
						result = '\t'.join((key,value[0],value[1],elements1[0],elements[0],elements[1],elements1[1]))+'\n'
						f.write(result)	
				else:
					missing.add(elements1[0])
		else:
			showmissing.add(key)
	print 'Show missing'
	print '\n'.join(list(showmissing))
	print 'Zip Missing'
	print '\n'.join(list(missing))
	f.close()
	print 'arnab6'

def dbcall():
	#users = createTable('rentrakoutput11', 'ShowID', 'Network_no', 'ShowName', 'USNG_100', 'Zip', 'Zip4', 'Rank'+path.split('/')[-2])
	users = createTable('rentrakoutput', 'ShowID', 'Network_no', 'ShowName', 'USNG_100', 'Zip', 'Zip4', 'Rank'+path.split('/')[-2])
	print 'Table creation success'
	i = users.insert()
	#v = {'ShowID': '2150186', 'Zip': '96752', 'Rank20150907-20150920': '1', 'Network_no': '410', 'USNG_100': '04QDK263297', 'ShowName': 'Kendra on Top', 'Zip4': '""'}
	#i.execute(v)
        showidToFileName, refFile = getShowidToUsngFileNameAndrefFile()
        vv = getShowIDToUsngAndRank(showidToFileName)
        showidToName = getShowidToName(refFile)
        tt = getusngzip4dict(llpath)
	c = 0
        for key,value in showidToName.iteritems():
                if vv.get(key):
                        showIDToUsngAndRank = vv[key]
                        for elements1 in showIDToUsngAndRank:
                                if tt.get(elements1[0]):
                                        usngzip4list = tt[elements1[0]]
                                        for elements in usngzip4list:
						v = {'ShowID' :key, 'Network_no':value[0],
							'ShowName':value[1], 'USNG_100': elements1[0], 
							'Zip': elements[0], 'Zip4': elements[1],'Rank20150907-20150920': elements1[1]}
						try:
							i.execute(v)
						except Exception as e:
							print 'arnab i was hit'
							i.execute(v)
							pass
        print 'arnab6'
if __name__ == '__main__':
	dbcall()