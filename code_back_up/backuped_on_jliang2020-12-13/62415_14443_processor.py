from os import listdir
import pandas as pd
import math
from db import *
import subprocess

path = '/disk2/Projects/Rentrak/20150907-20150920/'
cpath = '/disk2/Projects/Rentrak/20150921-20151004/'
llpath = '/disk2/Projects/Rentrak/zip4_lat_lon_usng.csv'
updatedfile = '/disk2/Projects/Rentrak/Mediastorm_USNG_tile_query_updated_with_geography.txt'
ReferenceFile =  'Mediastorm_showlist_xref_20150907-20150920.xlsx'
usngzip4dict = {}
#showlist = [1938475,1925577,2293230,1894187,1887065,2366514,2089348,2331240,2276769]

#Format is zip|zip4|lat|lon|usng_tile
#01001|0004|42.06582|-72.62216|18TXM967598
def getusngzip4dict(llpath):
	global usngzip4dict
	with open(llpath) as f:
		next(f)
		for line in f:
			v = line.split('|')
			key = v[-1].replace('\r\n','')
			if  not usngzip4dict.get(key):
				usngzip4dict[key] = []
				#"{0:0>4}".format('123')
			usngzip4dict[key].append((v[0],v[1]))

#Fromat is USNG_tile,lat,long,zip,zip4
#04QDK448245,21.923901,-159.534485,96741,9795
def getupdatedusngzip(updatedfile):
	global usngzip4dict
        with open(updatedfile) as f:
                next(f)
                for line in f:
                        v = line.split(',')
                        key = v[0]
                        if  not usngzip4dict.get(key):
                                usngzip4dict[key] = []
                        usngzip4dict[key].append((v[-2],v[-1].replace('\r\n','')))

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
	#refFile =  'Mediastorm_showlist_xref_20150907-20150920.xlsx'
	refFile = ReferenceFile 
	return showidToFileName, refFile

#For the ref file only
#Currently ref file is present in /disk2/Projects/Rentrak/20150907-20150920/
def getShowidToName (refFile):
	lpath = '/disk2/Projects/Rentrak/20150907-20150920/'
	v = pd.read_excel(lpath+refFile, sheetname='Sheet1')
	showidToName = {}
	c = 0
	while c<len(v):
		#if isinstance(v['series_no'][c], str)
		if not math.isnan(v['series_no'][c]):
			showidToName[str(int(v['series_no'][c]))] = (str(v['network_no'][c]),str(v['title'][c]))
		c+=1
	return showidToName


def run():
	showidToFileName, refFile = getShowidToUsngFileNameAndrefFile()
        vv = getShowIDToUsngAndRank(showidToFileName)
	showidToName = getShowidToName(refFile)
	with open('showidlist.txt', 'w') as f:
                for key,value in showidToName.iteritems():
                        f.write(':'.join([key,value[0],value[1]])+'\n')
	for key,value in showidToName.iteritems():
		header = 'ShowID\tNetwork_no\tShowName\tUSNG_100\tZip\tZip4\tRank'+path.split('/')[-2]+'\n'
		#showdetails = value[1].replace(' ','')+key
		showdetails = 'Show'+key
		filename = showdetails+'.csv'
		f = open(filename,'w')
		f.write(header)
		if vv.get(key):
			showIDToUsngAndRank = vv[key]
			for elements1 in showIDToUsngAndRank:
				if usngzip4dict.get(elements1[0]):
					usngzip4list = usngzip4dict[elements1[0]]
					for elements in usngzip4list:
						zip4 = elements[1] if elements[1]!= '""' else '0000'
						result = '\t'.join((key,value[0],value[1],elements1[0],elements[0],zip4,elements1[1]))+'\n'
						f.write(result)
		f.close()
		#This is for the  hyphen `KendraonTop2150186`
		#command = 'mysql RentrakDB -e \"select * from %s\" -B > %s.csv' %tablename %tablename
		#command =  'mysql RentrakDB drop table %s;'%showdetails
		#subprocess.call(command, shell = True)
		users = createTable(showdetails, 'ShowID', 'Network_no', 'ShowName', 'USNG_100', 'Zip', 'Zip4', 'Rank'+path.split('/')[-2])
		command = 'mysqlimport --ignore-lines=1 --fields-terminated-by=\'\t\' --verbose --local RentrakDB '+ filename
		subprocess.call(command, shell = True)

if __name__ == '__main__':
	getusngzip4dict(llpath)
	getupdatedusngzip(updatedfile)
	run()
	print 'arnab'
