from os import listdir
import pandas as pd
import math
from db import *
import subprocess

#path = '/disk2/Projects/Rentrak/20150907-20150920/'
path = '/disk2/Projects/Rentrak/20150921-20151004/'
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


def helper(prevweekfile,showidtoname):
	with open(prevweekfile) as f:
		t = f.read()
		v = t.split('\n')
		v.remove('')
		for elements in v:
			tt = elements.split(':')
			if not showidtoname.get(tt[0]):
				showidtoname[tt[0]] = (tt[1], tt[2])


def mergeShowlist(prevweekfile,currentweekfile):
	showidtoname = {}
	helper(prevweekfile,showidtoname)
	helper(currentweekfile,showidtoname)
	return showidtoname

def getDBDetails(tablename):
		#mydb -e "select * from mytable" -B > mytable.tsv 
		#tablename = 'KendraonTop2150186'
		#command = 'mysql RentrakDB -e \"select * from %s\" -B > %s.csv' %tablename %tablename
		command = 'mysql RentrakDB -e \"select * from {0}\" -B > {1}.csv'
		command = command.format(tablename,tablename)
		return subprocess.call(command, shell = True)

def previousweekFile(inputfile):
        v = pd.read_csv(inputfile, '\t', dtype = str)
        newcol = 'Rank'+path.split('/')[-2]
        #Note t is going to be the new header of the new output file
        #framelist = [v [(v['ShowID'] == str(elements))] for elements in showlist]
        #v = pd.concat(framelist)
        v.fillna('0000', inplace=True)
        #Making a new col on existing dataframe with new weeks name and default rank as zero
        v.loc[:,newcol] = '0'
        t = list(v.keys())
        t.append(newcol)
        weekinfodict = {}
        lenv = len(v)
        counter = 0
        while counter < lenv:
                weekinfodict[''.join(v.iloc[counter, 0:6])] = counter
                counter+=1
        return t, len(t[6:]),v, weekinfodict

def searchinDF(prevweekdf,weekinfodict, inputlist, newrank):
        counter = 0
        t = set(inputlist)
        key = ''.join(inputlist)
        if weekinfodict.get(key):
                prevweekdf.iloc[weekinfodict[key], -1] =  newrank
                print 'arnab i am here'
                return True

        return False

def run():
	showidToFileName, refFile = getShowidToUsngFileNameAndrefFile()
        vv = getShowIDToUsngAndRank(showidToFileName)
	showidToName = getShowidToName(refFile)
        with open('showidlist1.txt', 'w') as f:
		for key,value in showidToName.iteritems():
			f.write(':'.join([key,value[0],value[1]])+'\n')
	showidToName = mergeShowlist('showidlist.txt','showidlist1.txt')
	for key,value in showidToName.iteritems():
		#showdetails = value[1].replace(' ','')+key
		showdetails = 'Show'+key
		#Was present before so update the new week's rank
		lweeklength = 0
		import pdb
		pdb.set_trace()
		if not getDBDetails(showdetails):
			lheader, weeklength, prevweekdf, weekinfodict = previousweekFile(showdetails+'.csv')
			lweeklength = weeklength
			if vv.get(key):
				showIDToUsngAndRank = vv[key]
				for elements1 in showIDToUsngAndRank:
					if usngzip4dict.get(elements1[0]):
						usngzip4list = usngzip4dict[elements1[0]]
						for elements in usngzip4list:
							if int(elements1[1]) != 0:
								print 'arnab'
							inputlist = [key,value[0],value[1],elements1[0],elements[0],elements[1]]
							if not searchinDF(prevweekdf,weekinfodict, inputlist, elements1[1]):
								print 'not present'
				prevweekdf.to_csv(showdetails+'.csv','\t', index = False,header=False)
				return
		#This is for the  hyphen `KendraonTop2150186`
		'''
		users = createTable(showdetails, 'ShowID', 'Network_no', 'ShowName', 'USNG_100', 'Zip', 'Zip4', 'Rank'+path.split('/')[-2])
		command = 'mysqlimport --ignore-lines=1 --fields-terminated-by=\'\t\' --verbose --local RentrakDB '+ filename
		subprocess.call(command, shell = True)
		'''

if __name__ == '__main__':
	getusngzip4dict(llpath)
	getupdatedusngzip(updatedfile)
	run()
	print 'arnab'
