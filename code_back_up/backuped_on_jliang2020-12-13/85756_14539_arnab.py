from os import listdir
import pandas as pd
import math
from db import *

#path = '/disk2/Projects/Rentrak/20150907-20150920/'
#path = '/disk2/Projects/Rentrak/20150921-20151004/'
path = '/disk2/Projects/Rentrak/20151005-20151018/'
llpath = '/disk2/Projects/Rentrak/zip4_lat_lon_usng.csv'
updatedfile = '/disk2/Projects/Rentrak/Mediastorm_USNG_tile_query_updated_with_geography.txt'
ReferenceFile =  'Mediastorm_showlist_xref_20150907-20150920.xlsx'

#Format is zip|zip4|lat|lon|usng_tile
#01001|0004|42.06582|-72.62216|18TXM967598
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

#Fromat is USNG_tile,lat,long,zip,zip4
#04QDK448245,21.923901,-159.534485,96741,9795
def getupdatedusngzip(updatedfile, usngzip4dict):
        with open(updatedfile) as f:
                next(f)
                for line in f:
                        v = line.split(',')
                        key = v[0]
                        if  not usngzip4dict.get(key):
                                usngzip4dict[key] = []
                        usngzip4dict[key].append((v[-2],v[-1].replace('\r\n','')))
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


def previousweekFile(inputfile):
        v = pd.read_csv(inputfile, '\t', dtype = str)
        t = list(v.keys())
        #Appending the new week rank information
        #Below dict contains keys as ShowID\tNetwork_no\tShowName\tUSNG_100\tZip\tZip4
        #and values as list of ranks across weeks
	'''
        weekinfodict = {}
        lenv = len(v)
        counter = 0
        while counter < lenv:
                xx = v[t[0]][counter].split('\t')
		#x[:6] is till ShowID\tNetwork_no\tShowName\tUSNG_100\tZip\tZip4 and x[6:] will be all rank details
		#Making the following week rank as 0 by default which is going to be updated when the following week file is read
                weekinfodict['\t'.join(xx[:6])] = xx[6:] + ['0']
                counter+=1
	'''
	newcol = 'Rank'+path.split('/')[-2]
        t.append(newcol)
	#Making a new col on existing dataframe with new weeks name and default rank as zero
	v.loc[:,newcol] = '0'
        #Note t is going to be the new header of the new output file
        return t, len(t[6:]),v

def searchinDF(prevweekdf,prevweekdflen,inputlist, newrank):
	counter = 0
	t = set(inputlist)
	while counter < prevweekdflen:
		if set(prevweekdf.iloc[counter, 0:6]) & t == t:
			print 'arnab i am here'
			prevweekdf.iloc[counter, -1] =  newrank
			return True
		counter+=1
	return False


def ncreatoutput1():
	lheader, weeklength, prevweekdf = previousweekFile('sample.tsv')
	#lheader, weeklength, prevweekdf = previousweekFile('../week1/rentrakoutputW1.csv')
	prevweekdflen = len(prevweekdf)
	print 'hit ncreatoutput1'
	print ' first previousweekFile written'
	header = '\t'.join(lheader)+'\n'
	print 'header written'
	print header
	showidToFileName, refFile = getShowidToUsngFileNameAndrefFile()
        vv = getShowIDToUsngAndRank(showidToFileName)
	showidToName = getShowidToName(refFile)
	tt = getusngzip4dict(llpath)
	getupdatedusngzip(updatedfile, tt)
	print 'zip part done'
	appendingframe = []
	for key,value in showidToName.iteritems():
		if vv.get(key):
			showIDToUsngAndRank = vv[key]
		
			for elements1 in showIDToUsngAndRank:
				if tt.get(elements1[0]):
					usngzip4list = tt[elements1[0]]
					for elements in usngzip4list:
						inputlist = [key,value[0],value[1],elements1[0],elements[0],elements[1]]
						if not searchinDF(prevweekdf,prevweekdflen, inputlist, elements1[1]):
							nn = ['0'] * (weeklength)
							nn[-1] = elements1[1]
							inputlist.extend(nn)
							appendingframe.append(inputlist)
	print 'dumping data'
	prevweekdf.append(pd.DataFrame(appendingframe, dtype = str))
	prevweekdf.to_csv('output.csv', '\t')
	print 'arnab6'



if __name__ == '__main__':
	ncreatoutput1()
