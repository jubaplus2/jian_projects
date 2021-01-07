import collections;
import commonutils;
import hhidweights;
import hhidpersonidweights;
import cPickle as pickle
import pandas as pd


networkidandnamefuncentry = None;
programidandname = None;
pickleFileName = 'test_shelf.dat'
pickleFile = None
#clonemodeldict = None;

hhidlistS = None;
programidlistS = None;
programnamelistS = None
hhidlist = None;
programidlist = None;
programnamelist = None;
otprogramnameset = None;

def populateAllTable36(inputfile):
	#global clonemodeldict;
	fin = open(inputfile,"r");
	clonemodeldict = {}	
	for v in fin:
		if v[0:2] == '36':
			programid = v[9:16];
			hhid = v[29:36];
			personid = v[36:38]
			if  int(personid) != 0 and int(hhid) in hhidlistS and str(int(programid)) not in programidlistS:
				cablenetworkid = v[3:9]
				programcode = v[9:16]
				telecastid = v[16:23]
				householdnumber = v[29:36]
				startminprogramviewing = v[38:44]
				endminprogramviewing = v[44:50]
				classificationDate = v[68:76]
				vcrI = v[55]
				tsvc = v[76]
				mpdm = v[109:114]
				vpc = v[114:116]
				vodI = v[116:118]
				#key = hhid+':'+personid+':'+cablenetworkid+'_'+programcode+'_'+telecastid
				key = hhid+':'+cablenetworkid+'_'+programcode+'_'+telecastid
				content = [cablenetworkid, programid,telecastid,startminprogramviewing,endminprogramviewing, hhid, personid, classificationDate,
						vcrI, tsvc, mpdm, vpc,vodI]

				if clonemodeldict.get(key) is None:
					clonemodeldict[key] = [content]
				else:
					clonemodeldict[key].append(content)
					
				
	fin.close();	
	pickle.dump(clonemodeldict,pickleFile)
	


def populateAllTable30(inputfile):
        fin = open(inputfile,"r");
	global programidandname;
        for v in fin:           
                if v[0:2] == '30':

			programdistributor = v[3:8].strip()
			cablenetworkid = v[8:14]
			programid = v[14:21];
			telecastid = v[21:28]
			bcastdate = v[39:47]
			lprogramname = v[55:80].strip().replace(',','')
			reportedstartTime = v[86:92]

                        if str(int(programid)) not in programidlistS and  v[55:80].strip() not in programnamelistS:
				key = cablenetworkid+'_'+ programid+'_'+telecastid
				if programidandname.get(key) is None:
					programidandname[key] = [programdistributor,lprogramname,bcastdate,reportedstartTime]
				
	fin.close();

#hid.generatingoutput(fout,path+'NPM_ARD_W_2014_01_26_REFD_R0/N140130.ref', path+'NPM_ARD_W_2014_01_26_EHH_R0/N140130.EHH', path+'NPM_ARD_W_2014_01_26_CBLNM_R0/N14013V0.ECN', path+'NPM_ARD_W_2014_01_26_CBLNM_R0/N14013D0.ECN');
#Better to check how the signature actually gets called
#def generatingoutput(fout, inputfile1, inputfile2, inputfile3, inputfile4):
def generatingoutput(fout, inputfile1, inputfile4, inputfile2, inputfile3):
	#networkidandnamefunc(inputfile1);
        populateAllTable30(inputfile3);
        populateAllTable36(inputfile2);
		
		
		
def getKeyList(clonemodeldictList):
	x = set()
	for  clonemodeldict in clonemodeldictList:
		x|= set(clonemodeldict.keys())
	return list(x)
	
	


def mergeTable36AndTable30(clonemodeldictList, fout):
	keyList = getKeyList(clonemodeldictList)
	for keys in keyList:
		for clonemodeldict in clonemodeldictList:			
			lclonemodel = clonemodeldict.get(keys)
			if lclonemodel:					
					key30 = keys.split(':')[1]
					table30value = programidandname.get(key30)
					if table30value:
						for elements in lclonemodel:
							result = '\t'.join(elements[0:7])+'\t'+'\t'.join(table30value)+'\t'+'\t'.join(elements[7:])
							fout.write(result+'\n')
					
		
def merging(fout):
	global pickleFile
	pickleFile.close()
	clonemodeldictList = []
	with open(pickleFileName, 'rb') as pickleFile:
		while True:
			try:
				clonemodeldict = pickle.load(pickleFile)
				clonemodeldictList.append(clonemodeldict)
				
			except:
				pass
				break
	mergeTable36AndTable30(clonemodeldictList, fout)
		
def fileremove():
	import os
	try:
		os.remove(pickleFileName)
	except:
		pass


def readViewerShipFilesHHIDs(inputfile):
	v = pd.read_csv(inputfile, sep = '\t')
	return set(v['Household Number'])

def initialize():
	global hhidlist;
	global programidlist;
	global programnamelist;
	global hhidtupledict;
	#hhidlist = commonutils.getITlist('hhidlist.csv');
	global  hhidlistS
	hhidlistS = readViewerShipFilesHHIDs('/root/Projects/arnab/otfiles41/IT-HH-ViewershipDayPartsNew.csv')
	programidlist = commonutils.getITlist('programid.csv');
	global programidlistS
	programidlistS = set(programidlist)
	programnamelist = commonutils.getITlist('programnames.csv');
	global programnamelistS
	programnamelistS = set(programnamelist)
	#global clonemodeldict;
    	#clonemodeldict = {}
	global programidandname;
    	programidandname = {}
	global otprogramnameset;
	otprogramnameset = set();
	fileremove()
	global pickleFile
	pickleFile = open(pickleFileName, 'wb')