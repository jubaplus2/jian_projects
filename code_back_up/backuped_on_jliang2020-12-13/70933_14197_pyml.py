import collections;
import commonutils;
import hhidweights;
import hhidpersonidweights;
import kmeansclusterhhidmapping;
import shelve


networkidandnamefuncentry = None;
programidandname = None;
#clonemodeldict = None;

hhidlistS = None;
programidlistS = None;
programnamelistS = None
hhidlist = None;
programidlist = None;
programnamelist = None;
otprogramnameset = None;

class neilsenbcastime:
	def __init__(self):
		self.programname = '';		
		self.dayminskey = ''
		self.hourminskey = ''

	def populatetimestat(self,v):
				self.hourminskey = commonutils.hourLookup.index(v[86:92][2:4])
				self.dayminskey = commonutils.getWeekDay(v[39:47])				

class cloneModel:

	def __init__(self):
		
		self.networknamefreqdict =  {};
		self.networknamemindict = {}

		self.shownamefreqdict = {}
		self.shownamemindict = {}
		
		self.daysfreqdict = {}
		self.daysmindict = {}
		self.hoursfreqdict = {}
		self.hoursmindict = {}

		self.viewingmins = 0

	def populateclonemodel(self,v):
		self.viewingmins+=int(v[44:50]) - int(v[38:44])
		

	        networkname = v[3:9];
		#totalmin =int(v[44:50]) - int(v[38:44]);
		totalmin = self.viewingmins

		if self.networknamefreqdict.get(networkname) is None:
			self.networknamefreqdict[networkname] = 1;
		else:
			self.networknamefreqdict[networkname]+=1;
		if self.networknamemindict.get(networkname) is None:
			self.networknamemindict[networkname] = totalmin;
		else:
			self.networknamemindict[networkname]+=totalmin;
		
		showname =v[9:16];

		if self.shownamefreqdict.get(showname) is None:
			self.shownamefreqdict[showname] = 1;
		else:
			self.shownamefreqdict[showname]+=1;
		if self.shownamemindict.get(showname) is None:
			self.shownamemindict[showname] = totalmin;
		else:
			self.shownamemindict[showname]+=totalmin;

		'''
    		dateindex = commonutils.getWeekDay(v[68:76]);

		
		if self.daysfreqdict.get(dateindex) is None:
			self.daysfreqdict[dateindex] = 1;
		else:
			self.daysfreqdict[dateindex] += 1;


		if self.daysmindict.get(dateindex) is None:
			self.daysmindict[dateindex] = totalmin;
		else:
			self.daysmindict[dateindex] += totalmin;
		'''
	

def populateAllTable36(inputfile):
	#global clonemodeldict;
	fin = open(inputfile,"r");
	clonemodeldict = shelve.open('test_shelf.db',  writeback=True)
	for v in fin:
		if v[0:2] == '36':
			programid = v[9:16];
			hhid = v[29:36];
			cablenetworkid = v[3:9]
			programcode = v[9:16]
			telecastid = v[16:23]
			householdnumber = v[29:36]
			personid = v[36:38]
			startminprogramviewing = v[38:44]
			endminprogramviewing = v[44:50]
			if  int(personid) != 0 and str(int(hhid)) in hhidlistS and str(int(programid)) not in programidlistS:
				#key = hhid+':'+personid+':'+cablenetworkid+'_'+programcode+'_'+telecastid
				key = hhid+':'+cablenetworkid+'_'+programcode+'_'+telecastid
				if clonemodeldict.get(key) is None:
					lcloneModel = cloneModel();
				else:
					lcloneModel = clonemodeldict[key];

				lcloneModel.populateclonemodel(v);
				
				clonemodeldict[key]=lcloneModel;
				
	fin.close();
	clonemodeldict.close()
	


def populateAllTable30(inputfile):
        fin = open(inputfile,"r");
	global programidandname;
        for v in fin:           
                if v[0:2] == '30':

			programdistributor = v[3:8]
			cablenetworkid = v[8:14]
			programid = v[14:21];
			telecastid = v[21:28]
			bcastdate = v[39:47]
			lprogramname = v[55:80].strip().replace(',','')
			reportedstartTime = v[86:92]

                        if str(int(programid)) not in programidlistS and  v[55:80].strip() not in programnamelistS:
				key = cablenetworkid+'_'+ programid+'_'+telecastid
				if programidandname.get(key) is None:
					lneilsenbcastime = neilsenbcastime();
					t = v[55:80].strip().replace(',','');
					nn = networkidandnamefuncentry[v[8:14]]
					lneilsenbcastime.programname = nn+'_'+ t
					global otprogramnameset;
					otprogramnameset.add(lneilsenbcastime.programname);
					lneilsenbcastime.populatetimestat(v);
					programidandname[key] = lneilsenbcastime;
				
	fin.close();

def networkidandnamefunc(inputfile):
	global networkidandnamefuncentry;
	fin = open(inputfile,"r");
	for v in fin:
		if  v[0:2] == '80':
			cablenetworkid = v[11:17];
			cablenetworkname = v[22:52].strip().replace(',','');
			if networkidandnamefuncentry.get(cablenetworkid) is None:
				networkidandnamefuncentry[cablenetworkid] = cablenetworkname;


#hid.generatingoutput(fout,path+'NPM_ARD_W_2014_01_26_REFD_R0/N140130.ref', path+'NPM_ARD_W_2014_01_26_EHH_R0/N140130.EHH', path+'NPM_ARD_W_2014_01_26_CBLNM_R0/N14013V0.ECN', path+'NPM_ARD_W_2014_01_26_CBLNM_R0/N14013D0.ECN');
#Better to check how the signature actually gets called
#def generatingoutput(fout, inputfile1, inputfile2, inputfile3, inputfile4):
def generatingoutput(fout, inputfile1, inputfile4, inputfile2, inputfile3):
	networkidandnamefunc(inputfile1);
        populateAllTable30(inputfile3);
        populateAllTable36(inputfile2);

def getdv2(clonemodel):
	tfreq = sum(clonemodel.networknamefreqdict.values())
	tmin = sum(clonemodel.networknamemindict.values())
	return tfreq * tmin;

def printlist(inputlist):
        result = '';
        for elements in inputlist:
                result+=str(elements)+'\t';
        return result;

def getlistvalue(inputlist, clonemodelobjdict,globaldict):
	verbose = ['0'] * len(inputlist);
	for key in clonemodelobjdict:
		if globaldict.get(key) is not None:
			name = globaldict[key];
			value = clonemodelobjdict[key];
			verbose[inputlist.index(name)] = str(value);
	return verbose;
'''
def getprogramnamevalue(inputlist, clonemodelobjdict,globaldict):
	verbose = ['0'] * len(inputlist);
	for key in clonemodelobjdict:
		if globaldict.get(key) is not None:
			name = globaldict[key].programname;
			value = clonemodelobjdict[key];
			verbose[inputlist.index(name)] = str(value);
	return verbose;
'''

def getprogramnamevalue(inputlist, clonemodelobjdict,globaldict):
        verbose = ['0'] * len(inputlist);
        for key in clonemodelobjdict:
                for k,v in globaldict.iteritems():
                        if key in k:
                                name = v.programname
                                value = clonemodelobjdict[key];
                                verbose[inputlist.index(name)] = str(value);
                                break
        return verbose

def getcommonlistvalues(inputlist, clonemodelobjdict):
	verbose = ['0'] *len(inputlist);
	for key in clonemodelobjdict:
		verbose[key] = clonemodelobjdict[key];
	return verbose;

def gethoursstatvalues(inputlist,shownamemindict, programidandname):
	verbosefreq = [0] *len(inputlist);
	verbosemin = [0] *len(inputlist);
	for keys in shownamemindict:
		if programidandname.get(keys) is not None:
			lhoursfreqdict = programidandname[keys].hoursfreqdict;
			lhoursmindict = programidandname[keys].hoursmindict;
			for key1 in lhoursfreqdict:
				verbosefreq[inputlist.index(key1)] += lhoursfreqdict[key1];
			for key2 in lhoursmindict:
				verbosemin[inputlist.index(key2)] += lhoursmindict[key2];
	return verbosefreq,verbosemin;

def mergeTable36AndTable30(clonemodeldict):
	
	for keys, lclonemodel in clonemodeldict.iteritems():
		#key30 = keys.split(':')[2]
		key30 = keys.split(':')[1]
		table30value = programidandname.get(key30)
		if table30value:
			if lclonemodel.daysfreqdict.get(table30value.dayminskey):
				lclonemodel.daysfreqdict[table30value.dayminskey]+=1
			else:
				lclonemodel.daysfreqdict[table30value.dayminskey] = 1 

			if lclonemodel.daysmindict.get(table30value.dayminskey):
				lclonemodel.daysmindict[table30value.dayminskey]+=lclonemodel.viewingmins
			else:
				lclonemodel.daysmindict[table30value.dayminskey]= lclonemodel.viewingmins


			if lclonemodel.hoursfreqdict.get(table30value.hourminskey):
				lclonemodel.hoursfreqdict[table30value.hourminskey]+=1
			else:
				lclonemodel.hoursfreqdict[table30value.hourminskey]=1

			if lclonemodel.hoursmindict.get(table30value.hourminskey):
				lclonemodel.hoursmindict[table30value.hourminskey]+=lclonemodel.viewingmins
			else:
				lclonemodel.hoursmindict[table30value.hourminskey]= lclonemodel.viewingmins	
		
		
		
def merging(fout,networknamelist, lprogramnamelist):
		clonemodeldict = shelve.open('test_shelf.db',  writeback=True)
		mergeTable36AndTable30(clonemodeldict)
		for lkeys in clonemodeldict:
                keys = lkeys.split(':')[0]
                clustervalue = hhidtupledict[str(int(keys))];
                result = str(keys)+'\t'+str(clustervalue)+'\t';
                dv1 = clustervalue[0];
                lclonemodel = clonemodeldict[lkeys];
                dv2 =clustervalue[1] ;
                result+=dv1+'\t'+str(dv2)+'\t';
                networknamefreqlist = getlistvalue(networknamelist,lclonemodel.networknamefreqdict, networkidandnamefuncentry);
                result+=printlist(networknamefreqlist);
                networknameminlist = getlistvalue(networknamelist,lclonemodel.networknamemindict, networkidandnamefuncentry);
                result+=printlist(networknameminlist);

                shownamefreqlist = getprogramnamevalue(lprogramnamelist,lclonemodel.shownamefreqdict, programidandname);
                result+=printlist(shownamefreqlist);
                shownameminlist = getprogramnamevalue(lprogramnamelist,lclonemodel.shownamemindict, programidandname);
                result+=printlist(shownameminlist);

                dayfreqlist = getcommonlistvalues(commonutils.dayLookup, lclonemodel.daysfreqdict);
                result+=printlist(dayfreqlist);

                #hoursfreqlist,hoursminlist = gethoursstatvalues(commonutils.hourLookup,lclonemodel.shownamemindict, programidandname);
                hoursfreqlist = getcommonlistvalues(commonutils.hourLookup, lclonemodel.hoursfreqdict);
                result+=printlist(hoursfreqlist);

                dayminlist = getcommonlistvalues(commonutils.dayLookup, lclonemodel.daysmindict);
                result+=printlist(dayminlist);
                hoursminlist = getcommonlistvalues(commonutils.hourLookup, lclonemodel.hoursmindict);
                result+=printlist(hoursminlist);
                fout.write(result+'\n');
		clonemodeldict.close()
		
		

def initialize():
	global hhidlist;
	global programidlist;
	global programnamelist;
	global hhidtupledict;
	#hhidlist = commonutils.getITlist('hhidlist.csv');
	hhidtupledict = commonutils.getITtupledict('DV1_DV2.csv');
	hhidlist = hhidtupledict.keys();
	global  hhidlistS
	hhidlistS = set(hhidlist)
	
	programidlist = commonutils.getITlist('programid.csv');
	global programidlistS
	programidlistS = set(programidlist)
	programnamelist = commonutils.getITlist('programnames.csv');
	global programnamelistS
	programnamelistS = set(programnamelist)
	global networkidandnamefuncentry;
	networkidandnamefuncentry = {};
	#global clonemodeldict;
    	#clonemodeldict = {}
	global programidandname;
    	programidandname = {}
	global otprogramnameset;
	otprogramnameset = set();
