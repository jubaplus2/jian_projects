import collections;
import commonutils;
import hhidweights;
import hhidpersonidweights;


cableidinfo = '';
programidinfo = '';
programnameinfo = '';
networkidandnamefuncentry = None;
entry30 = None;
entry36 = None;

def updateids(string1,string2,string3):
        global cableidinfo;     
        cableidinfo = string1;  
        global programidinfo;           
        programidinfo = string2;        
        global programnameinfo; 
        programnameinfo = string3;
	global networkidandnamefuncentry;
	networkidandnamefuncentry = {};
	global entry36;
    	entry36 = collections.OrderedDict();
	global entry30;
    	entry30 = collections.OrderedDict();

class hhidalldetailsTable36:

    def __init__(self,v):
        self.hhid = v[29:36];
        self.cableid = v[3:9];
        self.programid = v[9:16];
	self.totalmin = 0;
	self.viewfreq = set();
	self.personidset = set();

class hhidalldetailsTable30:

    def __init__(self,v):
        self.cableid = v[8:14];
        self.programid = v[14:21];
	self.programname = v[55:80].strip().replace(',','');
	self.bcastdayset = set();
        self.bcastduration = 0;
        self.bcastfrequency = 0;

def populateAllTable36(inputfile):
	global entry36;
	fin = open(inputfile,"r");
	for v in fin:
		if v[0:2] == '36':
			cableid = v[3:9];
			programid = v[9:16];
			if cableid == cableidinfo and programid == programidinfo:
				hhid = v[29:36];
				key=cableid+programid+':'+hhid;
				if entry36.get(key) is None:
					lhhidalldetailsTable36 = hhidalldetailsTable36(v);
				else:
					lhhidalldetailsTable36 = entry36[key];
				lhhidalldetailsTable36.totalmin+=int(v[44:50]) - int(v[38:44]);
				telecastid = v[16:23];
				lhhidalldetailsTable36.viewfreq.add(telecastid);
				lhhidalldetailsTable36.personidset.add(v[36:38]);
				entry36[key]=lhhidalldetailsTable36;
				
    	fin.close();

def populateAllTable30(inputfile):
        fin = open(inputfile,"r");
	global entry30;
        for v in fin:           
                if v[0:2] == '30':
                        cableid = v[8:14];
			programid = v[14:21];
                        #if cableid == cableidinfo and v[55:80]== programnameinfo:
                        if cableid == cableidinfo and programid == programidinfo:
				#programid = v[14:21];
				key = cableid+programid;
				if entry30.get(key) is None:
					lhhidalldetailsTable30 = hhidalldetailsTable30(v);
				else:
					lhhidalldetailsTable30 =  entry30[key];
				lhhidalldetailsTable30.bcastduration +=int(v[98:104]);
				lhhidalldetailsTable30.bcastfrequency += 1;
				lhhidalldetailsTable30.bcastdayset.add(v[39:47]);
				entry30[key] = lhhidalldetailsTable30;
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
	return networkidandnamefuncentry;


def merging(fout):
	for key in entry36:
		lhhidalldetailsTable36 = entry36[key];
		lhhidalldetailsTable30 = entry30[key.split(':')[0]];
		result = lhhidalldetailsTable36.hhid+'\t'+lhhidalldetailsTable36.cableid+'\t'+lhhidalldetailsTable36.programid+'\t';
		v1 = networkidandnamefuncentry[lhhidalldetailsTable36.cableid];
		v2 = lhhidalldetailsTable30.programname;
		result+= v1+'\t'+v2+'\t'+str(lhhidalldetailsTable30.bcastfrequency)+'\t'+str(lhhidalldetailsTable30.bcastduration)+'\t';
		result+=str(len(lhhidalldetailsTable36.viewfreq))+'\t'+str(lhhidalldetailsTable36.totalmin)+'\t';
		lShareOfProgramViewershipMinutes = round(lhhidalldetailsTable36.totalmin * 100/float(lhhidalldetailsTable30.bcastduration), 4);
		lShareOfProgramViewershipFrequency = round(len(lhhidalldetailsTable36.viewfreq) * 100/float(lhhidalldetailsTable30.bcastfrequency),4);
		result+=str(lShareOfProgramViewershipMinutes)+'\t'+str(lShareOfProgramViewershipFrequency)+'\t';
		result+=str(len(lhhidalldetailsTable36.personidset))+'\n';
		fout.write(result);

#def generatingoutput(fout, inputfile1,inputfile2, inputfile3, inputfile4):
def generatingoutput(fout, inputfile1, inputfile4, inputfile2, inputfile3):
	networkidandnamefunc(inputfile1);
        populateAllTable30(inputfile3);
        populateAllTable36(inputfile2);
