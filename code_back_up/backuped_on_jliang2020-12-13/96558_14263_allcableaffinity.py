import collections;
import commonutils;
import affinity;

cableidinfo = '';
programidinfo = '';
programnameinfo = '';
headerstring = None;


def updateids(string1,string2,string3):
        global cableidinfo;
        cableidinfo = string1;
        global programidinfo;
        programidinfo = string2;
        global programnameinfo;
        programnameinfo = string3;
	global headerstring;
	headerstring = set();

class hhidalldetailsTable36:

    def __init__(self,v):
        self.hhid = v[29:36];
        self.cableid = v[3:9];
        self.programid = v[9:16];
	self.totalmin = 0;
	self.viewfreq = set();

class hhidalldetailsTable30:

    def __init__(self,v):
        self.cableid = v[8:14];
        self.programid = v[14:21];
	self.programname = v[55:80].strip().replace(',','');
	self.programdist = v[3:8].strip().replace(',','');

def populateAllTable36(inputfile):
    	entry = collections.OrderedDict();
	fin = open(inputfile,"r");
	for v in fin:
		if v[0:2] == '36':
			cableid = v[3:9];
			hhid = v[29:36];
			programid = v[9:16];
			personid = v[36:38]
			if int(personid) != 0 and cableid == cableidinfo and programid == programidinfo:
				key=cableid+programid+':'+hhid;
				if entry.get(key) is None:
					lhhidalldetailsTable36 = hhidalldetailsTable36(v);
				else:
					lhhidalldetailsTable36 = entry[key];
				lhhidalldetailsTable36.totalmin+=int(v[44:50]) - int(v[38:44]);
				telecastid = v[16:23];
				#v[68:76] is the classification date or the viewing date
				xx = programid+telecastid+v[68:76]
				#lhhidalldetailsTable36.viewfreq.add(telecastid);
				lhhidalldetailsTable36.viewfreq.add(xx);
				entry[key]=lhhidalldetailsTable36;
    	fin.close();
    	return entry;

def populateAllTable30(inputfile):
        fin = open(inputfile,"r");
        entry = collections.OrderedDict();
        for v in fin:           
                if v[0:2] == '30':
                        cableid = v[8:14];
			programid = v[14:21];
                        #if cableid == cableidinfo and v[55:80]== programnameinfo:
                        if cableid == cableidinfo and programid == programidinfo:
				#programid = v[14:21];
				key = cableid+programid;
				if entry.get(key) is None:
					lhhidalldetailsTable30 = hhidalldetailsTable30(v);
					entry[key] = lhhidalldetailsTable30;
	fin.close();
        return entry;

def networkidandnamefunc(inputfile):
	entry = {};
	fin = open(inputfile,"r");
	for v in fin:
		if  v[0:2] == '80':
			cablenetworkid = v[11:17];
			cablenetworkname = v[22:52].strip().replace(',','');
			if entry.get(cablenetworkid) is None:
				entry[cablenetworkid] = cablenetworkname;
	return entry;


def merging(affinitydetails,entry36,entry30):
	global headerstring;
	#headerstring = set();
	for key in entry36:
		lhhidalldetailsTable36 = entry36[key];
		lhhidalldetailsTable30 = entry30[key.split(':')[0]];
		#v1 = networkidandname[lhhidalldetailsTable36.cableid];
		v1 = lhhidalldetailsTable30.programdist;
		v2 = lhhidalldetailsTable30.programname;
		#result = lhhidalldetailsTable36.cableid+'_'+lhhidalldetailsTable36.programid+'\t'+ v1+'_'+v2+'\t';
		svi = str(commonutils.getShiftViewIndicator(str(int(lhhidalldetailsTable36.hhid))+':'+str(int(lhhidalldetailsTable36.programid))))
		result = lhhidalldetailsTable36.cableid+'_'+lhhidalldetailsTable36.programid+'\t'+ v1+'_'+v2+'\t'
		headerstring.add(result+'SVI')
		if affinitydetails.get(lhhidalldetailsTable36.hhid) is None:
			affobj = affinity.affinityobject();	
		else:
			affobj = affinitydetails[lhhidalldetailsTable36.hhid];


		if affobj.affinitystringdict.get(result+svi) is None:
			laffinityfreqminvalues = affinity.affinityfreqminvalues();
		else:
			laffinityfreqminvalues = affobj.affinitystringdict[result+svi];
		laffinityfreqminvalues.totalmin+=lhhidalldetailsTable36.totalmin;
		laffinityfreqminvalues.viewfreq+=len(lhhidalldetailsTable36.viewfreq);
		affobj.affinitystringdict[result+svi] = laffinityfreqminvalues;
		affobj.totalmin += lhhidalldetailsTable36.totalmin;
		affobj.viewfreq += len(lhhidalldetailsTable36.viewfreq);
		affinitydetails[lhhidalldetailsTable36.hhid] = affobj;

def generatingoutput(affinitydetails, inputfile1, inputfile4,inputfile2, inputfile3):
#def generatingoutput(affinitydetails, inputfile1, inputfile2, inputfile3, inputfile4):
	#networkidandname = networkidandnamefunc(inputfile1);
        entry30 = populateAllTable30(inputfile3);
        entry36 = populateAllTable36(inputfile2);
        merging(affinitydetails,entry36,entry30)
