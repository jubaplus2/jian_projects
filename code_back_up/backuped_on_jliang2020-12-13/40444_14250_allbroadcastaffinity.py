import collections;
import commonutils;
import affinity;

programidinfo = '';
programnameinfo = '';
headerstring = None;


def updateids(string2,string3): 
        global programidinfo;   
        programidinfo = string2;
        global programnameinfo; 
        programnameinfo = string3;
	global headerstring;
	headerstring = set();


class hhidalldetailsTable36:

    def __init__(self,v):
        self.hhid = v[29:36];
        self.programid = v[9:16].strip();
	self.totalmin = 0;
	self.viewfreq = set();

class hhidalldetailsTable30:

    def __init__(self,v):
        self.programdist = v[3:8];
        self.programid = v[14:21].strip();
	self.programname = v[55:80].strip().replace(',','');

def populateAllTable36(inputfile):
    	entry = collections.OrderedDict();
	fin = open(inputfile,"r");
	for v in fin:
		if v[0:2] == '36':
			programid = v[9:16].strip();
			personid = v[36:38]
			hhid = v[29:36]
			if int(personid) != 0 and programid == programidinfo:
				key=programid+':'+hhid;
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
			programid = v[14:21].strip();
			key = programid;
			if entry.get(key) is None:
				lhhidalldetailsTable30 = hhidalldetailsTable30(v);
				entry[key] = lhhidalldetailsTable30;
	fin.close();
        return entry;


def merging(affinitydetails,entry36,entry30):
	global headerstring;
	#headerstring = set();
	for key in entry36:
		lhhidalldetailsTable36 = entry36[key];
		lhhidalldetailsTable30 = entry30[key.split(':')[0]];
		v1 = lhhidalldetailsTable30.programdist;
		v2 = lhhidalldetailsTable30.programname;
		svi = str(commonutils.getShiftViewIndicator(str(int(lhhidalldetailsTable36.hhid))+':'+str(int(lhhidalldetailsTable36.programid))))
		result = lhhidalldetailsTable30.programdist+'_'+lhhidalldetailsTable36.programid+'\t'+ v1+'_'+v2+'\t'
		headerstring.add(result+'SVI')
		if affinitydetails.get(lhhidalldetailsTable36.hhid) is None:
                        affobj = affinity.affinityobject();
                else:
                        affobj = affinitydetails[lhhidalldetailsTable36.hhid];
                #affobj.affinitystring.add(result);
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

def generatingoutput(affinitydetails,inputfile2, inputfile3, inputfile4):
        entry30 = populateAllTable30(inputfile3);
        entry36 = populateAllTable36(inputfile2);
        merging(affinitydetails,entry36,entry30);
