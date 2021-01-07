import collections;
import commonutils;
import hhidweights;
import hhidpersonidweights;
cableidinfo = '';
programidinfo = '';
programnameinfo = '';

class hhidalldetailsTable36:

    def __init__(self,v):
        self.hhid = v[29:36];
	self.cableid = v[3:9];
	self.personid = v[36:38]
        self.programid = v[9:16].strip();
        self.date = v[68:76];
	self.newList = []
	self.age = v[56:59]
        self.sexcode = v[59]

class hhidalldetailsTable30:

    def __init__(self,v):
	self.cableid = v[8:14];
        self.programdist = v[3:8].strip().replace(',','');
        self.programid = v[14:21].strip();
	self.programname = v[55:80].strip().replace(',',' ');
	self.bcastday = v[39:47];
        self.bcaststarttime = v[86:92];
        self.bcastduration = v[98:104];
	self.telecastid = v[21:28];
	self.episodeName = v[130:170].strip().replace(',','');

def updateids(string1,string2,string3):
	global cableidinfo;
        cableidinfo = string1;
        global programidinfo;
        programidinfo = string2;
        global programnameinfo;
        programnameinfo = string3;

def gettabwt(bdate, hhidpersonidweightsdict, key):
        intabwt = '';
        hhidpersonidweightsdictkey = commonutils.getSunday(bdate)+':'+key;
        if hhidpersonidweightsdict.get(hhidpersonidweightsdictkey) is not None:
                hhidpersoniddetails = hhidpersonidweightsdict[hhidpersonidweightsdictkey];
                dateindex = commonutils.getWeekDay(bdate);
                intabwt = hhidpersoniddetails.intabwt[dateindex];
        return intabwt;

def personidsumtabwt(inputlist,bdate, hhidpersonidweightsdict,hhid):
        result = 0;
        for element in inputlist:
                key = hhid+element;
                value = gettabwt(bdate, hhidpersonidweightsdict, key);
                if value != '':
                        result+=int(value);
        return result;

def populateAllTable36(inputfile):
    	entry = {}
	fin = open(inputfile,"r");
	for v in fin:
		if v[0:2] == '36':
			cableid = v[3:9];
			programid = v[9:16].strip();
			hhid = v[29:36];
			personid = v[36:38]
			if int(personid) != 0 and cableid == cableidinfo and programid == programidinfo:
				telecastid = v[16:23];
                                viewershipdate= v[68:76];
				key=cableid+programid+telecastid+':'+viewershipdate+hhid+personid;
				startminprogramviewing = v[38:44]
                                endminprogramviewing = v[44:50]
				diff = str(int(endminprogramviewing) - int(startminprogramviewing))
                                classificationDate = v[68:76]
                                vcrI = v[55]
                                tsvc = v[76]
                                mpdm = v[109:114].strip()
                                vpc = v[114:116]
                                vodI = v[116:118]
				content = [startminprogramviewing, endminprogramviewing, diff, vcrI,tsvc,mpdm, vpc,vodI]
				if entry.get(key) is None:
					lhhidalldetailsTable36 = hhidalldetailsTable36(v);
				else:
					lhhidalldetailsTable36 = entry[key];
				lhhidalldetailsTable36.newList.append(content)
				entry[key]=lhhidalldetailsTable36;
    	fin.close();
    	return entry;

def populateAllTable30(inputfile):
        fin = open(inputfile,"r");
        entry = {}
        for v in fin:           
                if v[0:2] == '30':
			cableid = v[8:14];
			programid = v[14:21].strip();
			#if programid == programidinfo  and v[55:80].strip().replace(',',' ') == programnameinfo:
			if cableid == cableidinfo and  programid == programidinfo:
				telecastid = v[21:28];
				key = cableid+programid+telecastid;
				if entry.get(key) is None:
					lhhidalldetailsTable30 = hhidalldetailsTable30(v);
					entry[key] = lhhidalldetailsTable30;
	fin.close();
        return entry;



def merging(fout,entry36,entry30,hhidweightsdict, hhidpersonidweightsdict):
        for key in entry36:
                lhhidalldetailsTable36 = entry36[key];
                lhhidalldetailsTable30 = entry30[key.split(':')[0]];
                result30 = '\t'.join([lhhidalldetailsTable30.cableid, lhhidalldetailsTable30.programid,lhhidalldetailsTable30.telecastid,
                                lhhidalldetailsTable30.bcastday,lhhidalldetailsTable30.programdist,lhhidalldetailsTable30.programname,
                                lhhidalldetailsTable30.episodeName,lhhidalldetailsTable30.bcastduration,lhhidalldetailsTable30.bcaststarttime])
                result36 = '\t'.join([lhhidalldetailsTable36.date,lhhidalldetailsTable36.hhid,lhhidalldetailsTable36.personid,
                                        lhhidalldetailsTable36.hhid+'_'+lhhidalldetailsTable36.personid,lhhidalldetailsTable36.age,lhhidalldetailsTable36.sexcode])
                hhidintabwt = gettabwt(lhhidalldetailsTable36.date, hhidweightsdict, lhhidalldetailsTable36.hhid);
                personsintabwt = gettabwt(lhhidalldetailsTable36.date, hhidpersonidweightsdict, lhhidalldetailsTable36.hhid+lhhidalldetailsTable36.personid)
                for elements in lhhidalldetailsTable36.newList:
                        result36List = '\t'.join(elements)
                        result = result30+'\t'+result36+'\t'+result36List+'\t'+personsintabwt+'\t'+hhidintabwt+'\n'
                        fout.write(result);


def generatingoutput(fout, inputfile1, inputfile4, inputfile2, inputfile3):
        entry30 = populateAllTable30(inputfile3);
        entry36 = populateAllTable36(inputfile2);
	hhidweightsdict = hhidweights.populatehhidweightdetails(inputfile4);
	hhidpersonidweightsdict = hhidpersonidweights.populatehhidweightdetails(inputfile4);
        merging(fout,entry36,entry30,hhidweightsdict, hhidpersonidweightsdict);
