import pandas as pd;
import collections;

class clusterdv2:

    def __init__(self):
        self.clusterid = '';
        self.dv2 = '';




def clusterhhidmapping(inputfile):
	v = pd.read_csv(inputfile);
        counter = 0;
        limit = len(v);
        clusterhhidmappingdict =  collections.OrderedDict();
        while counter < limit:
                if clusterhhidmappingdict.get(int(v.iloc[counter][2])) is None:
			lclusterdv2 = clusterdv2();
			lclusterdv2.clusterid = v.iloc[counter][1];
			lclusterdv2.dv2 =  str(int(v.iloc[counter][5]) * int(v.iloc[counter][6]));
                        clusterhhidmappingdict[int(v.iloc[counter][2])] = lclusterdv2;
                counter+=1;
        return clusterhhidmappingdict;

class hhiddetails:

    def __init__(self):
        self.hhid = '';
	self.personiddict = {};





class personiddetails:
	def __init__(self):
		self.pid = '';
		self.h39 = '';
		self.h42 = '';
		self.h43 = '';
		self.h44 = '';
		self.h45 = '';
		self.h46 = '';
		self.h47 = '';
		self.h48 = '';
		self.h49 = '';
		self.h50 = '';
		self.h51 = '';
		self.h54 = '';
		self.h55 = '';
		self.h56 = '';
		self.h58 = '';
		self.h60 = '';
		self.h61 = '';
		self.h62 = '';
		self.h63 = '';
		self.h66 = '';

	def fillpersoniddetails(self,v):
		self.pid = v[20:22];
		self.h39 = v[38:41];
		self.h42 = v[41];
		self.h43 = v[42];
		self.h44 = v[43];
		self.h45 = v[44];
		self.h46 = v[45];
		self.h47 = v[46];
		self.h48 = v[47];
		self.h49 = v[48];
		self.h50 = v[49];
		self.h51 = v[50:53];
		self.h54 = v[53];
		self.h55 = v[54];
		self.h56 = v[55:57];
		self.h58 = v[57:59];
		self.h60 = v[59];
		self.h61 = v[60];
		self.h62 = v[61];
		self.h63 = v[62:65];
		self.h66 = v[65:68];

def populatehhiddetails(inputfile):
    fin = open(inputfile,"r");
    hhiddetailsdict = {};
    for v in fin:
        if v[0:2] == '24':
            key = str(int(v[13:20]));
	    pid = v[20:22];
            if hhiddetailsdict.get(key) is None:
                lhhiddetails = hhiddetails();
	    else:
		lhhiddetails = hhiddetailsdict[key];
	    if lhhiddetails.personiddict.get(pid) is None:
            	lpersoniddetails = personiddetails();
                lpersoniddetails.fillpersoniddetails(v);
                lhhiddetails.personiddict[pid] = lpersoniddetails;
                hhiddetailsdict[key] = lhhiddetails;
		

    return hhiddetailsdict;

def populatehhiddetailsold(inputfile, hhiddetailsdict):
    fin = open(inputfile,"r");
    for v in fin:
        if v[0:2] == '24':
            key = str(int(v[13:20]));
            pid = v[20:22];
            if hhiddetailsdict.get(key) is None:
                lhhiddetails = hhiddetails();
            else:
                lhhiddetails = hhiddetailsdict[key];
            if lhhiddetails.personiddict.get(pid) is None:
                lpersoniddetails = personiddetails();
                lpersoniddetails.fillpersoniddetails(v);
                lhhiddetails.personiddict[pid] = lpersoniddetails;
                hhiddetailsdict[key] = lhhiddetails

path = '/home/account1/marymary/'
newpath = '/disk2/Projects/NielsenData/'

ehhfilelist = [path+'NPM_ARD_W_2015_06_07_EHH_R0/N150550.EHH',
	path+'NPM_ARD_W_2014_09_14_EHH_R0/N140910.EHH',
        path+'NPM_ARD_W_2014_09_21_EHH_R0/N140920.EHH',
        path+'NPM_ARD_W_2014_09_28_EHH_R0/N140930.EHH',
        path+'NPM_ARD_W_2014_10_05_EHH_R0/N140940.EHH',
        path+'NPM_ARD_W_2014_10_12_EHH_R0/N141010.EHH',
        path+'NPM_ARD_W_2014_10_19_EHH_R0/N141020.EHH',
        path+'NPM_ARD_W_2014_10_26_EHH_R0/N141030.EHH',
        path+'NPM_ARD_W_2014_11_02_EHH_R0/N141040.EHH',
        path+'NPM_ARD_W_2014_11_09_EHH_R0/N141110.EHH',
        path+'NPM_ARD_W_2014_11_16_EHH_R0/N141120.EHH',
        path+'NPM_ARD_W_2014_11_23_EHH_R0/N141130.EHH',
        path+'NPM_ARD_W_2014_11_30_EHH_R0/N141140.EHH',
        path+'NPM_ARD_W_2014_12_07_EHH_R0/N141150.EHH',
        path+'NPM_ARD_W_2014_12_14_EHH_R0/N141210.EHH',
        path+'NPM_ARD_W_2015_06_14_EHH_R0/N150610.EHH',
        path+'NPM_ARD_W_2015_06_21_EHH_R0/N150620.EHH',
        path+'NPM_ARD_W_2015_06_28_EHH_R0/N150630.EHH',
        path+'NPM_ARD_W_2015_07_05_EHH_R0/N150640.EHH',
	path+'NPM_ARD_W_2015_07_12_EHH_R0/N150710.EHH',
	path+'NPM_ARD_W_2015_07_19_EHH_R0/N150720.EHH',
	path+'NPM_ARD_W_2015_07_26_EHH_R0/N150730.EHH',
	path+'NPM_ARD_W_2015_08_02_EHH_R0/N150740.EHH',
	path+'NPM_ARD_W_2015_08_09_EHH_R0/N150810.EHH',
	path+'NPM_ARD_W_2015_08_16_EHH_R0/N150820.EHH',
	path+'NPM_ARD_W_2015_08_23_EHH_R0/N150830.EHH',
	path+'NPM_ARD_W_2015_08_30_EHH_R0/N150840.EHH',
	path+'NPM_ARD_W_2015_09_06_EHH_R1/N150851.EHH',
	path+'NPM_ARD_W_2015_09_13_EHH_R0/N150910.EHH',
	path+'NPM_ARD_W_2015_09_20_EHH_R0/N150920.EHH',
	path+'NPM_ARD_W_2015_09_27_EHH_R0/N150930.EHH',
	path+'NPM_ARD_W_2015_10_04_EHH_R0/N150940.EHH',
	path+'NPM_ARD_W_2015_10_11_EHH_R0/N151010.EHH',
	path+'NPM_ARD_W_2015_10_18_EHH_R0/N151020.EHH',
	path+'NPM_ARD_W_2015_10_25_EHH_R0/N151030.EHH',
	path+'NPM_ARD_W_2015_11_01_EHH_R0/N151040.EHH',
	path+'NPM_ARD_W_2015_11_08_EHH_R0/N151110.EHH',
	path+'NPM_ARD_W_2015_11_15_EHH_R0/N151120.EHH',
	path+'NPM_ARD_W_2015_11_22_EHH_R0/N151130.EHH',
        path+'NPM_ARD_W_2015_11_29_EHH_R0/N151140.EHH',
        path+'NPM_ARD_W_2015_12_06_EHH_R0/N151150.EHH',
	path+'NPM_ARD_W_2015_12_13_EHH_R0/N151210.EHH', 
        path+'NPM_ARD_W_2015_12_20_EHH_R0/N151220.EHH',
        path+'NPM_ARD_W_2015_12_27_EHH_R0/N151230.EHH',
        path+'NPM_ARD_W_2016_01_03_EHH_R0/N161240.EHH',
	path+'NPM_ARD_W_2016_01_10_EHH_R0/N160110.EHH', 
        path+'NPM_ARD_W_2016_01_17_EHH_R0/N160120.EHH', 
        path+'NPM_ARD_W_2016_01_24_EHH_R0/N160130.EHH', 
        path+'NPM_ARD_W_2016_01_31_EHH_R0/N160140.EHH', 
        newpath+'NPM_ARD_W_2016_02_07_EHH_R0/N160150.EHH',
        newpath+'NPM_ARD_W_2016_02_14_EHH_R0/N160210.EHH',
	newpath+'NPM_ARD_W_2016_02_21_EHH_R0/N160220.EHH',
	newpath+'NPM_ARD_W_2016_02_28_EHH_R0/N160230.EHH',
        newpath+'NPM_ARD_W_2016_03_06_EHH_R0/N160240.EHH',
	newpath+'NPM_ARD_W_2016_03_13_EHH_R0/N160310.EHH',
	newpath+'NPM_ARD_W_2016_03_20_EHH_R0/N160320.EHH',
	newpath+'NPM_ARD_W_2016_04_03_EHH_R0/N160340.EHH',
	newpath+'NPM_ARD_W_2016_04_10_EHH_R0/N160410.EHH',
	newpath+'NPM_ARD_W_2016_04_17_EHH_R0/N160420.EHH',
	newpath+'NPM_ARD_W_2016_04_24_EHH_R0/N160430.EHH',
	newpath+'NPM_ARD_W_2016_05_01_EHH_R0/N160440.EHH',
	newpath+'NPM_ARD_W_2016_05_08_EHH_R0/N160510.EHH',
	newpath+'NPM_ARD_W_2016_05_15_EHH_R0/N160520.EHH',
	newpath+'NPM_ARD_W_2016_05_22_EHH_R0/N160530.EHH',
	newpath+'NPM_ARD_W_2016_03_27_EHH_R0/N160330.EHH']


def printdetails(clusterhhidmappingdict, hhiddetailsdict):
	header = 'Personid\tHHID\tCluster\tDv1\tDv2\t';
	header+='Age\tGender Code\tAge/Gender Building Block Code\tVisitor Status Code\tPrincipal Shopper\tWorking Women Full Time Flag\t';
	header+='Working Women Part Time Flag\tLanguage Class Code\tFrequent Moviegoer Code\tAvid Moviegoer Code\tNumber of Working Hours\t';
	header+='Lady of Household Flag\tHead of Household Flag\tRelationship to Head of Household Code\tNielsen Occupation Code\tWorks Outside of Home Flag\t';
	header+='Principal Moviegoer Code\tEducation Range\tInternet Usage-Homes\tInternet Usage- Work';
	fout = open('/disk2/Projects/outsiders2output/PersonModel.csv','w');
	fout.write(header+'\n');
	delimiter = '\t';
	for key in clusterhhidmappingdict:
		if hhiddetailsdict.get(str(key)) is not None:
			lclusterdv2 = clusterhhidmappingdict[key];
			dv1 = '1';
			if lclusterdv2.clusterid == 2:
				dv1 = '0';
			lhhiddetails = hhiddetailsdict[str(key)];
			for pids in lhhiddetails.personiddict:
				result = pids+'\t'+str(key)+'\t'+str(lclusterdv2.clusterid)+'\t'+dv1+'\t'+lclusterdv2.dv2+'\t';
				lpididdetails = lhhiddetails.personiddict[pids];
				result+=lpididdetails.h39+delimiter;
				result+=lpididdetails.h42+delimiter;
				result+=lpididdetails.h43+delimiter;
				result+=lpididdetails.h44+delimiter;
				result+=lpididdetails.h45+delimiter;
				result+=lpididdetails.h46+delimiter;
				result+=lpididdetails.h47+delimiter;
				result+=lpididdetails.h48+delimiter;
				result+=lpididdetails.h49+delimiter;
				result+=lpididdetails.h50+delimiter;
				result+=lpididdetails.h51+delimiter;
				result+=lpididdetails.h54+delimiter;
				result+=lpididdetails.h55+delimiter;
				result+=lpididdetails.h56+delimiter;
				result+=lpididdetails.h58+delimiter;
				result+=lpididdetails.h60+delimiter;
				result+=lpididdetails.h61+delimiter;
				result+=lpididdetails.h62+delimiter;
				result+=lpididdetails.h63+delimiter;
				result+=lpididdetails.h66+delimiter;
				fout.write(result+'\n');
	fout.close();
					

if __name__ == "__main__":
	clusterhhidmappingdict = clusterhhidmapping('/disk2/Projects/outsiders2output/kmeansoutput.csv');
	#hhiddetailsdict = populatehhiddetails('/home/account1/marymary/NPM_ARD_W_2015_08_23_EHH_R0/N150830.EHH');
	hhiddetailsdict = populatehhiddetails('/home/account1/marymary/NPM_ARD_W_2015_09_27_EHH_R0/N150930.EHH');
	for elements in ehhfilelist:
                populatehhiddetailsold(elements, hhiddetailsdict)
	printdetails(clusterhhidmappingdict, hhiddetailsdict);
	print 'arnab'
