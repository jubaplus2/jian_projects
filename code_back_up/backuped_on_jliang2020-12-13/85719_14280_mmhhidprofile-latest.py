import pandas as pd;
import collections;
path = '/home/account1/marymary/'
newpath = '/disk2/Projects/NielsenData/'

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
	self.h37 = '';
	self.h38 = '';
	self.h39 = '';
	self.h40 = '';
	self.h41 = '';
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
	self.h52 = '';
	self.h53 = '';
	self.h55 = '';
	self.h56 = '';
	self.h57 = '';
	self.h59 = '';
	self.h60 = '';
	self.h61 = '';
	self.h62 = '';
	self.h64 = '';
	self.h65 = '';
	self.h66 = '';
	self.h67 = '';
	self.h68 = '';
	self.h69 = '';
	self.h70 = '';
	self.h71 = '';
	self.h72 = '';
	self.h74 = '';
	self.h76 = '';
	self.h78 = '';
	self.h80 = '';
	self.h82 = '';
	self.h83 = '';
	self.h91 = '';
	self.h92 = '';
	self.h94 = '';
	self.h95 = '';
	self.h98 = '';
	self.h100 = '';
	self.h102 = '';
	self.h103 = '';
	self.h104 = '';
	self.h105 = '';
	self.h106 = '';
	self.h107 = '';
	self.h108 = '';
	self.h109 = '';
	self.h110 = '';
	self.h111 = '';
	self.h112 = '';
	self.h113 = '';
	self.h115 = '';
	self.h120 = '';
	self.h121 = '';
	self.h122 = '';
	self.h123 = '';
	self.h124 = '';
	self.h125 = '';
	self.h126 = '';
	self.h127 = '';
	self.h128 = '';
	self.h129 = '';
	self.h130 = '';
	self.h131 = '';
	self.t14 = [' ']*9

    def fillhhiddetails(self,v):
        self.hhid = v[13:20];
	self.h37 = v[36];
	self.h38 = v[37];
	self.h39 = v[38];
	self.h40 = v[39];
	self.h41 = v[40];
	self.h42 = v[41];
	self.h43 = v[42];
	self.h44 = v[43];
	self.h45 = v[44];
	self.h46 = v[45];
	self.h47 = v[46];
	self.h48 = v[47];
	self.h49 = v[48];
	self.h50 = v[49];
	self.h51 = v[50];
	self.h52 = v[51];
	self.h53 = v[52:54];
	self.h55 = v[54];
	self.h56 = v[55];
	self.h57 = v[56:58];
	self.h59 = v[58];
	self.h60 = v[59];
	self.h61 = v[60];
	self.h62 = v[61:63];
	self.h64 = v[63];
	self.h65 = v[64];
	self.h66 = v[65];
	self.h67 = v[66];
	self.h68 = v[67];
	self.h69 = v[68];
	self.h70 = v[69];
	self.h71 = v[70];
	self.h72 = v[71:73];
	self.h74 = v[73:75];
	self.h76 = v[75:77];
	self.h78 = v[77:79];
	self.h80 = v[79:81];
	self.h82 = v[81];
	self.h83 = v[82:90];
	self.h91 = v[90];
	self.h92 = v[91:93];
	self.h94 = v[93];
	self.h95 = v[94];
	self.h98 = v[97:99];
	self.h100 = v[99:101];
	self.h102 = v[101];
	self.h103 = v[102];
	self.h104 = v[103];
	self.h105 = v[104];
	self.h106 = v[105];
	self.h107 = v[106];
	self.h108 = v[107];
	self.h109 = v[108];
	self.h110 = v[109];
	self.h111 = v[110];
	self.h112 = v[111];
	self.h113 = v[112:114];
	self.h115 = v[114:119];
	self.h120 = v[119];
	self.h121 = v[120];
	self.h122 = v[121];
	self.h123 = v[122];
	self.h124 = v[123];
	self.h125 = v[124];
	self.h126 = v[125];
	self.h127 = v[126];
	self.h128 = v[127];
	self.h129 = v[128];
	self.h130 = v[129];
	self.h131 = v[130:132];

'''
def populateTable14(hhiddetailsdict, inputfile):
	fin = open(inputfile,"r");
	for v in fin:
		if v[0:2] == '14':
			hhid = v[13:20]
			lhhiddetails = hhiddetailsdict.get(str(int(hhid)))
			if lhhiddetails:
				lhhiddetails.t14 =(v[64],v[65:68],v[85],v[71],v[72],v[73],v[87],v[91],v[95])
        fin.close()
'''

def populateTable14New(hhiddetailsdict, inputfile):
	v = open('hhcfileList','r')
	t = v.read().split('\n')
	t.remove('')
	for elements in list(set(t)):
		linputfile = path+elements
		try:
			fin = open(linputfile,"r");
		except:
			linputfile = newpath+elements
			fin = open(linputfile,"r");
			pass

		for v in fin:
			if v[0:2] == '14':
				hhid = v[13:20]
				lhhiddetails = hhiddetailsdict.get(str(int(hhid)))
				if lhhiddetails:
					lhhiddetails.t14 =(v[64],v[65:68],v[85],v[71],v[72],v[73],v[87],v[91],v[95])
		fin.close()


def populatehhiddetails(inputfile):
    fin = open(inputfile,"r");
    hhiddetailsdict = {};
    for v in fin:
        if v[0:2] == '20':
            key = str(int(v[13:20]));
            if hhiddetailsdict.get(key) is None:
                lhhiddetails = hhiddetails();
                lhhiddetails.fillhhiddetails(v);
                hhiddetailsdict[key] = lhhiddetails;

    return hhiddetailsdict;

def populatehhiddetailsold(inputfile, hhiddetailsdict):
    fin = open(inputfile,"r");
    #hhiddetailsdict = {};
    for v in fin:
        if v[0:2] == '20':
            key = str(int(v[13:20]));
            if hhiddetailsdict.get(key) is None:
                lhhiddetails = hhiddetails();
                lhhiddetails.fillhhiddetails(v);
                hhiddetailsdict[key] = lhhiddetails;


def printdetails(clusterhhidmappingdict, hhiddetailsdict):
	header = 'HHID\tCluster\tDv1\tDv2\t';
	header+='GeoTerrCode\tTime Zone Code\tCounty Size Code\tHead of Household Race\tLanguage of the Household\t';
	header+='Presence of Children\tPresence of Children < 2\tPresence of Children < 3\tPresence of Children 2 to 5\t';
	header+='Presence of Children < 6\tPresence of Children 6 to 11\tPresence of Children 12 to 17\tNumber of Persons Kids less than 3\t';
	header+='Number of Persons Children\tNumber of Persons Adults\tHousehold Size Code\tHousehold Income Ranges\tNumber of Incomes\t';
	header+='Head of Household Age Break\tHead of Household Age\tHead of Household Education\tHead of Household Occupation\tLady of House Present Flag\t';
	header+='Lady of House Occupation Code\tWired Cable\tPay Channels\tCable Plus\tAlternate Delivery System\tWired Digital Cable\tDBS Owner\tDVD Owner\t';
	header+='Presence of DVR\tNumber of TV Sets\tNumber of TV Sets with Pay\tNumber of TV Sets with Wired Cable\tNumber of TV Sets with Wired Cable and Pay\t';
	header+='Number of VCRs\tVideo Game Owner\tHousehold Online Date\tHead of Household Origin Code\tHead of Household Hispanic Specific Ethnicity\t';
	header+='Number of DVRs\tHousehold with Cable Services via Telco\tNumber of Cars\tNumber of Trucks\tNew Car Prospect Last 3 Years\tNew Car Prospect Last 5 Years\t';
	header+='New Truck Prospect Last 3 Years\tNew Truck Prospect Last 5 Years\tDomestic Vehicle Indicator\tForeign Vehicle Indicator\tDog Indicator\tCat Indicator\t';
	header+='Long Distance Carrier Code\tPC Access-Home Indicator\tPC Access with Internet Access- Home Indicator\tHousehold Income Ranges Detailed\tHousehold Income Amount\t';
	header+='Household Income Non Working\tHead of Household Gender\tHead of Household Works Outside Home\tMetered Market Flag\tHome Ownership Status Code\tHome Structure Type\t';
	header+='Home Ownership Secondary Home Status\tBeverage Usage Bottled Water\tBeverage Usage Coffee or Tea\tBeverage Usage Soft Drinks\tBeverage Usage Table Wine\t';
	header+='NSI Market Rank (Ranges)\tLPMFlag\tLPMMarketCode\tAsianHHInd\tNOCC\tNOLC\tNODC\tNOT\tNOS\tNOPMP\n';
	#fout = open('/home/account1/hapandleonard/testoutput/HHProfileModelhapandleonardTO.csv','w');
	#fout = open('/disk2/itot/gfe/HHProfileModel.csv','w');
	fout = open('/disk2/Projects/outsiders2output/HHProfileModel1.csv','w');
	fout.write(header);
	delimiter = '\t';
	for key in clusterhhidmappingdict:
		if hhiddetailsdict.get(str(key)) is not None:
			lclusterdv2 = clusterhhidmappingdict[key];
			dv1 = '1';
			if lclusterdv2.clusterid == 2:
				dv1 = '0';
			result = str(key)+'\t'+str(lclusterdv2.clusterid)+'\t'+dv1+'\t'+lclusterdv2.dv2+'\t';
			lhhiddetails = hhiddetailsdict[str(key)];
			result+=lhhiddetails.h37+delimiter;
			result+=lhhiddetails.h38+delimiter;
			result+=lhhiddetails.h39+delimiter;
			result+=lhhiddetails.h40+delimiter;
			result+=lhhiddetails.h41+delimiter;
			result+=lhhiddetails.h42+delimiter;
			result+=lhhiddetails.h43+delimiter;
			result+=lhhiddetails.h44+delimiter;
			result+=lhhiddetails.h45+delimiter;
			result+=lhhiddetails.h46+delimiter;
			result+=lhhiddetails.h47+delimiter;
			result+=lhhiddetails.h48+delimiter;
			result+=lhhiddetails.h49+delimiter;
			result+=lhhiddetails.h50+delimiter;
			result+=lhhiddetails.h51+delimiter;
			result+=lhhiddetails.h52+delimiter;
			result+=lhhiddetails.h53+delimiter;
			result+=lhhiddetails.h55+delimiter;
			result+=lhhiddetails.h56+delimiter;
			result+=lhhiddetails.h57+delimiter;
			result+=lhhiddetails.h59+delimiter;
			result+=lhhiddetails.h60+delimiter;
			result+=lhhiddetails.h61+delimiter;
			result+=lhhiddetails.h62+delimiter;
			result+=lhhiddetails.h64+delimiter;
			result+=lhhiddetails.h65+delimiter;
			result+=lhhiddetails.h66+delimiter;
			result+=lhhiddetails.h67+delimiter;
			result+=lhhiddetails.h68+delimiter;
			result+=lhhiddetails.h69+delimiter;
			result+=lhhiddetails.h70+delimiter;
			result+=lhhiddetails.h71+delimiter;
			result+=lhhiddetails.h72+delimiter;
			result+=lhhiddetails.h74+delimiter;
			result+=lhhiddetails.h76+delimiter;
			result+=lhhiddetails.h78+delimiter;
			result+=lhhiddetails.h80+delimiter;
			result+=lhhiddetails.h82+delimiter;
			result+=lhhiddetails.h83+delimiter;
			result+=lhhiddetails.h91+delimiter;
			result+=lhhiddetails.h92+delimiter;
			result+=lhhiddetails.h94+delimiter;
			result+=lhhiddetails.h95+delimiter;
			result+=lhhiddetails.h98+delimiter;
			result+=lhhiddetails.h100+delimiter;
			result+=lhhiddetails.h102+delimiter;
			result+=lhhiddetails.h103+delimiter;
			result+=lhhiddetails.h104+delimiter;
			result+=lhhiddetails.h105+delimiter;
			result+=lhhiddetails.h106+delimiter;
			result+=lhhiddetails.h107+delimiter;
			result+=lhhiddetails.h108+delimiter;
			result+=lhhiddetails.h109+delimiter;
			result+=lhhiddetails.h110+delimiter;
			result+=lhhiddetails.h111+delimiter;
			result+=lhhiddetails.h112+delimiter;
			result+=lhhiddetails.h113+delimiter;
			result+=lhhiddetails.h115+delimiter;
			result+=lhhiddetails.h120+delimiter;
			result+=lhhiddetails.h121+delimiter;
			result+=lhhiddetails.h122+delimiter;
			result+=lhhiddetails.h123+delimiter;
			result+=lhhiddetails.h124+delimiter;
			result+=lhhiddetails.h125+delimiter;
			result+=lhhiddetails.h126+delimiter;
			result+=lhhiddetails.h127+delimiter;
			result+=lhhiddetails.h128+delimiter;
			result+=lhhiddetails.h129+delimiter;
			result+=lhhiddetails.h130+delimiter;
			result+=lhhiddetails.h131+delimiter;
			result+=delimiter.join(lhhiddetails.t14)
			fout.write(result+'\n');
	fout.close();


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


if __name__ == "__main__":
	#clusterhhidmappingdict = clusterhhidmapping('/home/account1/mtvteenwolf/testoutput/kmeansoutput.csv');
	clusterhhidmappingdict = clusterhhidmapping('/disk2/Projects/outsiders2output/kmeansoutput.csv');
	#hhiddetailsdict = populatehhiddetails('/home/account1/marymary/NPM_ARD_W_2015_08_23_EHH_R0/N150830.EHH');
	hhiddetailsdict = populatehhiddetails('/home/account1/marymary/NPM_ARD_W_2015_09_27_EHH_R0/N150930.EHH');
	ehhfilelist = list(set(ehhfilelist))
	for elements in ehhfilelist:
		populatehhiddetailsold(elements, hhiddetailsdict)
	#tt = populatehhiddetails('/home/account1/marymary/NPM_ARD_W_2014_06_22_EHH_R0/N140630.EHH')
	#populateTable14(hhiddetailsdict, '/home/account1/marymary/NPM_MRD_W_2016_01_31_HHC_R0/W160140.HHC')
	populateTable14New(hhiddetailsdict, 'hhcfileList')
	printdetails(clusterhhidmappingdict, hhiddetailsdict);
	print 'arnab'