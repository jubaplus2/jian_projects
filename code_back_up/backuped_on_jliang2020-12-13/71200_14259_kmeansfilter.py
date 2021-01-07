import pandas as pd;
import collections;

class hhidstat:

    def __init__(self):
        self.bf = 0;
        self.bct = 0;
        self.vf = 0;
        self.tm = 0;

def process(inputfile):
	v = pd.read_csv(inputfile, sep='\t');
	inputdict = collections.OrderedDict();
	counter = 0;
	limit = len(v);
	while counter < limit:
		hhid = v.iloc[counter][0];
		bf = v.iloc[counter][5];
		bct = v.iloc[counter][6];
		vf = v.iloc[counter][7];
		tm = v.iloc[counter][8];
		counter+=1;
		if inputdict.get(hhid) is None:
			lhhidstat = hhidstat();
		else:
			lhhidstat = inputdict[hhid];
		lhhidstat.bf+=bf;
		lhhidstat.bct+=bct;
		lhhidstat.vf+=vf;
		lhhidstat.tm+=tm;
		inputdict[hhid] = lhhidstat;
	return inputdict;

def processoutput(inputdict):
	#fout = open('/home/account1/mtvteenwolf/testoutput/kmeansinput.csv','w');
	fout = open('/disk2/Projects/outsiders2output/kmeansinput.csv','w');
        header = 'HHID\tTotalBroadCastFrequency\tTotalBroadCastTime\tViewershipFrequency\t';
        header+='TotalMinutes\tShareOfProgramViewershipMinutes\tShareOfProgramViewershipFrequency\n';
        fout.write(header);
	for keys in inputdict:
		lhhidstat = inputdict[keys];
		shareOfProgramViewershipMinutes = str(round(lhhidstat.tm * 100/float(lhhidstat.bct), 4));
		shareOfProgramViewershipFrequency = str(round(lhhidstat.vf * 100/float(lhhidstat.bf), 4));
		result = str(keys)+'\t'+str(lhhidstat.bf)+'\t'+str(lhhidstat.bct)+'\t'+str(lhhidstat.vf)+'\t'+str(lhhidstat.tm)+'\t';
		result+=shareOfProgramViewershipMinutes+'\t'+shareOfProgramViewershipFrequency+'\n';
		fout.write(result);
	fout.close();

if __name__ == "__main__":
	#inputdict = process('/home/account1/betrealhusholy/testoutput/IT-HH-ViewFreqMinShares2BETRealH.csv');
	inputdict = process('/disk2/Projects/outsiders2output/IT-HH-ViewFreqMinShares2.csv')
	processoutput(inputdict);
