import allcableaffinity;
import allbroadcastaffinity
import collections;
import commonutils
import collections
import sh1
import sh2
import sh3
import sh4
import sh5
import sh6
import sh7
import sh8

sviList = ['1','2','3']

def printlist(inputlist, inputstring = None):
	result = '';
	v = '\t';
	if inputstring != None:
		v += inputstring+'\t'
	for elements in inputlist:
		result+=elements+v;
	return result;

def finalCount(affinitystringdict,elements1):
        cc = elements1.split('SVI')
        viewfreq = 0
        totalmin = 0 
        svi = set()
        finalsvi = '0'
        for elements in sviList:
                affinitystringdictvalue = affinitystringdict.get(cc[0]+elements)
                if affinitystringdictvalue:
                        viewfreq+=affinitystringdictvalue.viewfreq
                        totalmin+=affinitystringdictvalue.totalmin
                        svi.add(elements)
        if '1' in svi and not set(['2','3']) & svi:
                finalsvi = '1'
        elif '1' not in svi:
                finalsvi = '2'
        elif set(['1','2','3']) & svi:
                finalsvi = '3'
        return '\t'.join([cc[0]+finalsvi,str(viewfreq), str(totalmin)])


def getheaderstring(affinitystringdict, headerstring, totalfreq, totalmin):
        verbose = ['\t0\0\t0\t0'] * len(headerstring);
        #use list comprehension
        counter = 0;
        for elements1 in headerstring:
                result = finalCount(affinitystringdict,elements1)
                verbose[counter] = result;
                counter+=1;
        return printlist(verbose);


def printaffinitydetails(affinitydetails, headerstring):
	fout = open('/disk2/Projects/outsiders2output/IT-HHID-ShowAffinityNew.csv','w');
	#header = 'HHID\t'+printlist(headerstring,'TotalFrequency\tTotalMinutes\tTotalFreqPercent\tTotalMinPercent')+'\n';
	header = 'HHID\tHHIDInTabWt\t'+printlist(headerstring,'TotalFrequency\tTotalMinutes')+'\n';
	fout.write(header);
	for keys in affinitydetails:
		
		laffinity = affinitydetails[keys];
		hhidtabwt = '0'
		if commonutils.tabwtmeandict.get(int(keys)):
			hhidtabwt = str(commonutils.tabwtmeandict[int(keys)])
		
		result = keys+'\t'+hhidtabwt+'\t'+getheaderstring(laffinity.affinitystringdict, headerstring,laffinity.viewfreq,laffinity.totalmin)+'\n';
		fout.write(result);
	fout.close();
	


if __name__ == "__main__":
	#path = '/home/account1/TeenWolves/';
        path = '/home/account1/marymary/';
	commonutils.readViewerShipFile('/disk2/Projects/outsiders2output/IT-HH-ViewershipDayPartsNew.csv')
	affinitydetails = collections.OrderedDict();
	headerstring = [];
	sh1.cableoutput(path,affinitydetails, allcableaffinity);
	headerstring += list(allcableaffinity.headerstring);
	sh2.cableoutput(path,affinitydetails, allcableaffinity);
	headerstring += list(allcableaffinity.headerstring);
	sh3.cableoutput(path,affinitydetails, allcableaffinity);
	headerstring += list(allcableaffinity.headerstring);
	sh4.cableoutput(path,affinitydetails, allcableaffinity);
	headerstring += list(allcableaffinity.headerstring);
	sh5.cableoutput(path,affinitydetails, allcableaffinity);
	headerstring += list(allcableaffinity.headerstring);
	sh6.cableoutput(path,affinitydetails, allcableaffinity);
	headerstring += list(allcableaffinity.headerstring);
	sh7.cableoutput(path,affinitydetails, allcableaffinity);
	headerstring += list(allcableaffinity.headerstring);
	sh8.cableoutput(path,affinitydetails, allcableaffinity);
	headerstring += list(allcableaffinity.headerstring);
	printaffinitydetails(affinitydetails, headerstring);
	print 'arnab';
