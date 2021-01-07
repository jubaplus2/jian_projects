import datetime;
import pandas as pd
import collections
#wetv = '007516';
#marriagebootcamp = '0373371';
#marriagebootcampstring = 'MARRIAGE BOOT CAMP       ';

dayLookup = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'];
#hourLookup = ['0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23'];
hourLookup = ['06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29'];

def getWeekDay(inputDate):
        from datetime import datetime;
        return datetime.strptime(inputDate , '%Y%m%d').weekday();

def getSunday(inputdate):
    v = getWeekDay(inputdate);
    value = 6 - v;
    if value != 0:
        Date = datetime.datetime.strptime(inputdate, "%Y%m%d");
        EndDate = Date + datetime.timedelta(days=value);
        return EndDate.strftime("%Y%m%d");
    else:
        return inputdate;

def getITlist(inputfile):
	v = open(inputfile,'r');
	t = v.read();
	itlist= t.split('\n')
	itlist.remove('');
	return itlist;

tsvcDetails = {}
tabwtmeandict = {}

def readViewerShipFile(inputFile):
        v = pd.read_csv(inputFile, sep = '\t')
        meanCounter = collections.Counter(v['Household Number'])
        global tsvcDetails
        global tabwtmeandict
        for i,value in v.iterrows():
                hhidkeys = value['Household Number']
                hhidTabwts = value['HH In-tab Weight']
                programCode = value['Program Code']
                tsvc = value['Time Shifted Viewing Code']
                if  tabwtmeandict.get(hhidkeys):
                        tabwtmeandict[hhidkeys]+=hhidTabwts
                else:
                        tabwtmeandict[hhidkeys]=hhidTabwts
                tsvckey = str(hhidkeys)+':'+str(programCode)
                if tsvcDetails.get(tsvckey) is None:
                        tsvcDetails[tsvckey] = set()
                tsvcDetails[tsvckey].add(tsvc)

        for k,value in tabwtmeandict.iteritems():
                value = value/float(meanCounter[k])
                tabwtmeandict[k] = value


def getShiftViewIndicator(input):
	if tsvcDetails.get(input):
		value = tsvcDetails[input]
		if 1 in value and not set([2,3]) & value:
			return 1
		if 1 not in value:
			return 2
		if set([1,2,3]) & value:
			return 3
	else:
		return 0

def getITtupledict(inputfile):
        x = {};
        v = pd.read_csv(inputfile,sep = '\t')
        vlen = len(v);
        counter = 0;
        while counter < vlen:
                x[str(int(v.iloc[counter][0]))]= (str(int(v.iloc[counter][1])),str(int(v.iloc[counter][2])))
                counter+=1
        return x
