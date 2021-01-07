import pandas as pd; 
import collections;


v01 = ['0', '1'];
v5 = [' ','1','2','3','4','5'];
v9 = ['0','1','2','3','4','5','6','7','8','9'];
v12 = ['1','2','3','4','5','6', '7','8', '9', '10','11','12'];


def printlist(inputstring,inputlist):
	outputlist = [inputstring + elements for elements in inputlist];
	result = '\t'.join(map(str,outputlist));
	result+='\t';
	return result;

def generateoutputlist(inputvalue,inputlist):
	v = ['0'] *len(inputlist);
	v[inputlist.index(str(int(inputvalue)))] = '1';
	return v;

def generateoutputlistblank(inputvalue,inputlist):
	v = ['0'] *len(inputlist);
	if inputvalue == ' ':
		v[inputlist.index(inputvalue)] = '1';
	else:
		v[inputlist.index(str(int(inputvalue)))] = '1';
	return v;

def processheader(inputstring):
	t = inputstring.split('\t');
	result = '\t'.join(map(str,t[0:13]))+'\t';
	result+='\t'.join(map(str,t[13:15]))+'\t';
	result+=t[15]+'\t'+'\t'.join(map(str,t[16:18]))+'\t'+printlist(t[18],v12);
	result+=printlist(t[19],v9)+'\t'.join(map(str,t[20:22]))+'\t'+printlist(t[22],v5)+'\t'.join(map(str,t[23:25]));
	return result;

def getbinYN(inputstring):
	result = '0';
	if inputstring == 'Y':
		result = '1';
	return result;

def getgenderYN(inputstring):
	result = '0';
	if inputstring == 'M':
		result = '1';
	return result;

def getLangClassCode(inputstring):
	if int(inputstring) == 5 or int(inputstring) == 6:
		return '0';
	else:
		return '1';


def processoutput(inputstring):
	t = inputstring.split('\t');
	t.remove('\n');
	result='\t'.join(map(str,t[0:6]))+'\t';
	result+=getgenderYN(t[6])+'\t';
	result+='\t'.join(map(str,t[7:9]))+'\t';
	result+=getbinYN(t[9])+'\t';
	result+=getbinYN(t[10])+'\t';
	result+=getbinYN(t[11])+'\t'+getLangClassCode(t[12])+'\t';
	result+=getbinYN(t[13])+'\t';
	result+=getbinYN(t[14])+'\t'+t[15]+'\t';
	result+=getbinYN(t[16])+'\t';
	result+=getbinYN(t[17])+'\t';
	x18 = generateoutputlist(t[18],v12);
	result+='\t'.join(map(str,x18))+'\t';
	x19 = generateoutputlist(t[19],v9);
	result+='\t'.join(map(str,x19))+'\t';
	result+=getbinYN(t[20])+'\t';
	result+=getbinYN(t[21])+'\t';
	x22 = generateoutputlistblank(t[22],v5);
	result+='\t'.join(map(str,x22))+'\t';
	result+='\t'.join(map(str,t[23:25]))+'\t';
	return result;


if __name__ == "__main__":
	f = open('/disk2/Projects/outsiders2output/PersonModel.csv','r');
	v = f.readlines();
	fout = open('/disk2/Projects/outsiders2output/PersonModelAnalytical.csv','w');
	header = processheader(v[0]);
	fout.write(header);
	counter = 1;
	limit = len(v);
	while counter < limit:
		fout.write(processoutput(v[counter])+'\n');
		counter+=1;
	fout.close();
	print 'arnab1'
