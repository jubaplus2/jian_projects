import pandas as pd; 
import collections;
v37 = ['1','2','3','4','5','6','7'];
v39 = ['6','7','8','9'];
v01 = ['0', '1'];
#1-6 = <50k, 7-8 = 50-74k, 9 = 75-100, 10 = 101 -124, 11= 125+
lv53 = ['<50k','50-74k','75-100k','101-124k','>125k'];
v53 = {};
v53[1] = '<50k';
v53[2] = '<50k';
v53[3] = '<50k';
v53[4] = '<50k';
v53[5] = '<50k';
v53[6] = '<50k';
v53[7] = '50-74k';
v53[8] = '50-74k';
v53[9] = '75-100k';
v53[10] = '101-124k';
v53[11] = '>125k';
v6 = ['1','2','3','4','5','6'];
v5 = ['1','2','3','4','5'];
v62 = ['0','1','2','3','4','5','6', '7','8', '9'];
#h91 = 'Head of Household Origin Code'; 1 should be 1 and 2 should be 0
v113 = ['10k','15k','25k','35k','45k','55k','65k','75k','85k','95k','105k','115k','125k','135k','145k','155k','165k','175k','185k','195k','200k'];
l113 = {};
l113[1] =  '10k'
l113[2] = '15k'
l113[3] = '25k'
l113[4] = '35k'
l113[5] = '45k'
l113[6] = '55k'
l113[7] = '65k'
l113[8] = '75k'
l113[9] = '85k'
l113[10] = '95k'
l113[11] = '105k'
l113[12] = '115k'
l113[13] = '125k'
l113[14] = '135k'
l113[15] = '145k'
l113[16] = '155k'
l113[17] = '165k'
l113[18] = '175k'
l113[19] = '185k'
l113[20] = '195k'
l113[21] = '200k';

def printlist(inputstring,inputlist):
	outputlist = [inputstring + elements for elements in inputlist];
	result = '\t'.join(map(str,outputlist));
	result+='\t';
	return result;

def generateoutputlist(inputvalue,inputlist):
	v = ['0'] *len(inputlist);
	v[inputlist.index(str(int(inputvalue)))] = '1';
	return v;

def generateoutputliststring(inputvalue,inputlist):
	v = ['0'] *len(inputlist);
	v[inputlist.index(inputvalue)] = '1';
	return v;

def processheader(inputstring):
	t = inputstring.split('\t');
	result = '\t'.join(map(str,t[0:4]))+'\t'+printlist(t[4],v37)+printlist(t[5],v5)+printlist(t[6],v39); 
	result+=printlist(t[7],v01)+printlist(t[8],v01)+'\t'.join(map(str,t[9:20]))+'\t'+printlist(t[20],lv53);
	result+=t[21]+'\t'+printlist(t[22],v6)+t[23]+'\t'+printlist(t[24],v5)+printlist(t[25],v6)+t[26]+'\t';
	result+=printlist(t[27],v62)+'\t'.join(map(str,t[28:43]))+'\t'+printlist(t[43],v01)+'\t'.join(map(str,t[44:60]))+'\t'+printlist(t[60],v113)+'\t'.join(map(str,t[61:73]))+'\t';
	result+=printlist(t[73].replace('\n',''),v5)+'\t'.join(t[74:]);
	return result;

def getbinYN(inputstring):
	result = '0';
	if inputstring == 'Y':
		result = '1';
	return result;
	

def processoutput(inputstring):
	inputstring = inputstring.replace('\n','')
	t = inputstring.split('\t');
	#t.remove('\n');
	l4 = generateoutputlist(t[4],v37);
	l5 = generateoutputlist(t[5],v5);
	l6 = generateoutputlist(t[6],v39);
	ll7 = '1';
	if t[7] == '2':
		ll7 = '0';
	l7 = generateoutputlist(ll7,v01);
	ll8 = '1';
	if t[8] == '5':
		ll8 = '0';
	l8 = generateoutputlist(ll8,v01);
	result = '\t'.join(map(str,t[0:4]))+'\t'+'\t'.join(map(str,l4))+'\t'+'\t'.join(map(str,l5))+'\t';
	result+='\t'.join(map(str,l6))+'\t'+'\t'.join(map(str,l7))+'\t'+'\t'.join(map(str,l8))+'\t';
	result+=getbinYN(t[9])+'\t'+getbinYN(t[10])+'\t'+getbinYN(t[11])+'\t'+getbinYN(t[12])+'\t'+getbinYN(t[13])+'\t'+getbinYN(t[14])+'\t'+getbinYN(t[15])+'\t';
	result+='\t'.join(map(str,t[16:20]))+'\t';
	ll53 = generateoutputliststring(v53[int(t[20])],lv53);
	result+='\t'.join(map(str,ll53))+'\t';
	v = '0';
	if t[21] != '':
		v = t[21]; 	
	l22 = generateoutputlist(t[22],v6);
	result+=v+'\t'+'\t'.join(map(str,l22))+'\t'+t[23]+'\t';
	result+='\t'.join(map(str,generateoutputlist(t[24],v5)))+'\t'
	result+='\t'.join(map(str,generateoutputlist(t[25],v6)))+'\t'+getbinYN(t[26])+'\t';
	result+='\t'.join(map(str,generateoutputlist(t[27],v62)))+'\t';
	result+=getbinYN(t[28])+'\t'+getbinYN(t[29])+'\t'+getbinYN(t[30])+'\t'+getbinYN(t[31])+'\t'+getbinYN(t[32])+'\t'+getbinYN(t[33])+'\t';
	result+=getbinYN(t[34])+'\t'+getbinYN(t[35])+'\t';
	result+='\t'.join(map(str,t[36:41]))+'\t'+getbinYN(t[41])+'\t'+t[42]+'\t';
	ll43 = '1';
	if t[43] == '2':
		ll43 = '0';
	l43 = generateoutputlist(ll43,v01);
	result+='\t'.join(map(str,l43))+'\t'+'\t'.join(map(str,t[44:46]))+'\t'+getbinYN(t[46])+'\t'+'\t'.join(map(str,t[47:49]))+'\t';
	result+=getbinYN(t[49])+'\t'+getbinYN(t[50])+'\t'+getbinYN(t[51])+'\t'+getbinYN(t[52])+'\t'+getbinYN(t[53])+'\t'+getbinYN(t[54])+'\t';
	result+=getbinYN(t[55])+'\t'+getbinYN(t[56])+'\t'+getbinYN(t[57])+'\t'+getbinYN(t[58])+'\t'+getbinYN(t[59])+'\t';
	ll113 = generateoutputliststring(l113[int(t[60])],v113);
	result+='\t'.join(map(str,ll113))+'\t'+t[61]+'\t';
	result+=getbinYN(t[62])+'\t'+getbinYN(t[63])+'\t'+getbinYN(t[64])+'\t'+getbinYN(t[65])+'\t'+getbinYN(t[66])+'\t'+getbinYN(t[67])+'\t';
	result+=getbinYN(t[68])+'\t'+getbinYN(t[69])+'\t'+getbinYN(t[70])+'\t'+getbinYN(t[71])+'\t'+getbinYN(t[72])+'\t';
	result+='\t'.join(map(str,generateoutputlist(t[73],v5)))+'\t';
	result+=getbinYN(t[74])+'\t'+t[75]+'\t'+getbinYN(t[76])+'\t'+'\t'.join(t[77:])
	return result;
	
	

if __name__ == "__main__":
	f = open('/disk2/Projects/outsiders2output/HHProfileModel1.csv','r');
	v = f.readlines();
	fout = open('/disk2/Projects/outsiders2output/HHProfileModelAnalytical1.csv','w');
	header = processheader(v[0]);
	fout.write(header);
	counter = 1;
	limit = len(v);
	while counter < limit:
		fout.write(processoutput(v[counter])+'\n');
		counter+=1;
	fout.close();
	print 'arnab1'