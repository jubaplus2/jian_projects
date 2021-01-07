import otfileprocess;
import otclonemodel;
import commonutils;

def printlist(inputlist, filler):
	result = '';
	for elements in inputlist:
		result+=elements+filler+'\t';
	return result;


if __name__ == "__main__":
	#fout = open('/home/account1/mtvteenwolf/testoutput/OT-CloneModel.csv','w');
	path = '/home/account1/';
	with open('/disk2/Projects/sundancerectifyoutput/OTRaw3.csv','w') as fout:
		header  = 'CableID\tProgramcode\tTelecastN\tViewStartTime\tViewEndTime\tHHID\tPersonID\tCableName\tProgramName\tDate\tStartTime\t'
		header+='Classification Date\tVCR Record Only Indicator\tTime Shifted Viewing Code\tMinimum Play Delay Minutes\tViewing Platform Code\tRecent-Telecast VOD Indicator'

		fout.write(header+'\n')
		otfileprocess.cableoutputotfile(path,fout, otclonemodel);
		otclonemodel.merging(fout);
	print 'arnab done'
