import allcablefreq;
import sh1
import sh2
import sh3
import sh4
import sh5
import sh6
import sh7

if __name__ == "__main__":
	fout = open('/disk2/Projects/outsiders2output/IT-HH-ViewFreqMinShares2.csv','w');
	header = 'HHID\tNetworkID/ProgramDistributor\tProgramID\tNetworkName\tProgramName\tTotalBroadCastFrequency\tTotalBroadCastTime\tViewershipFrequency\t';
	header+='TotalMinutes\tShareOfProgramViewershipMinutes\tShareOfProgramViewershipFrequency\tPersons\n';
	fout.write(header);

        #path = '/home/account1/TeenWolves/';
        path = '/home/account1/marymary/';
	sh1.cableoutput(path,fout, allcablefreq);
	allcablefreq.merging(fout);
	sh3.cableoutput(path,fout, allcablefreq);
	allcablefreq.merging(fout);
	sh7.cableoutput(path,fout, allcablefreq);
	allcablefreq.merging(fout);
	sh2.cableoutput(path,fout, allcablefreq);
	allcablefreq.merging(fout);
	sh4.cableoutput(path,fout, allcablefreq);
	allcablefreq.merging(fout);
	sh5.cableoutput(path,fout, allcablefreq);
	allcablefreq.merging(fout);
	sh6.cableoutput(path,fout, allcablefreq);
	allcablefreq.merging(fout);
	sh7.cableoutput(path,fout, allcablefreq);
	allcablefreq.merging(fout);
	print 'arnab';
        fout.close();
