import allcable;
import allbroadcast
import sh1
import sh2
import sh3
import sh4
import sh5
import sh6
import sh7
if __name__ == "__main__":
	#fout = open('/home/account1/mtvteenwolf/testoutput/IT-HH-ViewershipDayParts2-1MTVTeenWolf.csv','w');
        fout = open('/disk2/Projects/outsiders2output/IT-HH-ViewershipDayPartsNew.csv','w');
	header = 'Cable Network ID\tProgram Code\tTelecast Number\tBroadcast Date\tProgram Distributor\tProgram Name\tEpisode Name\tReported Duration (Program Duration)\tReported Start Time (Program)\tClassification Date (Viewership Date)\tHousehold Number\tPerson ID\tHHID_PersonID\tAge\tSex Code\tStart Minute of Program Viewing\tEnd Minute of Program Viewing\tView Length\tVCR Record Only Indicator\tTime Shifted Viewing Code\tMinimum Play Delay Minutes\tViewing Platform Code\tRecent-Telecast VOD Indicator\tPerson In-tab Weight\tHH In-tab Weight\n'

	fout.write(header);

	#path = '/home/account1/TeenWolves/';
        path = '/home/account1/marymary/';
	sh1.cableoutput(path,fout, allcable);
	sh2.cableoutput(path,fout, allcable);
	sh3.cableoutput(path,fout, allcable);
	sh4.cableoutput(path,fout, allcable);
	sh5.cableoutput(path,fout, allcable);
	sh6.cableoutput(path,fout, allcable);
	sh7.cableoutput(path,fout, allcable);
	print 'Part 1 over';
        fout.close();
