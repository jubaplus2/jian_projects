class hhiddetails:

    def __init__(self):
        self.hhid = '';
        self.intabwt = [];
        self.scalarwt = [];
        self.inttabstatus = [];

    def fillhhiddetails(self,v):
        self.hhid = v[13:20];

        self.intabwt.append(v[20:26]);
        self.intabwt.append(v[26:32]);
        self.intabwt.append(v[32:38]);
        self.intabwt.append(v[38:44]);
        self.intabwt.append(v[44:50]);
        self.intabwt.append(v[50:56]);
        self.intabwt.append(v[56:62]);

        self.scalarwt.append(v[62:69]);
        self.scalarwt.append(v[69:76]);
        self.scalarwt.append(v[76:83]);
        self.scalarwt.append(v[83:90]);
        self.scalarwt.append(v[90:97]);
        self.scalarwt.append(v[97:104]);
        self.scalarwt.append(v[104:111]);

        self.inttabstatus.append(v[11]);
        self.inttabstatus.append(v[12]);
        self.inttabstatus.append(v[13]);
        self.inttabstatus.append(v[14]);
        self.inttabstatus.append(v[15]);
        self.inttabstatus.append(v[16]);
        self.inttabstatus.append(v[17]);


def populatehhidweightdetails(inputfile):
    fin = open(inputfile,"r");
    datehhidwtdetails = {};
    for v in fin:
        if v[0:2] == '25':
            hhid = v[13:20];
            key = v[5:13]+':'+hhid;
            if datehhidwtdetails.get(key) is None:
                lhhiddetails = hhiddetails();
                lhhiddetails.fillhhiddetails(v);
                datehhidwtdetails[key] = lhhiddetails;

    return datehhidwtdetails;
