class hhidpersoniddetails:

    def __init__(self):
        self.hhid = '';
        self.personid = '';
        self.intabwt = [];
        self.scalarwt = [];

    def fillhhidpersoniddetails(self,v):
        self.hhid = v[13:20];
        self.personid = v[20:22];

        self.intabwt.append(v[22:28]);
        self.intabwt.append(v[28:34]);
        self.intabwt.append(v[34:40]);
        self.intabwt.append(v[40:46]);
        self.intabwt.append(v[46:52]);
        self.intabwt.append(v[52:58]);
        self.intabwt.append(v[58:64]);

        self.scalarwt.append(v[64:71]);
        self.scalarwt.append(v[71:78]);
        self.scalarwt.append(v[78:85]);
        self.scalarwt.append(v[85:92]);
        self.scalarwt.append(v[92:99]);
        self.scalarwt.append(v[99:106]);
        self.scalarwt.append(v[106:113]);


def populatehhidweightdetails(inputfile):
    fin = open(inputfile,"r");
    datehhidpersoniddetails = {};
    for v in fin:
        if v[0:2] == '26':
            hhid = v[13:20];
	    personid = v[20:22];
            #key = v[5:13]+':'+hhid+':'+personid;
            key = v[5:13]+':'+hhid+personid;
            if datehhidpersoniddetails.get(key) is None:
                lhhidpersoniddetails = hhidpersoniddetails();
                lhhidpersoniddetails.fillhhidpersoniddetails(v);
                datehhidpersoniddetails[key] = lhhidpersoniddetails;

    return datehhidpersoniddetails;
