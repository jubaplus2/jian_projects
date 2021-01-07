from db import *

def insertDB(users):
        i = users.insert()
        v = {'ShowID': '2150186', 'Zip': '96752', 'Rank20150907-20150920': '1', 'Network_no': '410', 'USNG_100': '04QDK263297', 'ShowName': 'Kendra on Top', 'Zip4': '""'}
        v1 = {'ShowID' :'2150186', 'Network_no':'410','ShowName':'Kendra on Top', 'USNG_100': '04QDK263297', 'Zip': '96752', 'Zip4': '0001','Rank20150907-20150920': '2'}
        i.execute(v)
        i.execute(v1)
        s = users.select()
        rs = s.execute()
        #row = rs.fetchone()
        row = rs.fetchall()
        print row

path = '/disk2/Projects/Rentrak/20150907-20150920/'
#users = createTable('rentrakoutput11', 'ShowID', 'Network_no', 'ShowName', 'USNG_100', 'Zip', 'Zip4', 'Rank'+path.split('/')[-2])
users = createTable('rentrakoutput11', 'ShowID', 'Network_no', 'ShowName', 'USNG_100', 'Zip', 'Zip4', 'Rank'+path.split('/')[-2])
i = users.insert()
v = {'ShowID': '2150186', 'Zip': '96752', 'Rank20150907-20150920': '1', 'Network_no': '410', 'USNG_100': '04QDK263297', 'ShowName': 'Kendra on Top', 'Zip4': '""'}
v1 = {'ShowID' :'2150186', 'Network_no':'410','ShowName':'Kendra on Top', 'USNG_100': '04QDK263297', 'Zip': '96752', 'Zip4': '0001','Rank20150907-20150920': '2'}
i.execute(v)
i.execute(v1)
s = users.select()
rs = s.execute()
#row = rs.fetchone()
row = rs.fetchall()
print row
