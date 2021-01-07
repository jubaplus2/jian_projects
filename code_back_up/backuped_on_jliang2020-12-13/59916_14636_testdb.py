from sqlalchemy import *

db = create_engine('mysql://root:jubaplus2015@localhost/RentrakDB?charset=utf8&use_unicode=0', pool_recycle=3600)
db.echo = False  # Try changing this to True and see what happens
metadata = MetaData(db)

def createTable(rentrakoutput,showID,networkNo, showName,USNG_100, lzip,zip4,rank):
	try:
		result = db.execute('drop table %s'%rentrakoutput)
	except Exception as e:
		pass
	users = Table(rentrakoutput, metadata,
	Column(showID, String(40)),
	Column(networkNo, String(40)),
	Column(showName, String(40)),
	Column(USNG_100, String(40)),
	Column(lzip, String(40)),
	Column(zip4, String(40)),
	Column(rank, String(40)),
	)
	users.create()
	return users

def insertDB(users):
	i = users.insert()
	#v = {'ShowID' :'2150186', 'Network_no':'410','ShowName':'Kendra on Top', 'USNG_100': '04QDK263297', 'Zip': '96752', 'Zip4': '""','Rank20150907-20150920': '1'}
	v = {'ShowID': '2150186', 'Zip': '96752', 'Rank20150907-20150920': '1', 'Network_no': '410', 'USNG_100': '04QDK263297', 'ShowName': 'Kendra on Top', 'Zip4': '""'}
	v1 = {'ShowID' :'2150186', 'Network_no':'410','ShowName':'Kendra on Top', 'USNG_100': '04QDK263297', 'Zip': '96752', 'Zip4': '0001','Rank20150907-20150920': '2'}
	#i.execute(ShowID = '2150186', Network_no = '410',ShowName = 'Kendra on Top', USNG_100 = '04QDK263297', Zip = '96752' ,Zip4 = '0002',Rank20150907-20150920 = '1')
	#i.execute(ShowID = '2150186', Network_no = '410',ShowName = 'Kendra on Top', USNG_100 = '04QDK263297', Zip = '96752' ,Zip4 = '0001',Rank20150907-20150920 = '2')
	i.execute(v)
	i.execute(v1)
	s = users.select()
	rs = s.execute()
	#row = rs.fetchone()
	row = rs.fetchall()
	print row
	lusers = Table('rentrakoutput11', metadata, autoload=True)
	s = lusers.select()
	rs = s.execute()
	row = rs.fetchall()
	print row

def createTableN(rentrakoutput,inputlist):
        try:
                result = db.execute('drop table %s'%rentrakoutput)
        except Exception as e:
                pass
        users = Table(rentrakoutput, metadata,
        Column(inputlist[0], String(40)),
        Column(inputlist[1], String(40)),
        Column(inputlist[2], String(40)),
        Column(inputlist[3], String(40)),
        Column(inputlist[4], String(40)),
        Column(inputlist[5], String(40)),
        Column(inputlist[6], String(40)),
        Column(inputlist[7], String(40)),
        )
        users.create()
        return users

#users.create()
path = '/disk2/Projects/Rentrak/20150907-20150920/'
users = createTable('rentrakoutput11', 'ShowID', 'Network_no', 'ShowName', 'USNG_100', 'Zip', 'Zip4', 'Rank'+path.split('/')[-2])
insertDB(users)
inputlist = ['ShowID', 'Network_no', 'ShowName', 'USNG_100', 'Zip', 'Zip4', 'Rank20150907-20150920', 'Rank20150921-20151004']
import pdb
pdb.set_trace()
users = createTableN('rentrakoutputW1', inputlist)
'''
i = users.insert()
i.execute(name='Mary', age=30, password='secret')
i.execute({'name': 'John', 'age': 42},
          {'name': 'Susan', 'age': 57},
          {'name': 'Carl', 'age': 33})

s = users.select()
rs = s.execute()
row = rs.fetchone()
print 'Id:', row[0]
print 'Name:', row['name']
print 'Age:', row.age
print 'Password:', row[users.c.password]

for row in rs:
    print row.name, 'is', row.age, 'years old'
'''
