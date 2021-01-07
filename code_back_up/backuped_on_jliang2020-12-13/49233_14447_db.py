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
