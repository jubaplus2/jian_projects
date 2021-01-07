from sqlalchemy import *
#engine = create_engine('mysql://root:jubaplus2015@localhost/bedrock?charset=utf8&use_unicode=0', pool_recycle=3600)
engine = create_engine('mysql://root:jubaplus2015@localhost/tutoro?charset=utf8&use_unicode=0', pool_recycle=3600)
connection = engine.connect()
v = ('Arnab','Math','ComputerProgrammer')
vv = "insert into employee values %s" %str(v)
result = engine.execute(vv)
result = engine.execute("select name from employee")
print result
for row in result:
	print "name:", row['name']

