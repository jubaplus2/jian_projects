from sqlalchemy import *

#db = create_engine('sqlite:///tutorial.db')
db = create_engine('mysql://root:jubaplus2015@localhost/testbedrock?charset=utf8&use_unicode=0', pool_recycle=3600)

db.echo = False  # Try changing this to True and see what happens

metadata = MetaData(db)
try:
	v = 'users3'
	#result = db.execute('drop table users3')
	result = db.execute('drop table %s'%v)
except Exception as e:
	pass

def func():
	users = Table('users3', metadata,
	    #Column('user_id', Integer, primary_key=True),
	    Column('user_id', Integer),
	    Column('name', String(40)),
	    Column('age', Integer),
	    Column('password', String(40)),
	)
	users.create()
	return users

users = func()

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
