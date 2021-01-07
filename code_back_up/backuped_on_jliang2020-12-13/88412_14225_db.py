from sqlalchemy import *

db = create_engine('mysql://root:jubaplus2015@localhost/testRentrak?charset=utf8&use_unicode=0', pool_recycle=3600)

db.echo = False  # Try changing this to True and see what happens

metadata = MetaData(db)

users = Table('rentrakoutput', metadata,
    Column('showID', String(40), primary_key=True),
    Column('showName', String(40)),
    Column('USNG_100', String(40)),
    Column('zip', String(40)),
    Column('zip4', String(40)),
)
users.create()

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
