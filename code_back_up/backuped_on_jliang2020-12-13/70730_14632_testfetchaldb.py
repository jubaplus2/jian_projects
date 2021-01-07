from sqlalchemy import *
db = create_engine('mysql://root:jubaplus2015@localhost/RentrakDB?charset=utf8&use_unicode=0', pool_recycle=3600)
db.echo = False  # Try changing this to True and see what happens
metadata = MetaData(db)
table = 'rentrakoutput'
'''
 dir(row)
['__add__', '__class__', '__contains__', '__delattr__', '__delitem__', '__delslice__', '__doc__', '__eq__', '__format__', '__ge__', '__getattribute__', '__getitem__', '__getslice__', '__gt__', '__hash__', '__iadd__', '__imul__', '__init__', '__iter__', '__le__', '__len__', '__lt__', '__mul__', '__ne__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__reversed__', '__rmul__', '__setattr__', '__setitem__', '__setslice__', '__sizeof__', '__str__', '__subclasshook__', 'append', 'count', 'extend', 'index', 'insert', 'pop', 'remove', 'reverse', 'sort']
(Pdb) row.headers()
*** AttributeError: 'list' object has no attribute 'headers'
(Pdb) row[1]
('2150186', '410', 'Kendra on Top', '04QDK263297', '96752', '0001', '1')
(Pdb) row[-1]
('1925577', '367', 'Watch What Happens Live', '19TFL407028', '04619', '4109', '1')
'''
lusers = Table(table, metadata, autoload=True)
coldetails = [c.name for c in lusers.columns]
print coldetails
s = lusers.select()
rs = s.execute()
row = rs.fetchall()
print row
