#!/usr/bin/python
import MySQLdb

def allprogram():
    con = MySQLdb.connect('localhost','linked_admin', 'EyAeRxy4PH3Ut5L8','linked',  unix_socket="/var/lib/mysql/mysql.sock")
    with con:  
        cur = con.cursor()
        cur.execute("DROP TABLE IF EXISTS programdetails")
        cur.execute("CREATE TABLE programdetails(NetworkID VARCHAR(6),\
                     ProgramDate DATE,\
                     TelecastN INT(7),\
                     shareoffreq DECIMAL(20,8),\
                     ProgramFinalName VARCHAR(100),\
                     PRIMARY KEY (NetworkID,TelecastN))")
    print "1:An empty TABLE programdetails has been created."

