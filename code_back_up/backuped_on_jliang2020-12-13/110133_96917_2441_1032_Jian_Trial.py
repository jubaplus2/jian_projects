import pymysql

def createdataset():
    con = pymysql.connect('localhost','root', 'JubaPlus-2017', "biglotsdata", unix_socket="/var/lib/mysql/mysql.sock")
    with con:  
        cur = con.cursor()
        cur.execute("DROP TABLE IF EXISTS detail_store_sales")
        cur.execute("CREATE TABLE detail_store_sales(location_id VARCHAR(10),\
                     week_end_dt DATE,\
                     fiscal_week_nbr INT(5),\
                     gross_sales_amt DECIMAL(20,2),\
                     gross_transaction_cnt INT(10),\
                     class_code_id VARCHAR(20),\
                     class_gross_sales_amt DECIMAL(20,2),\
                     PRIMARY KEY (location_id,week_end_dt))")
    print("1:An empty TABLE detail_store_sales has been created.")

createdataset()

import logging
import datetime
import os
import datetime
import gc
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

engine = create_engine('mysql://root:JubaPlus-2017@localhost/biglotsdata')
logging.basicConfig(filename='Log_Read_Detail_Store_Sales.log', level=logging.INFO)
logging.info('biglotslog')
logging.info(datetime.datetime.now())



filepath = ['/home/jubauser1/BigLots/MediaStormDataExtract/MediaStorm Data Extract - Detail Store Sales1 (stores up to 1000).txt',
'/home/jubauser1/BigLots/MediaStormDataExtract/MediaStorm Data Extract - Detail Store Sales2 (stores 1001 to 1450).txt',
           '/home/jubauser1/BigLots/MediaStormDataExtract/MediaStorm Data Extract - Detail Store Sales3 (stores 1451 to 1750).txt',
           '/home/jubauser1/BigLots/MediaStormDataExtract/MediaStorm Data Extract - Detail Store Sales4 (stores 1751 to 2000).txt',
           '/home/jubauser1/BigLots/MediaStormDataExtract/MediaStorm Data Extract - Detail Store Sales5 (stores 2001 to 4500).txt',
           '/home/jubauser1/BigLots/MediaStormDataExtract/MediaStorm Data Extract - Detail Store Sales6 (stores 4501 to 5000).txt',
           '/home/jubauser1/BigLots/MediaStormDataExtract/MediaStorm Data Extract - Detail Store Sales7 (stores 5001 to 6990).txt'] 

for i in filepath:
    df_detail_store_sales = pd.read_table(i,sep = '|',na_values=['?'])
    df_detail_store_sales['week_end_dt'] = pd.to_datetime(df_detail_store_sales['week_end_dt'])
    df_detail_store_sales.to_sql(name='detail_store_sales', con=engine, if_exists = 'append', index=False, chunksize=1000)
    logging.info(i)
