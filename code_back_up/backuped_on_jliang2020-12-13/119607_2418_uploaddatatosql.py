# -*- coding: utf-8 -*-
"""
Created on Mon Oct 24 16:41:06 2016

@author: Jubaplus
"""
import logging
import datetime
import os
import datetime
import gc
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

engine = create_engine('mysql://root:JubaPlus-2017@localhost/biglotsdata')
logging.basicConfig(filename='Log_viewershiplogsallfileloop.log', level=logging.INFO)
logging.info('viewershiplogs')
logging.info(datetime.datetime.now())



filepath = ['/home/jubauser1/BigLots/MediaStormDataExtract/MediaStorm Data Extract - Detail Store Sales3 (stores 1451 to 1750).txt',
'/home/jubauser1/BigLots/MediaStormDataExtract/MediaStorm Data Extract - Detail Store Sales1 (stores up to 1000).txt',
'/home/jubauser1/BigLots/MediaStormDataExtract/MediaStorm Data Extract - Detail Store Sales2 (stores 1001 to 1450).txt'] 

for i in filepath:
    df = pd.read_csv(i,sep = '|')
    df['week_end_dt'] = pd.to_datetime(df['week_end_dt'])
    varfile.to_sql(name=detailstoresales, con=engine, if_exists = 'append', index=False, chunksize=1000)
    logging.info(i)



