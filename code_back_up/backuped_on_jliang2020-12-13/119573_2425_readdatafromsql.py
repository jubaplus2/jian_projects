# -*- coding: utf-8 -*-
"""
Created on Mon Oct 24 16:41:06 2016

@author: Jubaplus
"""
import pandas as pd
import numpy as np
import logging
import datetime
import function3036
from sqlalchemy import create_engine
import os

startdate = 20170612
enddate = 20170625
cableid = '006196'
cablename = 'LIF  '
engine = create_engine('mysql://root:JubaPlus-2017@localhost/biglotsdata')

start_date = datetime.datetime.strptime(str(startdate), '%Y%m%d').date()
end_date = datetime.datetime.strptime(str(enddate), '%Y%m%d').date()

type36 = pd.read_sql(("select * from minuteviewing_cwk where NetworkName=%(n0)s \
                       and Date>=%(sd0)s\
                       and Date<=%(ed0)s\
                       and Day ='1Mon'\
                       and playbacksegments != 'Live7D' "),
                       params={'n0':cablename,'sd0':start_date,'ed0':end_date},
                       con=engine)

