
# coding: utf-8

# In[ ]:

from celery import Celery
celery = Celery('SK_Weather_Task')
celery.config_from_object('SK_weather_celeryconfig')

import json
from urllib import request
import time
import datetime

import logging
logging.basicConfig(filename='SK_Weather_openweathermap.log',level=logging.INFO,format='%(asctime)s %(message)s')

@celery.task
def fetch_weather_for_SK_zips():
    zip_list =json.load(open('/home/jian/Projects/Smoothie_King/Weather_Data/zips_for_SmoothieKing_stores.json', 'r'))

    weather_by_zip = dict()
    for zip_code in zip_list:
        url = 'http://api.openweathermap.org/data/2.5/weather?zip=%s,us&appid=f7f8e8da09d515073ef5c781fd2ceb48'%zip_code
        time.sleep(1)
        try:
            req = request.urlopen(url)
            # response = urllib2.urlopen(req)
            result = req.read()

            weather_by_zip[zip_code] = eval(result)
            
            #logging.info('%s suceeded.'%zip_code)
        except Exception as e:
            logging.info('%s %s'%(zip_code, e))

    json.dump(weather_by_zip, open('/home/jian/Projects/Smoothie_King/Weather_Data/Daily_Actual/%s.json'%datetime.datetime.now().strftime('%Y-%m-%d: %H'), 'w'))
    
    forcast_weather_by_zip = dict()
    for zip_code in zip_list:
        url = 'http://api.openweathermap.org/data/2.5/forecast?zip=%s,us&appid=f7f8e8da09d515073ef5c781fd2ceb48'%zip_code
        time.sleep(1)
        try:
            req = request.urlopen(url)
            # response = urllib2.urlopen(req)
            result = req.read()

            forcast_weather_by_zip[zip_code] = eval(result)
            
            #logging.info('%s suceeded.'%zip_code)
        except Exception as e:
            logging.info('%s %s'%(zip_code, e))

    json.dump(forcast_weather_by_zip, open('/home/jian/Projects/Smoothie_King/Weather_Data/Forecast/%s.json'%datetime.datetime.now().strftime('%Y-%m-%d: %H'), 'w'))

