import json
from urllib import request
import time
import datetime

import logging
logging.basicConfig(filename='/home/jian/BL_weekly_crontab/cron_11_BL_weather/cron_11_weather_log.log',level=logging.INFO)


zip_list =json.load(open('/home/sharefolder/Weather/Json_data/weather_celery/zip_set.json', 'r'))

weather_by_zip = dict()
for zip_code in zip_list:
    url = 'http://api.openweathermap.org/data/2.5/weather?zip=%s,us&appid=931f5e95a5c3b97de1c2fccdff056857'%zip_code
    time.sleep(1)
    try:
        req = request.urlopen(url)
        # response = urllib2.urlopen(req)
        result = req.read()

        weather_by_zip[zip_code] = eval(result)
        
        #logging.info('%s suceeded.'%zip_code)
    except Exception as e:
        logging.info('%s %s'%(zip_code, e))

json.dump(weather_by_zip, open('/home/sharefolder/Weather/Json_data/daily/%s.json'%datetime.datetime.now().strftime('%Y-%m-%d: %H'), 'w'))

forcast_weather_by_zip = dict()
for zip_code in zip_list:
    url = 'http://api.openweathermap.org/data/2.5/forecast?zip=%s,us&appid=931f5e95a5c3b97de1c2fccdff056857'%zip_code
    time.sleep(1)
    try:
        req = request.urlopen(url)
        # response = urllib2.urlopen(req)
        result = req.read()

        forcast_weather_by_zip[zip_code] = eval(result)
        
        #logging.info('%s suceeded.'%zip_code)
    except Exception as e:
        logging.info('%s %s'%(zip_code, e))

json.dump(forcast_weather_by_zip, open('/home/sharefolder/Weather/Json_data/forecast/%s.json'%datetime.datetime.now().strftime('%Y-%m-%d: %H'), 'w'))
