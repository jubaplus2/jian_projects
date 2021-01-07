import json
import urllib2
import time
import datetime

import logging
logging.basicConfig(filename='/home/jzou/biglots/weather/openweathermap.log',
                    level=logging.INFO,
                    format='%(asctime)s %(message)s')

zip_list = json.load(open('/home/jzou/biglots/weather/zip_set.json', 'r'))

count = 0
weather_by_zip = dict()
for zip_code in zip_list:
    if count >= 60:
        count = 0
        time.sleep(60)
    else:
        count += 1
    url = 'http://api.openweathermap.org/data/2.5/weather?zip=%s,us&appid=931f5e95a5c3b97de1c2fccdff056857'%zip_code
    try:
        req = urllib2.Request(url)
        response = urllib2.urlopen(req)
        result = response.read()

        weather_by_zip[zip_code] = eval(result)
        
        logging.info('%s suceeded.'%zip_code)
    except Exception as e:
        logging.info('%s %s'%(zip_code, e))

json.dump(weather_by_zip, 
          open('/home/jzou/biglots/weather/api_response/%s.json'%datetime.datetime.now().strftime('%Y-%m-%d'), 'w'))
