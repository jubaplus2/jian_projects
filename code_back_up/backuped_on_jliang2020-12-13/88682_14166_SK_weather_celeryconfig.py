
# coding: utf-8

# In[ ]:

# CELERY
BROKER_URL = 'amqp://guest@localhost//'
CELERY_RESULT_BACKEND = 'amqp'
CELERY_ACCEPT_CONTENT = ['application/json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = 'US/Eastern'

CELERY_ROUTES = {
    'SK_Weather_Task.fetch_weather_for_SK_zips': {'queue': 'SK_Weather_celery_JL'},
}

from celery.schedules import crontab
 
CELERYBEAT_SCHEDULE = {
    'every-hour': {
        'task': 'SK_Weather_Task.fetch_weather_for_SK_zips',
        'schedule': crontab(minute=30, hour='15'),
        'args': (),
    },
}

