# CELERY
BROKER_URL = 'amqp://guest@localhost//'
CELERY_RESULT_BACKEND = 'amqp'
CELERY_ACCEPT_CONTENT = ['application/json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = 'US/Eastern'

CELERY_ROUTES = {
    'tasks.fetch_all_zips': {'queue': 'openweathermap'},
}

from celery.schedules import crontab
 
CELERYBEAT_SCHEDULE = {
    'every-hour': {
        'task': 'tasks.fetch_all_zips',
        'schedule': crontab(minute=1, hour='11,14,17'),
        'args': (),
    },
}

