BROKER_URL = 'amqp://guest@localhost//'
CELERY_RESULT_BACKEND = 'amqp'
CELERY_ACCEPT_CONTENT = ['application/json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = 'US/Eastern'
CELERYD_MAX_TASKS_PER_CHILD = 10

CELERY_ROUTES = {
    'Saatva_Keywords_Download_Tasks.Saatva_Keywords_From_AdWords': {'queue': 'Saatva_Keywords_every_10_mins'},
}

from celery.schedules import crontab

CELERYBEAT_SCHEDULE = {

    'Every_10_Minutes': {
        'task': 'Saatva_Keywords_Download_Tasks.Saatva_Keywords_From_AdWords',
        'schedule': 600.0,
        'args': (),
    }
}

