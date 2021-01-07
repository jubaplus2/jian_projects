BROKER_URL = 'amqp://guest@localhost//'
CELERY_RESULT_BACKEND = 'amqp'
CELERY_ACCEPT_CONTENT = ['application/json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = 'US/Eastern'
CELERYD_MAX_TASKS_PER_CHILD = 10

CELERY_ROUTES = {
    'tasks.biglots_weekly': {'queue': 'biglots_weekly'},
}

from celery.schedules import crontab

CELERYBEAT_SCHEDULE = {

    'Try_1st_Time': {
        'task': 'tasks.biglots_weekly',
        'schedule': crontab(minute=30, hour=15, day_of_week=2),
        'args': (),
    },
    'Try_2nd_Time': {
        'task': 'tasks.biglots_weekly',
        'schedule': crontab(minute=30, hour=23, day_of_week=2),
        'args': (),
    },
    'Try_3rd_Time': {
        'task': 'tasks.biglots_weekly',
        'schedule': crontab(minute=10, hour=21, day_of_week=3),
        'args': (),
    },
    'Try_4th_Time': {
        'task': 'tasks.biglots_weekly',
        'schedule': crontab(minute=13, hour=21, day_of_week=4),
        'args': (),
    }
}

