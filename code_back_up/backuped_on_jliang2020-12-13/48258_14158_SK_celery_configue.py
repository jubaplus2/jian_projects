BROKER_URL = 'amqp://guest@localhost//'
CELERY_RESULT_BACKEND = 'amqp'
CELERY_ACCEPT_CONTENT = ['application/json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = 'US/Eastern'
CELERYD_MAX_TASKS_PER_CHILD = 5

CELERY_ROUTES = {
    'SK_weekly_task_20191002.Smoothie_King_weekly': {'queue': 'SmoothieKing_weekly'},
}

from celery.schedules import crontab

CELERYBEAT_SCHEDULE = {

    'Run_Monday': {
        'task': 'SK_weekly_task_20191002.Smoothie_King_weekly',
        'schedule': crontab(minute=55, hour=15, day_of_week=1),
        'args': ()
        }
}