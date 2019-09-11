from datetime import datetime, timedelta

from bokeh.models import HTMLTemplateFormatter

DATEFORMAT = '%Y-%m-%d %H:%M:%S'
config = {

    'dates': {
        'DATEFORMAT': '%Y-%m-%d %H:%M:%S',
        'last_date': datetime.today() - timedelta(days=1),
        'current_year_start':datetime(2018,1,1,0,0,0),
        'DAYS_TO_LOAD':30
    },
    'hidden_path': '../../../data/'

}
