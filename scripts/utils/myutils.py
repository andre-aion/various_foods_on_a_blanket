
# when a tab does not work
from bokeh.models import Div, Panel
# handle weird twitter times
from datetime import datetime, timezone
import pytz
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from scripts.utils.mylogger import mylogger
import math

logger = mylogger(__file__)
analyser = SentimentIntensityAnalyzer()


def tab_error_flag(tabname):
    # Make a tab with the layout
    text = """ERROR CREATING {} TAB, 
    CHECK THE LOGS""".format(tabname.upper())
    div = Div(text=text,
              width=200, height=100)

    tab = Panel(child=div, title=tabname)

    return tab


def sentiment_analyzer_scores(sentence):
    try:
        # logger.warning('text being analyzed:%s', sentence)
        score = analyser.polarity_scores(sentence)
        logger.warning('LINE 30: score:%s',score)
        for item in score.keys():
            if math.isnan(score[item]):
                score[item] = 0
        return round(score['pos'],2),round(score['neg'],2), round(score['neu'],2)
    except Exception:
        score = {
            'pos': 0,
            'neg': 0,
            'neu': 0,
            'compound': 0
        }
        logger.error('sentiment analyzer failed',exec_info=True)
        return 0,0,0