# -*- coding: utf-8 -*-

import json

import datetime
from datetime import datetime, timedelta, timezone
from os.path import dirname, join

from bokeh.layouts import gridplot
from bokeh.models import Spacer, Panel, WidgetBox
from bokeh.models.widgets import Div, Select, TextInput, Button, DatePicker
from dateutil.relativedelta import relativedelta
from fbprophet import Prophet
from holoviews import streams
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from scripts.utils.mylogger import mylogger

import pandas as pd
import hvplot.pandas
import twitter
import tweepy as tw

import holoviews as hv
from tornado.gen import coroutine

import unittest

from config.dashboard import config as dashboard_config
from scripts.utils.myutils import tab_error_flag, sentiment_analyzer_scores

hv.extension('bokeh', logo=False)
renderer = hv.renderer('bokeh')

logger = mylogger(__file__)


@coroutine
def twitter_loader_tab(panel_title):
    class TwitterLoader():
        def __init__(self, search_term='beiber'):
            # TWITTER SETUP
            self.api = None
            self.topic = search_term

            self.options = {
                'messages': [str(x) for x in range(10,1000,50)]
            }
            self.limits = {
                'messages': int(self.options['messages'][0]),
            }
            self.hidden_path = dashboard_config['hidden_path']
            self.DATEFORMAT = "%Y-%d-%m %H:%M:%S"
            self.df = None
            min_date = datetime.today() - timedelta(days=7)
            print(min_date)
            self.selects = {
                'window': Select(title='Select rolling mean window',
                                 value='1',
                                 options=[str(x) for x in range(1, 20, 2)]),
                'date_since': DatePicker(title="Tweets since:", min_date=min_date,
                                         max_date=datetime.today(), value=min_date)
            }
            self.selects_values = {
                'window': int(self.selects['window'].value),
                'date_since': self.selects['date_since'].value
            }
            self.resample_period = {
                'menu': []
            }
            for val in range(30, 350, 30):
                self.resample_period['menu'].append(str(val) + 'Min')
            self.resample_period['value'] = self.resample_period['menu'][0]
            # DIV VISUAL SETUP
            self.trigger = -1
            self.html_header = 'h2'
            self.margin_top = 150
            self.margin_bottom = -150

            self.div_style = """ 
                           style='width:350px; margin-left:25px;
                           border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
                       """

            self.header_style = """ style='color:blue;text-align:center;' """

            self.page_width = 1250
            txt = """<hr/>
                               <div style="text-align:center;width:{}px;height:{}px;
                                      position:relative;background:black;margin-bottom:200px">
                                      <h1 style="color:#fff;margin-bottom:300px">{}</h1>
                               </div>""".format(self.page_width, 50, 'Welcome')
            self.notification_div = {
                'top': Div(text=txt, width=self.page_width, height=20),
                'bottom': Div(text=txt, width=self.page_width, height=10),
            }

            self.section_divider = '-----------------------------------'
            self.section_headers = {

                'twitter': self.section_header_div(text='Twitter search results:',
                                                   width=600, html_header='h2', margin_top=155,
                                                   margin_bottom=-155),
            }

            # ----- UPDATED DIVS END

        # ----------------------  DIVS ----------------------------

        def section_header_div(self, text, html_header='h2', width=600, margin_top=150, margin_bottom=-150):
            text = """<div style="margin-top:{}px;margin-bottom:-{}px;"><{} style="color:#4221cc;">{}</{}></div>""" \
                .format(margin_top, margin_bottom, html_header, text, html_header)
            return Div(text=text, width=width, height=15)

        def notification_updater(self, text):
            txt = """<hr/><div style="text-align:center;width:{}px;height:{}px;
                                         position:relative;background:black;">
                                         <h1 style="color:#fff;margin-bottom:300px">{}</h1>
                                   </div>""".format(self.page_width, 50, text)
            for key in self.notification_div.keys():
                self.notification_div[key].text = txt

        def title_div(self, text, width=700):
            text = '<h2 style="color:#4221cc;">{}</h2>'.format(text)
            return Div(text=text, width=width, height=15)

        # //////////////////////////  DIVS SETUP END   /////////////////////////////////

        # /////////////////////////// UTILS BEGIN ///////////////////////////

        def twitter_datetime_to_epoch(self, ts):
            ts = datetime.strptime(ts, '%a %b %d %H:%M:%S %z %Y')
            ts_epoch = ts.created_at()
            ts = datetime.strftime(ts, self.DATEFORMAT)
            ts = datetime.strptime(ts, self.DATEFORMAT)
            return ts, ts_epoch

        def write_to_file(self):
            try:
                filename = """{}_searches_for_last_{}sec_or_last_{}messages.csv""".format(self.topic,
                                                                                          self.limits['time'],
                                                                                          self.limits['messages'])
                self.df.to_csv(filename, sep='\t', index=False)
            except:
                logger.error('Error writing to file', exc_info=True)

        # /////////////////////////// UTILS END /////////////////////
        def reset_data(self):
            self.df = None

        def get_credentials(self, filename='twitter_credentials.json'):
            try:
                filename = self.hidden_path + filename
                filepath = join(dirname(__file__), filename)
                print(filepath)
                if self.api is None:
                    with open(filepath, 'r') as f:
                        credentials_dict = json.load(f)
                    auth = tw.OAuthHandler(credentials_dict['consumer_key'], credentials_dict['consumer_secret'])
                    auth.set_access_token(credentials_dict['access_token_key'],credentials_dict['access_token_secret'],)
                    self.api = tw.API(auth, wait_on_rate_limit=True)
                logger.info('CREDENTIALS LOADED')
                try:
                    self.api.verify_credentials()
                    print("Authentication OK")
                except:
                    print("Error during authentication")
            except:
                print('credentials not loaded')

        def load_data_about_topic(self):
            try:
                if self.api is None:
                    self.get_credentials()
                date_since = datetime.combine(self.selects_values['date_since'],datetime.min.time())
                logger.warning('LINE 186:%s,messages=%s',self.topic,self.limits['messages'])
                # initialize a list to hold all the tweepy Tweets
                alltweets = []

                # make initial request for most recent tweets (200 is the maximum allowed count)
                new_tweets = self.api.search(q=self.topic,count=self.limits['messages'])

                # save most recent tweets
                alltweets.extend(new_tweets)

                # save the id of the oldest tweet less one
                oldest = alltweets[-1].id - 1

                # keep grabbing tweets until there are no tweets left to grab
                stop = False
                while not stop:
                    print(f"getting tweets before {oldest}")

                    # all subsequent requests use the max_id param to prevent duplicates
                    new_tweets = self.api.search(q=self.topic, count=100,
                                                max_id=oldest, tweet_mode='extended')

                    # save most recent tweets
                    alltweets.extend(new_tweets)
                    if len(alltweets) > self.limits['messages'] or len(new_tweets) <= 0:
                        stop = True
                    # update the id of the oldest tweet less one
                    oldest = alltweets[-1].id - 1

                    print(f"...{len(alltweets)} tweets downloaded so far")

                # transform the tweepy tweets into a 2D array that will populate the csv
                results = []
                for tweet in alltweets:
                    try:
                        results.append([tweet.created_at, tweet.text])
                    except:
                        print("skipped this one")

                self.df = pd.DataFrame(data=results,columns=['created_at','text'])
                logger.warning('LINE 211 self.df:%s',self.df.head(20))
            except:
                logger.error('error in loading data', exc_info=True)


        def run(self):
            try:
                self.load_data_about_topic()
                # self.write_to_file()

            except Exception:
                logger.error('run', exc_info=True)

        # #################################### PLOTS ######################################
        def sentiment_analysis(self, launch=1):
            try:
                df = self.df[['text', 'created_at']]
                cols = ['pos', 'neg', 'neu']
                for col in cols:
                    if col not in df.columns:  # create only once
                        df[col] = 0

                df['pos'], df['neg'], df['neu'] = zip(*df['text'].map(sentiment_analyzer_scores))
                df = df.fillna(0)
                logger.warning('resample period:%s', self.resample_period['value'])
                df = df.set_index('created_at').resample(self.resample_period['value']) \
                    .agg({'pos': 'mean',
                          'neg': 'mean',
                          'neu': 'mean'})
                df = df.reset_index()
                df = df.fillna(0)
                logger.warning('LINE 307, df:%s', df.head(30))

                p = df.hvplot.line(x='created_at', y=cols, width=1200, height=600)
                return p
            except Exception:
                logger.error('run', exc_info=True)

        def visual(self, launch=1):
            try:
                p = self.df.hvplot.table(columns=['created_at', 'text'],
                                    width=1200, height=2000)
                return p
            except Exception:
                logger.error('output data', exc_info=True)

        def jitter(self, launch=1):
            try:
                df = self.df.copy()
                df['jitter'] = df['created_at'].diff(periods=-1)
                df['jitter'] = df['jitter'] * -1
                df = df.dropna()

                p = df.hvplot.line(x='created_at', y='jitter', width=1200, height=600)
                return p
            except Exception:
                logger.error('output data', exc_info=True)


    def update_tweet_search():
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.reset_data()
        thistab.limits['messages'] = int(inputs['messages_limit'].value)
        thistab.topic = inputs['search_term'].value
        thistab.run()
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        stream_launch_sentiment.event(launch_this=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_resample_period(attr, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.resample_period['value'] = new
        thistab.trigger += 1
        # stream_launch_rolling_mean.event(launch=thistab.trigger)
        stream_launch_sentiment.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    try:
        # SETUP
        thistab = TwitterLoader()
        thistab.run()

        # MANAGE STREAM
        stream_launch = streams.Stream.define('Launch', launch=-1)()
        stream_launch_rolling_mean = streams.Stream.define('Launch', launch=-1)()
        stream_launch_sentiment = streams.Stream.define('Launch', launch=-1)()

        # DYNAMIC GRAPHS/OUTPUT
        hv_visual = hv.DynamicMap(thistab.visual, streams=[stream_launch])
        visual = renderer.get_plot(hv_visual)

        hv_jitter = hv.DynamicMap(thistab.jitter, streams=[stream_launch])
        jitter = renderer.get_plot(hv_jitter)

        hv_sentiment_analysis = hv.DynamicMap(thistab.sentiment_analysis, streams=[stream_launch_sentiment])
        sentiment_analysis = renderer.get_plot(hv_sentiment_analysis)

        # CREATE WIDGETS
        inputs = {
            'search_term': TextInput(title='Enter search term. For list, use commas', value=thistab.topic),

            'messages_limit': Select(title='Select messages limit (5000 = unbounded)',
                                     value=str(thistab.limits['messages']),
                                     options=thistab.options['messages']),

            'resample': Select(title='Select resample period',
                               value=thistab.resample_period['value'],
                               options=thistab.resample_period['menu'])

        }
        tweet_search_button = Button(label='Enter filters/inputs, then press me', button_type="success")

        # WIDGET CALLBACK
        tweet_search_button.on_click(update_tweet_search)
        inputs['resample'].on_change('value', update_resample_period)

        # COMPOSE LAYOUT
        # group controls (filters/input elements)
        controls_tweet_search = WidgetBox(
            inputs['search_term'],
            inputs['messages_limit'],
            tweet_search_button,
        )

        controls_rolling_mean = WidgetBox(
            thistab.selects['window'],
        )

        controls_resample_period = WidgetBox(
            inputs['resample']
        )

        grid = gridplot([
            [thistab.notification_div['top']],
            [Spacer(width=20, height=70)],
            [thistab.title_div('Sentiment analysis of tweets:', 1000)],
            [Spacer(width=20, height=30)],
            [sentiment_analysis.state, controls_resample_period],
            [thistab.title_div('Time between tweets:', 1000)],
            [Spacer(width=20, height=30)],
            [jitter.state],
            [thistab.title_div('Twitter search results (use filters on right, then click button):', 1000)],
            [Spacer(width=20, height=30)],
            [visual.state, controls_tweet_search],

            [thistab.notification_div['bottom']],
        ])

        # Make a tab with the layout
        tab = Panel(child=grid, title=panel_title)
        return tab

    except Exception:
        logger.error('Twitter loader:', exc_info=True)
        return tab_error_flag(panel_title)




