# -*- coding: utf-8 -*-

import json

import datetime
from datetime import datetime, timedelta, timezone
from os.path import dirname, join

from bokeh.layouts import gridplot
from bokeh.models import Spacer, Panel,WidgetBox
from bokeh.models.widgets import Div, Select, TextInput, Button
from dateutil.relativedelta import relativedelta
from fbprophet import Prophet
from holoviews import streams
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


from scripts.utils.mylogger import mylogger


import pandas as pd
import hvplot.pandas
import twitter

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
        def __init__(self,search_term = 'beiber'):
            # TWITTER SETUP
            self.api = None
            self.topic = search_term
            self.options = {
                'messages': [str(x) for x in range(100, 10, -10)]+['5000'],
                'time': ['40000'] + [str(x) for x in range(30, 100000, 3000)],
            }
            self.limits = {
                'messages' : int(self.options['messages'][0]),
                'time' : int(self.options['time'][0]) #secs
            }
            self.hidden_path = dashboard_config['hidden_path']
            self.timestamp = {
                'start_loading' : datetime.now(timezone.utc).timestamp(),
                'stop_loading' :  datetime.now(timezone.utc).timestamp() - self.limits['time']
            }
            self.DATEFORMAT = "%Y-%d-%m %H:%M:%S"
            self.df = None
            self.messages_dict = {
                'message_ID': [],
                'human_readable_creation_date':[],
                'creation_date': [],
                'text': [],
                'user_ID': [],
                'user_creation_date': [],
                'user_name': [],
                'user_screen_name': []
            }

            self.selects = {
                'window' : Select(title='Select rolling mean window',
                                 value='1',
                                 options=[str(x) for x in range(1,20,2)]),
            }
            self.selects_values = {
                'window': int(self.selects['window'].value),
            }
            self.resample_period = {
                'menu' : []
            }
            for val in range(300,3000,200):
                self.resample_period['menu'].append(str(val)+'S')
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
            ts_epoch = ts.timestamp()
            ts = datetime.strftime(ts, self.DATEFORMAT)
            ts = datetime.strptime(ts,self.DATEFORMAT)
            return ts, ts_epoch

        def write_to_file(self):
            try:
                filename = """{}_searches_for_last_{}sec_or_last_{}messages.csv""".format(self.topic,self.limits['time'],
                                                                         self.limits['messages'])
                self.df.to_csv(filename,sep='\t', index=False)
            except:
                logger.error('Error writing to file', exc_info=True)

        # /////////////////////////// UTILS END /////////////////////
        def reset_data(self):
            self.messages_dict = {
                'message_ID': [],
                'human_readable_creation_date':[],
                'creation_date': [],
                'text': [],
                'user_ID': [],
                'user_creation_date': [],
                'user_name': [],
                'user_screen_name': []
            }
            self.df = None

        def get_credentials(self, filename='twitter_credentials.json'):
            try:
                filename = self.hidden_path +filename
                filepath = join(dirname(__file__),filename)
                print(filepath)
                if self.api is None:
                    with open(filepath, 'r') as f:
                        credentials_dict = json.load(f)
                    self.api = twitter.Api(
                        consumer_key=credentials_dict['consumer_key'],
                        consumer_secret=credentials_dict['consumer_secret'],
                        access_token_key=credentials_dict['access_token_key'],
                        access_token_secret=credentials_dict['access_token_secret'],
                    )
                logger.info('CREDENTIALS LOADED')
            except:
                print('credentials not loaded')



        def construct_query(self):
            try:
                qry = 'q='
                if ',' in self.topic:
                    topics = self.topic.split(',')
                    for topic, count in enumerate(topics):
                        if count > 0:
                            qry += '%20' + topic
                        else:
                            qry += topic
                else:
                    qry += self.topic

                qry += '&count={}'.format(self.limits['messages'])
                qry += '&result_type=recent'
                logger.warning('QUERY CONSTRUCTED:%s',qry)
                print(qry)
                return qry

            except:
                logger.error('error constructing query',exc_info=True)
                return "q=beiber&count=100&result_type=recent"


        def load_data_about_topic(self):
            try:
                if self.api is None:
                    self.get_credentials()
                qry = self.construct_query()
                results = self.api.GetSearch(raw_query=qry)
                self.timestamp['start_loading'] = datetime.now(timezone.utc).timestamp()
                self.timestamp['stop_loading'] = self.timestamp['start_loading'] - self.limits['time']
                logger.warning('# of results retreived:%s',len(results))
                return results
            except:
                logger.error('error in loading data', exc_info=True)

        # parse, truncate to requested records or seconds, make a dataframe from groupby
        def parse_results(self,results):
            try:

                messages_count = 0
                stop = False

                logger.warning('start:end= %s:  %s',self.timestamp['start_loading'],self.timestamp['stop_loading'])
                while not stop:
                    res = results[messages_count]
                    tweet_ts, ts_epoch = self.twitter_datetime_to_epoch(res.created_at)
                    self.messages_dict['message_ID'].append(res.id)
                    self.messages_dict['creation_date'].append(ts_epoch)
                    self.messages_dict['human_readable_creation_date'].append(tweet_ts)
                    self.messages_dict['text'].append(res.text)
                    user = res.user
                    ts, ts_epoch_user = self.twitter_datetime_to_epoch(user.created_at)
                    self.messages_dict['user_ID'].append(user.id)
                    self.messages_dict['user_creation_date'].append(ts_epoch_user)
                    self.messages_dict['user_name'].append(user.name)
                    self.messages_dict['user_screen_name'].append(user.screen_name)
                    messages_count += 1

                    # the 100000  represents unlimited messages in case we want to load more than 30 seconds worth
                    if messages_count >= len(results):
                        stop = True
                        if self.limits['messages'] != 5000:
                            if messages_count >= self.limits['messages']:
                                stop = True
                        # make a dataframe
                self.df = pd.DataFrame.from_dict(self.messages_dict)
                if self.df is not None:
                    logger.warning('df:, length=%s,%s',len(self.df),self.df.head())

            except:
                logger.error('error in parsing results', exc_info=True)

        def munge_data(self):
            try:
                if self.df is not None:
                    # groupby user, then sort by message time
                    self.df = self.df.sort_values(by=['creation_date','user_ID'])
                else:
                    self.df = pd.DataFrame.from_dict(self.messages_dict)
            except:
                logger.error('munge data', exc_info=True)

        def run(self):
            try:
                results = self.load_data_about_topic()
                self.parse_results(results)
                self.munge_data()
                #self.write_to_file()

            except Exception:
                logger.error('run', exc_info=True)

        # #################################### PLOTS ######################################
        def sentiment_analysis(self,launch = 1):
            try:
                df = self.df[['text','human_readable_creation_date']]
                cols = ['pos', 'neg', 'neu']
                for col in cols:
                    if col not in df.columns:  # create only once
                        df[col] = 0

                df['pos'], df['neg'], df['neu'] = zip(*df['text'].map(sentiment_analyzer_scores))
                df = df.fillna(0)
                logger.warning('resample period:%s',self.resample_period['value'])
                df = df.set_index('human_readable_creation_date').resample(self.resample_period['value'])\
                    .agg({'pos': 'mean',
                          'neg': 'mean',
                          'neu': 'mean'})
                df = df.reset_index()
                df = df.fillna(0)
                logger.warning('LINE 307, df:%s',df.head(30))

                p = df.hvplot.line(x='human_readable_creation_date', y=cols, width=1200, height=600)
                return p
            except Exception:
                logger.error('run', exc_info=True)
                
                
        def visual(self,launch=1):
            try:

                df = self.df[self.df.creation_date >= self.timestamp['stop_loading']]
                p = df.hvplot.table(columns=['message_ID','creation_date','human_readable_creation_date','text',
                                                  'user_ID','user_creation_date','user_name','user_screen_name'],
                                         width=1200,height=2000)
                return p
            except Exception:
                logger.error('output data', exc_info=True)

        def jitter(self, launch=1):
            try:
                df = self.df.set_index('human_readable_creation_date')
                df = df[['creation_date']]
                df['jitter'] = df['creation_date'].diff(periods=-1)
                df['jitter'] = df['jitter'] * -1
                df = df.dropna()

                df = df.reset_index()
                p = df.hvplot.line(x='creation_date',y='jitter',width=1200,height=600)
                return p
            except Exception:
                logger.error('output data', exc_info=True)


        def rolling_mean(self,launch=1):
            try:
                df = self.df.set_index('human_readable_creation_date')
                df = df.resample(self.resample_period['value']).agg({'message_ID':'count'})
                df = df['message_ID'].rolling(self.selects_values['window']).mean()
                df = df.reset_index()
                df = df.rename(columns={'message_ID':'messages',
                                        'human_readable_creation_date':'date'})
                p = df.hvplot.scatter(x='date', y='messages', width=1200, height=500)

                return p
            except Exception:
                logger.error('time series analysis', exc_info=True)

    def update_tweet_search():
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.reset_data()
        thistab.limits['messages'] = int(inputs['messages_limit'].value)
        thistab.limits['time'] = int(inputs['time_limit'].value)
        thistab.topic = inputs['search_term'].value
        thistab.run()
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        stream_launch_sentiment.event(launch_this=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_rolling_mean(attr,old,new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.selects_values['window'] = int(thistab.selects['window'].value)
        thistab.trigger += 1
        stream_launch_rolling_mean.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_resample_period(attr, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.resample_period['value'] = new
        thistab.trigger += 1
        #stream_launch_rolling_mean.event(launch=thistab.trigger)
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
        hv_visual = hv.DynamicMap(thistab.visual,streams=[stream_launch])
        visual = renderer.get_plot(hv_visual)

        hv_jitter = hv.DynamicMap(thistab.jitter, streams=[stream_launch])
        jitter = renderer.get_plot(hv_jitter)

        hv_rolling_mean = hv.DynamicMap(thistab.rolling_mean, streams=[stream_launch_rolling_mean])
        rolling_mean = renderer.get_plot(hv_rolling_mean)

        hv_sentiment_analysis = hv.DynamicMap(thistab.sentiment_analysis, streams=[stream_launch_sentiment])
        sentiment_analysis = renderer.get_plot(hv_sentiment_analysis)
        
        # CREATE WIDGETS
        inputs = {
            'search_term' : TextInput(title='Enter search term. For list, use commas',value=thistab.topic),

            'messages_limit' : Select(title='Select messages limit (5000 = unbounded)',
                                   value= str(thistab.limits['messages']),
                                   options=thistab.options['messages']),

            'time_limit' : Select(title='Select time limit (seconds)',
                                    value=str(thistab.limits['time']),
                                    options=thistab.options['time']),
            'resample' :  Select(title='Select resample period',
                                  value=thistab.resample_period['value'],
                                  options=thistab.resample_period['menu'])


        }
        tweet_search_button = Button(label='Enter filters/inputs, then press me', button_type="success")

        # WIDGET CALLBACK
        tweet_search_button.on_click(update_tweet_search)
        thistab.selects['window'].on_change('value',update_rolling_mean)
        inputs['resample'].on_change('value',update_resample_period)
        

        # COMPOSE LAYOUT
        # group controls (filters/input elements)
        controls_tweet_search = WidgetBox(
            inputs['search_term'],
            inputs['messages_limit'],
            inputs['time_limit'],
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
            [thistab.title_div('Smooth graphs:', 1000)],
            [Spacer(width=20, height=30)],
            [rolling_mean.state, controls_rolling_mean],
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




