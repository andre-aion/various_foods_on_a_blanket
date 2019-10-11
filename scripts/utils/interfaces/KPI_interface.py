import gc
import random
from math import inf, floor
from os.path import join, dirname

import pandas as pd
import numpy as np
from dask import dataframe as dd
from datetime import datetime,timedelta,date
from dateutil.relativedelta import relativedelta

from scripts.databases.pythonClickhouse import PythonClickhouse
from scripts.databases.pythonRedis import PythonRedis
from scripts.utils.mylogger import mylogger
from static.css.KPI_interface import KPI_card_css

import hvplot.pandas
import hvplot.dask

logger = mylogger(__file__)


# get rid of tzinfo
def clean_dates_from_db(x):
    try:
        return datetime(x.year, x.month, x.day, x.hour, 0, 0)
    except Exception:
        logger.error('clean dates from db', exc_info=True)



class KPI:
    menus = {
        'history_periods': ['1','2','3','4','5','6','7','8','9','10'],
        'resample_period':['W','M','Q'],
        'social_media':['twitter','facebook'],
        'social_media_variables':[
            'tw_mentions','tw_positive','tw_compound','tw_neutral',
            'tw_negative', 'tw_emojis_positive','tw_emojis_compound',
            'tw_emojis_negative','tw_emojis_count',
            'tw_replies_from_followers','tw_replies_from_following',
            'tw_reply_hashtags'],

    }
    def __init__(self,table,name,cols,credentials):
        self.df = None
        self.ch = PythonClickhouse(credentials=credentials['clickhouse'])
        self.table = table
        self.cols = cols
        self.div_style = """ style='width:350px; margin-left:25px;
                                border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
                                """

        self.header_style = """ style='color:blue;text-align:center;' """
        self.welcome_txt = """<div style="text-align:center;background:black;width:100%;">
                                         <h1 style="color:#fff;">
                                         {}</h1></div>""".format('Welcome')
        css_path = join(dirname(__file__),"../../../static/css/KPI_interface.css")
        self.KPI_card_css = KPI_card_css
        self.DATEFORMAT = '%Y-%m-%d %H:%M:%S'
        self.DATEFORMAT_PTD = '%Y-%m-%d'

        self.initial_date = datetime.strptime("2018-04-25 00:00:00",self.DATEFORMAT)
        self.account_type = 'all'
        self.trigger = -1
        self.periods_to_plot = {
            1 : ['week', 'month'],
            2: ['quarter']
        }
        self.pop_history_periods = 3 # number of periods for period over period
        self.pop_start_date = datetime(2015,1,5,0,0,0)
        self.pop_end_date = self.pop_start_date + timedelta(days=8)
        self.timestamp_col = ''
        self.checkboxgroup = {}
        self.sig_effect_dict = {}
        self.name = name
        self.redis_stat_sig_key = 'adoption_features:' + self.name
        self.card_grid_row = {
            'year': 0,
            'quarter': 1,
            'month': 2,
            'week': 3

        }
        weekly_pay = 1200
        num_engineers = 40
        self.payroll = {
            'week': weekly_pay*num_engineers,
            'month':weekly_pay*num_engineers*4,
            'quarter':weekly_pay*num_engineers*4*3,
            'year':weekly_pay*num_engineers*4*3*4
        }
        self.resample_period = self.menus['resample_period'][0]
        
        self.groupby_dict = {
            'tw_mentions': 'sum',
            'tw_positive': 'mean',
            'tw_compound': 'mean',
            'tw_neutral': 'mean',
            'tw_negative': 'mean',
            'tw_emojis_positive': 'mean',
            'tw_emojis_compound': 'mean',
            'tw_emojis_negative': 'mean',
            'tw_emojis_count': 'sum',
            'tw_replies_from_followers': 'sum',
            'tw_replies_from_following': 'sum',
            'tw_reply_hashtags': 'sum'
        }

        self.pop_history_periods = 3  # number of periods for period over period
        self.variable = 'item'
        self.grouby_var = ''
        self.page_width = 1200

        # make block timestamp the index
    def load_df(self,start_date,end_date,cols,timestamp_col,supplemental_where=None):
        try:

            if isinstance(end_date,date):
                end_date = datetime.combine(end_date,datetime.min.time())
            if isinstance(start_date,date):
                start_date = datetime.combine(start_date,datetime.min.time())
            end_date += timedelta(days=1)
            temp_cols = cols.copy()

            df = self.ch.load_data(self.table, temp_cols, start_date, end_date,timestamp_col,supplemental_where)
            # filter out the double entry
            if len(cols) > 0:
                return df[cols]
            else:
                return df
            #df[timestamp_col] = df[timestamp_col].map(lambda x: clean_dates_from_db(x))
        except Exception:
            logger.error('load df',exc_info=True)

    def load_df_pym(self, req_startdate, req_enddate, table, cols, timestamp_col):
        try:
            # get min and max of loaded df
            if self.df is not None:
                loaded_min = self.df[timestamp_col].min()
                loaded_max = self.df[timestamp_col].max()

                if loaded_min <= req_startdate and loaded_max >= req_enddate:
                    df = self.df[(self.df[timestamp_col] >= req_startdate) &
                                 (self.df[timestamp_col] <= req_enddate)]
                    return df
            return self.pym.load_df(req_startdate, req_enddate, table=table,
                                    cols=cols, timestamp_col=timestamp_col)

        except Exception:
            logger.error('load_df', exc_info=True)

    def update_cards(self, dct):
        try:
            txt = ''
            for period, data in dct.items():
                design = random.choice(list(KPI_card_css.keys()))
                title = period + ' to date'
                txt += self.card(title=title, data=data, card_design=design)

            text = """<div style="margin-top:100px;display:flex; flex-direction:row;">
                                                {}
                                                </div>""".format(txt)

            self.KPI_card_div.text = text

        except Exception:
            logger.error('update cards', exc_info=True)

    def reset_checkboxes(self, value='all', checkboxgroup=''):
        try:
            self.checkboxgroup[checkboxgroup].value = value
        except Exception:
            logger.error('reset checkboxes', exc_info=True)

    def first_date_in_quarter(self,timestamp):
        try:
            curr_quarter = int((timestamp.month - 1) / 3 + 1)
            return datetime(timestamp.year, 3 * curr_quarter - 2, 1)

        except Exception:
            logger.error('period to date', exc_info=True)
    
    def first_date_in_period(self,timestamp,period):
        try:
            if period == 'week':
                start = timestamp - timedelta(days=timestamp.weekday())
            elif period == 'month':
                start = datetime(timestamp.year,timestamp.month,1,0,0,0)
            elif period == 'year':
                start = datetime(timestamp.year,1,1,0,0,0)
            elif period == 'quarter':
                start = self.first_date_in_quarter(timestamp)
            return start
        except Exception:
            logger.error('period to date', exc_info=True)

    def period_to_date(self, df, timestamp = None, timestamp_filter_col=None, cols=[], period='week'):
        try:
            if timestamp is None:
                timestamp = datetime.now()
                timestamp = datetime(timestamp.year,timestamp.month,timestamp.day,timestamp.hour,0,0)
            logger.warning('LINE 203: timestdamp:%s',timestamp)
            if timestamp is not None:
                start = self.first_date_in_period(timestamp,period)
                # filter
                if timestamp_filter_col is None:
                    timestamp_filter_col = self.timestamp_col

                #logger.warning('df:%s',df[timestamp_filter_col])

                df = df[(df[timestamp_filter_col] >= start) & (df[timestamp_filter_col] <= timestamp)]
                if len(cols) >0:
                    df = df[cols]
            return df
        except Exception:
            logger.error('period to date',exc_info=True)

    def label_qtr_pop(y):
        try:
            curr_quarter = int((y.month - 1) / 3 + 1)
            start = datetime(y.year, 3 * curr_quarter - 2, 1)
            return abs((start - y).days)
        except Exception:
            logger.error('df label quarter', exc_info=True)

    def shift_period_range(self,period,start,end):
        try:
            if period == 'week':
                start = start - timedelta(days=7)
                end = end - timedelta(days=7)
            elif period == 'month':
                start = start - relativedelta(months=1)
                end = end - relativedelta(months=1)
            elif period == 'year':
                start = start - relativedelta(years=1)
                end = end - relativedelta(years=1)
            elif period == 'quarter':
                start = start - relativedelta(months=3)
                end = end - relativedelta(months=3)
            #logger.warning('%s start:end=%s:%s',period,start,end)
            return start, end
        except Exception:
            logger.error('shift period range',exc_info=True)


    # -----------------------  UPDATERS  ------------------------------------------
    def card(self, title, data, width=200, height=200, card_design='folders'):
        try:
            txt = """
            <div style="flex: 1 1 0px;border: 1px solid black;{};width:{}px;
                        height:{}px;border-right=10px;">
                <h3>
                    {}
                </h3>
                </br>
                {}
            </div>""".format(
                self.KPI_card_css[card_design], width, height, title, data)
            return txt
        except Exception:
            logger.error('card', exc_info=True)


    def notification_updater(self, text):
        txt = """<hr/><div style="text-align:center;width:{}px;height:{}px;
                              position:relative;background:black;">
                              <h1 style="color:#fff;margin-bottom:300px">{}</h1>
                        </div>""".format(self.page_width,50,text)
        for key in self.notification_div.keys():
            self.notification_div[key].text = txt

    """
        update the section labels on the page

    """

    # -------------------- CALCULATE KPI's DEVELOPED FROM VARIABLES WITH STATITICALLY SIGNIFICANT EFFECT
    def card_text(self, title, data, card_design='folders'):
        try:
            txt = """
            <div {}>
            <h3>{}</h3></br>{}
            </div>
            """.format(self.KPI_card_css[card_design], title, data)
            return txt
        except Exception:
            logger.error('card text',exc_info=True)


    def calc_sig_effect_card_data(self, df,interest_var,period):
        try:

            significant_features = []
            if len(significant_features) > 0:
                cols = [interest_var]+list(significant_features.keys())
                tmp_df = df[cols]
                numer = tmp_df[interest_var].sum()

                variable_of_interest_tmp = interest_var.split('_')
                if variable_of_interest_tmp[-1] in ['watch']:
                    variable_of_interest_tmp[-1] += 'e'
                txt = ''
                for var in significant_features.keys():
                    point_estimate = 0
                    var_tmp = var.split('_')# slice out the 'fork' from 'aion_fork'
                    if numer != 0:
                        denom = tmp_df[var].sum()
                        point_estimate = '*'
                        if denom != 0:
                            point_estimate = round(numer/denom,3)
                    # add metrics based on variables
                    # update the divs
                    self.sig_effect_dict[var] = {
                        'title' : "{}s per {}".format(variable_of_interest_tmp[-1], var_tmp[-1]),
                        'point_estimate':point_estimate
                    }

                    txt += self.card(
                        title=self.sig_effect_dict[var]['title'],
                        data=self.sig_effect_dict[var]['point_estimate'],
                        card_design=random.choice(list(self.KPI_card_css.keys())))

                    return txt

        except Exception:
            logger.error('make sig effect columns', exc_info=True)

    def update_significant_DV_cards(self, dct):
        try:
            txt = ''
            for idx, period in enumerate(dct.keys()):
                txt += dct[period]
            text = """<div style="margin-top:100px;display:flex; flex-direction:column;">
                                            {}
                       </div>""".format(txt)

            self.KPI_card_div.text = text

        except Exception:
            logger.error('update cards', exc_info=True)



    """
        groupby the the data and make ratios between 
        significant variables and interest variables
    """
    def make_significant_ratios_df(self,df,resample_period,interest_var,timestamp_col):
        try:
            def ratio(df,col_old,col_new):
                df = df.assign(result=df[interest_var]/df[col_old])
                df = df.rename(columns={'result':col_new})
                #logger.warning('col-%s df:%s',col_old,df.head(5))

                return df

            # filter
            sig_features_dict = self.match_sigvars_to_coin_vars(df,interest_var)
            sig_features_dict[interest_var] = 'sum' # include interest var in aggregations
            sig_features_list = list(sig_features_dict.keys())
            # rename column for overwriting
            sig_vars_relabel = []
            for feature in sig_features_list:
                tmp = feature.split('_')
                sig_vars_relabel.append(tmp[-1])
            # groupby
            df = df.set_index(timestamp_col)

            df = df.resample(resample_period).agg(sig_features_dict)
            #logger.warning('LINE 413:%s',len(df))

            # create ratios
            for idx,col in enumerate(sig_features_list):
                if col != interest_var: # skip variable of interest
                    df = df.map_partitions(ratio,col,sig_vars_relabel[idx])

            # drop columns
            df = df.drop(sig_features_list, axis=1)
            df = df.fillna(0)
            return df
        except Exception:
            logger.error('significant ratios',exc_info=True)