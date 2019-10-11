# -*- coding: utf-8 -*-

import random
from dateutil.relativedelta import relativedelta
from holoviews import streams

from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import tab_error_flag
from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock
from scripts.utils.interfaces.KPI_interface import KPI
from config.dashboard import config as dashboard_config
from static.css.KPI_interface import KPI_card_css
from bokeh.layouts import gridplot, WidgetBox
from bokeh.models import Panel, Spacer
import gc
from bokeh.models.widgets import Div, \
    DatePicker, Select, Button

from datetime import datetime, date, timedelta

import holoviews as hv
from tornado.gen import coroutine
import numpy as np
import pandas as pd

lock = Lock()
executor = ThreadPoolExecutor()
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
renderer = hv.renderer('bokeh')


@coroutine
def kpi_inventory_tab(panel_title,credentials):
    class Thistab(KPI):
        def __init__(self, table,credentials,cols=[]):
            KPI.__init__(self, table, name='inventory', cols=cols,credentials=credentials)
            self.table = table
            self.df = None

            # setup selects
            self.menus = {
                'gender':['all','Male','Female']
            }
            self.select = {}
            self.select_values = {}
            for key in self.menus.keys():
                title = 'Select {}'.format(key)
                self.select[key] = Select(title=title, value='all',
                                          options=self.menus[key])
                self.select_values[key] = 'all'
            self.timestamp_col = 'timestamp_delivered'
            self.variable = 'delivery_amount'
            self.groupby_dict = {
                'delivery_amount': 'sum'
            }
            self.multiline_resample_period = 'D'
            self.pop = {
                'history_periods' : 3,
                'aggregate' : 'mean',
                'start':datetime(2015, 1, 5, 0, 0, 0),
                'end':self.pop_start_date + timedelta(days=8)
            }
            self.cols = cols

            self.load_data_start_date = datetime(2014, 1, 1, 0, 0, 0)
            self.load_data_end_date = datetime.now()

            self.ptd_startdate = datetime(datetime.today().year, 1, 1, 0, 0, 0)

            # cards

            self.KPI_card_div = self.initialize_cards(self.page_width, height=350)

            # ------- DIVS setup begin
            self.page_width = 1200
            txt = """<hr/><div style="text-align:center;width:{}px;height:{}px;
                          position:relative;background:black;margin-bottom:200px">
                          <h1 style="color:#fff;margin-bottom:300px">{}</h1>
                    </div>""".format(self.page_width, 50, 'Welcome')
            self.notification_div = {
                'top': Div(text=txt, width=self.page_width, height=20),
                'bottom': Div(text=txt, width=self.page_width, height=10),
            }

            self.section_divider = '-----------------------------------'
            self.section_headers = {
                'cards': self.section_header_div(
                    text='Period to date({})):{}'.format(self.variable, self.section_divider),
                    width=600, html_header='h2', margin_top=5,
                    margin_bottom=-155),
                'pop': self.section_header_div(text='Period over period:{}'.format(self.section_divider),
                                               width=600, html_header='h2', margin_top=5, margin_bottom=-155),
                'dow': self.section_header_div(text='Compare days of the week:'.format(self.section_divider),
                                               width=600, html_header='h2', margin_top=5, margin_bottom=-155),
            }

            # ----------------------  DIVS ----------------------------

        def section_header_div(self, text, html_header='h2', width=600, margin_top=150, margin_bottom=-150):
            text = """<div style="margin-top:{}px;margin-bottom:-{}px;"><{} style="color:#4221cc;">{}</{}></div>""" \
                .format(margin_top, margin_bottom, html_header, text, html_header)
            return Div(text=text, width=width, height=15)

        def section_header_div_updater(self, which_header, update_text):
            text = """<div style="margin-top:150 px;margin-bottom:--150px;">
                      <h2 style="color:#4221cc;">{}</h2></div>""" \
                .format(update_text)
            self.section_headers[which_header].text = text

        # ----------------------  DIVS ----------------------------

        def reset_checkboxes(self, value='all', checkboxgroup=''):
            try:
                self.checkboxgroup[checkboxgroup].value = value
            except Exception:
                logger.error('reset checkboxes', exc_info=True)

        def information_div(self, width=400, height=300):
            div_style = """ 
                          style='width:350px;margin-right:-800px;
                          border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
                      """
            txt = """
            <div {}>
            <h4 {}>How to interpret relationships </h4>
            <ul style='margin-top:-10px;'>
                <li>
                </li>
                <li>
                </li>
                <li>
                </li>
                <li>
                </li>
                 <li>
                </li>
                 <li>
                </li>
            </ul>
            </div>

            """.format(div_style, self.header_style)
            div = Div(text=txt, width=width, height=height)
            return div

        # ------------------- LOAD AND SETUP DATA -----------------------
        def filter_df(self, df):
            try:
                for item in self.select_values.keys():
                    if self.select_values[item] != 'all':
                        df = df[df[item] == self.select_values[item]]
                return df

            except Exception:
                logger.error('filters', exc_info=True)

        def set_select_menus(self, df):
            try:
                for item in self.select.keys():
                    if item in df.columns and len(df) > 0:
                        lst = list(set(df[item].values))
                        lst.append('all')
                        sorted(lst)

                        self.select[item].options = lst

            except Exception:
                logger.error('set filters menus', exc_info=True)


        # -------------------- CARDS -----------------------------------------
        def initialize_cards(self, width, height=250):
            try:
                txt = ''
                for period in ['year', 'quarter', 'month', 'week']:
                    design = random.choice(list(KPI_card_css.keys()))
                    txt += self.card(title='', data='', card_design=design)

                text = """<div style="margin-top:100px;display:flex; flex-direction:row;">
                       {}
                       </div>""".format(txt)
                div = Div(text=text, width=width, height=height)
                return div
            except Exception:
                logger.error('initialize cards', exc_info=True)

        # -------------------- GRAPHS -------------------------------------------



        def graph_periods_to_date(self, df1, timestamp_filter_col, variable):
            try:
                dct = {}
                for idx, period in enumerate(['week', 'month', 'quarter', 'year']):
                    if df1 is not None:
                        df = self.period_to_date(df1, timestamp=dashboard_config['dates']['last_date'],
                                                 timestamp_filter_col=timestamp_filter_col, period=period)

                        # get unique instances
                        # df = df[[variable]]
                        df = df.drop_duplicates(keep='first')
                        # logger.warning('post duplicates dropped:%s', df.head(10))
                        data = 0
                        if self.groupby_dict[variable] == 'sum':
                            data = int(df[variable].sum())
                        elif self.groupby_dict[variable] == 'mean':
                            data = "{}%".format(round(df[variable].mean(), 3))
                        else:
                            data = int(df[variable].count())
                        del df
                        gc.collect()
                        dct[period] = data
                    else:
                        dct[period] = 0

                self.update_cards(dct)


            except Exception:
                logger.error('graph periods to date', exc_info=True)

        def period_over_period(self, df, start_date, end_date, period,
                               history_periods,timestamp_col):
            def label_qtr_pop(y):
                try:
                    curr_quarter = int((y.month - 1) / 3 + 1)
                    start = datetime(y.year, 3 * curr_quarter - 2, 1)
                    if isinstance(y,date):
                        start = start.date()
                    return abs((start - y).days)
                except Exception:
                    logger.error('df label quarter', exc_info=True)
            try:
                # make columns for each history  period
                if len(df) == 0:
                    dfi = pd.date_range(self.pop['start'],self.pop['end'],freq='D')
                    dfi.rename(self.timestamp_col)
                    df = pd.DataFrame(random,index=dfi)
                    df[self.variable] = 0
                    print('LINE 239:%s',df.head())
                df = df.rename(columns={self.variable:'0_periods_prev',self.timestamp_col:'date'})
                df.set_index('date',inplace=True)

                for count in range(1,history_periods+1):
                    label = f"{count}_periods_prev"
                    print('LINE 252,label',label)
                    print('LINE 253',df.head())
                    try:
                        if period == 'month':
                            df[label] = df.shift(periods=30)
                        elif period == 'year':
                            df[label] = df.shift(periods=365)
                        elif period == 'week':
                            df[label] = df.shift(periods=7)
                        elif period == 'quarter':
                            df[label] = df.shift(periods=90)
                        df = df.fillna(0)
                    except Exception:
                        df[label] = 0
                    print('LINE 265, COUNT:',count)

                return df
            except Exception:
                logger.error('graph period over period', exc_info=True)


        def graph_period_over_period(self, period):
            try:
                periods = [period]
                start_date = self.pop['start']
                end_date = self.pop['end']
                if isinstance(start_date, date):
                    start_date = datetime.combine(start_date, datetime.min.time())
                if isinstance(end_date, date):
                    end_date = datetime.combine(end_date, datetime.min.time())
                cols = [self.variable, self.timestamp_col]
                df = self.load_df(start_date, end_date, cols=cols,
                                      timestamp_col=self.timestamp_col)

                for idx, period in enumerate(periods):
                    df_period = self.period_over_period(df, start_date=start_date, end_date=end_date,
                                                        period=period, history_periods=self.pop['history_periods'],
                                                        timestamp_col=self.timestamp_col)

                    title = "{} over {}".format(period, period)
                    plotcols = list(df.columns)
                    plotcols = plotcols.remove(self.variable)


                    if idx == 0:
                        p = df_period.hvplot.bar('date', plotcols, rot=45, title=title,
                                                 stacked=False, width=1200, height=500)
                    else:
                        p += df_period.hvplot.bar('date', plotcols, rot=45, title=title,
                                                  stacked=False, width=1200, height=500)
                return p

            except Exception:
                logger.error('period over period to date', exc_info=True)

        def pop_week(self, launch=-1):
            try:
                return self.graph_period_over_period('week')
            except Exception:
                logger.error('pop week', exc_info=True)

        def pop_month(self, launch=-1):
            try:
                return self.graph_period_over_period('month')
            except Exception:
                logger.error('pop month', exc_info=True)

        def pop_quarter(self, launch=-1):
            try:
                return self.graph_period_over_period('quarter')
            except Exception:
                logger.error('pop quarter', exc_info=True)

        def pop_year(self, launch=-1):
            try:
                return self.graph_period_over_period('year')
            except Exception:
                logger.error('pop year', exc_info=True)

        def multiline_dow(self, launch=1):
            try:
                df = self.df.copy()
                dct = {
                    'Y': 'year',
                    'M': 'month',
                    'W': 'week',
                    'Q': 'Qtr'
                }
                resample_period = dct[self.multiline_resample_period]
                yvar = self.multiline_vars['y']
                xvar = 'day_of_week'
                df[resample_period] = df[self.timestamp_col].dt.to_period(self.multiline_resample_period)
                df[resample_period] = df[resample_period].astype('str')
                df[xvar] = df[self.timestamp_col].dt.day_name()
                df = df.groupby([xvar, resample_period]).agg({yvar: 'mean'})
                df = df.reset_index()
                # logger.warning('LINE 402 df:%s',df.head(20))
                p = df.hvplot.line(resample_period, yvar, by='day_of_week', width=1200, height=500)
                p.opts(xrotation=45)
                return p
            except Exception:
                logger.error('multiline plot', exc_info=True)

    def update(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        for item in thistab.select_values.keys():
            thistab.select_values[item] = thistab.select[item].value
        thistab.graph_periods_to_date(thistab.df, thistab.timestamp_col, thistab.variable)
        thistab.section_header_updater('cards')
        thistab.section_header_updater('pop')
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("ready")

    def update_variable(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.variable = variable_select.value
        thistab.graph_periods_to_date(thistab.df, thistab.timestamp_col, thistab.variable)
        thistab.section_header_div_updater('cards', thistab.variable)
        # thistab.section_header_updater('cards',label='')
        # thistab.section_header_updater('pop',label='')
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("ready")

    def update_period_over_period():
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.pop['history_periods'] = history_periods_select.value
        thistab.pop_start_date = datepicker_pop_start.value  # trigger period over period
        thistab.pop_end_date = datepicker_pop_end.value  # trigger period
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("ready")

    def update_history_periods(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.pop['history_periods'] = pop_number_select.value
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("ready")

    try:
        table = 'inventory_warehouse'
        cols = ['delivery_amount','timestamp_delivered','gender']
        thistab = Thistab(table, credentials=credentials,cols=cols)
        # -------------------------------------  SETUP   ----------------------------
        # format dates
        first_date_range = thistab.initial_date
        last_date_range = datetime.now().date()

        last_date = dashboard_config['dates']['last_date']
        first_date = datetime(2014, 1, 1, 0, 0, 0)

        cols = [thistab.variable, thistab.timestamp_col]
        thistab.df = thistab.load_df(first_date, last_date, cols, thistab.timestamp_col)
        thistab.set_select_menus(thistab.df)
        thistab.graph_periods_to_date(thistab.df, timestamp_filter_col=thistab.timestamp_col,
                                      variable=thistab.variable)

        # MANAGE STREAM
        # date comes out stream in milliseconds
        # --------------------------------CREATE WIDGETS ---------------------------------

        daynum = datetime.now().day
        if daynum > 3:
            thistab.pop_end_date = datetime.now().date() - timedelta(days=daynum)
            thistab.pop_start_date = thistab.pop_end_date - timedelta(days=7)
        else:
            thistab.pop_start_date = thistab.first_date_in_period(thistab.pop_end_date, 'week')

        logger.warning('LINE 500: POP Start: END %s:%s', thistab.pop_start_date, thistab.pop_end_date)

        stream_launch = streams.Stream.define('Launch', launch=-1)()
        stream_launch_multiline = streams.Stream.define('Launch', launch=-1)()

        history_periods_select = Select(title='Select # of comparative periods',
                                        value=str(thistab.pop['history_periods']),
                                        options=['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'])

        datepicker_pop_start = DatePicker(title="Period start", min_date=first_date_range,
                                          max_date=last_date_range, value=thistab.load_data_start_date)

        datepicker_pop_end = DatePicker(title="Period end", min_date=first_date_range,
                                        max_date=last_date_range, value=thistab.load_data_end_date)

        pop_number_select = Select(title='Select # of comparative periods',
                                   value=str(5),
                                   options=['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'])
        pop_button = Button(label="Select dates/periods, then click me!", width=15, button_type="success")

        variable_select = Select(title='Select variable', value=thistab.variable,
                                 options=[thistab.variable] + list(thistab.select_values.keys()))

        # ---------------------------------  GRAPHS ---------------------------
        hv_pop_week = hv.DynamicMap(thistab.pop_week, streams=[stream_launch])
        pop_week = renderer.get_plot(hv_pop_week)

        hv_pop_month = hv.DynamicMap(thistab.pop_month, streams=[stream_launch])
        pop_month = renderer.get_plot(hv_pop_month)

        hv_pop_quarter = hv.DynamicMap(thistab.pop_quarter, streams=[stream_launch])
        pop_quarter = renderer.get_plot(hv_pop_quarter)

        # -------------------------------- CALLBACKS ------------------------
        # datepicker_start.on_change('value', update)
        # datepicker_end.on_change('value', update)
        for key in thistab.select_values.keys():
            thistab.select[key].on_change('value', update)

        variable_select.on_change('value', update_variable)
        pop_button.on_click(update_period_over_period)

        # -----------------------------------LAYOUT ----------------------------
        # put the controls in a single element
        controls = WidgetBox(
            variable_select,
            thistab.select['gender']
        )

        controls_pop = WidgetBox(datepicker_pop_start,
                                 datepicker_pop_end,
                                 history_periods_select,
                                 pop_button)

        # create the dashboards
        grid = gridplot([
            [thistab.notification_div['top']],
            [Spacer(width=20, height=70)],
            [thistab.section_headers['cards']],
            [Spacer(width=20, height=2)],
            [thistab.KPI_card_div, controls],
            [thistab.section_headers['pop']],
            [Spacer(width=20, height=25)],
            [pop_week.state, controls_pop],
            [pop_month.state],
            [pop_quarter.state],
            [thistab.notification_div['bottom']]
        ])

        # Make a tab with the layout
        tab = Panel(child=grid, title=panel_title)
        return tab

    except Exception:
        logger.error('rendering err:', exc_info=True)
        return tab_error_flag(panel_title)


