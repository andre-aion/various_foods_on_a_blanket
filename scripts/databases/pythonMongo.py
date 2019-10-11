import datetime

from pandas.io.json import json_normalize
from tornado.gen import coroutine

from scripts.utils.mylogger import mylogger
from concurrent.futures import ThreadPoolExecutor
from clickhouse_driver import Client as Clickhouse_Client

import pandas as pd
import dask as dd
import numpy as np
from datetime import datetime
import sqlalchemy as sa
import pandahouse
from pymongo import MongoClient
from pprint import pprint

logger = mylogger(__file__)

class PythonMongo:
    # port = '9000'
    # ch = sa.create_engine('clickhouse://default:@127.0.0.1:8123/aion')
    def __init__(self, db):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client['aion']
        collection = 'external'

    def load_data(self, table,start_date,end_date,timestamp_col):

        try:
            #logger.warning('load date range %s:%s',start_date,end_date)
            df = pd.DataFrame(list(self.db[table].find(
                {timestamp_col: {'$lte': end_date, '$gte': start_date}},{'_id':False}
            )))
            if df is not None:
                if len(df) > 0:
                    #logger.warning('external:%s',df.head(5))
                    # add month, day, year columns
                    df['block_month'] = df['date'].dt.month
                    df['block_day'] = df.date.dt.day
                    df['block_year'] = df.date.dt.year

                    df = dd.dataframe.from_pandas(df, npartitions=1)

            return df
        except Exception:
            logger.error('load data',exc_info=True)

    def load_df(self, start_date=None, end_date=None, cols=[], table=None, timestamp_col='timestamp'):
        try:
            if start_date is None and end_date is None:
                df = json_normalize(list(
                    self.db[table].find({}, {'_id': False})))
                #logger.warning('df:%s',df)
            else:
                logger.warning('start date:%s', start_date)
                logger.warning('table:%s', table)
                df = json_normalize(list(self.db[table].find({
                    timestamp_col:
                        {
                            "$gte": start_date,
                            "$lte": end_date
                        }
                }, {'_id': False}).sort(timestamp_col, 1)))
            if df is not None:
                if len(df) > 0:
                    if len(cols) > 0:
                        df = df[cols]
                # logger.warning('df after mongo load:%s',df.head(20))
            return df
        except Exception:
            logger.error('load df', exc_info=True)
