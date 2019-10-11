import datetime

from clickhouse_sqlalchemy import make_session
from sqlalchemy import create_engine, MetaData
from tornado.gen import coroutine

from scripts.utils.mylogger import mylogger
from concurrent.futures import ThreadPoolExecutor
from clickhouse_driver import Client

import pandas as pd
import dask as dd
import numpy as np
from datetime import datetime
import sqlalchemy as sa
import pandahouse

executor = ThreadPoolExecutor(max_workers=20)
logger = mylogger(__file__)

class PythonClickhouse:
    # port = '9000'
    def __init__(self,credentials):
        self.host = credentials['host']
        self.db = credentials['db']
        self.ch = sa.create_engine(f"clickhouse://default:password@{credentials['host']}:8123/{credentials['db']}")

        self.client = Client(host=credentials['host'],password=credentials['password'])
        self.client.execute('CREATE DATABASE IF NOT EXISTS amdattds;')
        self.pandahouse_host = f"http://{credentials['host']}:8123"
        self.conn = {'host':self.pandahouse_host,'database':self.db,'password':credentials['password']}
        self.session = make_session(self.ch)
        metadata = MetaData(bind=self.ch)
        metadata.reflect(bind=self.ch)
        self.connection = self.ch.connect()

    def create_database(self, db='frontend'):
        self.db = db
        sql = f"CREATE DATABASE IF NOT EXISTS {self.db}"
        self.client.execute(sql)

    # convert dates from any timestamp to clickhouse dateteime
    def ts_to_date(self, ts, precision='s'):
        try:
            if isinstance(ts, int):
                # change milli to seconds
                if ts > 16307632000:
                    ts = ts // 1000
                if precision == 'ns':
                    ts = datetime.utcfromtimestamp(ts)
                    # convert to nanosecond representation
                    ts = np.datetime64(ts).astype(datetime)
                    ts = pd.Timestamp(datetime.date(ts))
                elif precision == 's':  # convert to ms respresentation
                    ts = datetime.fromtimestamp(ts)

            elif isinstance(ts, datetime):
                return ts
            elif isinstance(ts,str):
                return datetime.strptime(ts,"%Y-%m-%d %H:%M:%S")

            #logger.warning('ts_to_date: %s', ts)
            return ts
        except Exception:
            logger.error('ms_to_date', exc_info=True)
            return ts

    def construct_read_query(self, table, cols, startdate, enddate,
                             timestamp_col='block_timestamp', supplemental_where=None):
        qry = 'SELECT '

        if len(cols) >= 1:
            for pos, col in enumerate(cols):
                if pos > 0:
                    qry += ','
                qry += col
        else:
            qry += '*'

        qry += """ FROM {}.{}""".format(self.db, table)

        qry += """ WHERE toDate({}) >= toDate('{}') AND 
                           toDate({}) <= toDate('{}') """ \
            .format(timestamp_col, startdate, timestamp_col, enddate)

        if supplemental_where is not None:
            qry += supplemental_where

        qry += """ ORDER BY {} """.format(timestamp_col)
        logger.warning('LINE 90: query:%s', qry)
        return qry

    def load_data(self, table, cols, start_date, end_date, timestamp_col='block_timestamp',
                  supplemental_where=None):
        start_date = self.ts_to_date(start_date)
        end_date = self.ts_to_date(end_date)
        # logger.warning('load data start_date:%s', start_date)
        # logger.warning('load_data  to_date:%s', end_date)

        if start_date > end_date:
            logger.warning("END DATE IS GREATER THAN START DATE")
            logger.warning("BOTH DATES SET TO START DATE")
            start_date = end_date
        sql = self.construct_read_query(table, cols, start_date, end_date, timestamp_col,
                                        supplemental_where=supplemental_where)

        try:
            query_result = self.client.execute(sql, settings={'max_execution_time': 3600},
                                               with_column_types=True)
            cols = [col[0] for col in query_result[1]]  # get the names of the columns
            df = pd.DataFrame(query_result[0], columns=cols)
            # if transaction table change the name of nrg_consumed

            logger.warning('columns loaded:%s',df.columns.tolist())
            logger.warning("DATA LOADED:%s", df.head(10))
            return df

        except Exception:
            logger.error(' load data :%s', exc_info=True)

        # cols is a dict, key is colname, type is col type

    def construct_create_query(self, table, table_dict, columns,order_by):
        count = 0
        try:
            qry = 'CREATE TABLE IF NOT EXISTS ' + self.db + '.' + table + ' ('
            logger.warning('%s',qry)
            logger.warning('%s',columns)
            for col in columns:
                #logger.warning("key:%s",col)
                if count > 0:
                    qry += ','
                qry += col + ' ' + table_dict[col]
                #logger.warning("key:value - %s:%s",col,table_dict[col])
                count += 1
            qry += ") ENGINE = MergeTree() ORDER BY ({});".format(order_by)

            #logger.warning('create table query:%s', qry)
            return qry
        except Exception:
            logger.error("Construct table query",exc_info=True)

    def construct_insert_query(self,df,table):
        try:
            qry = 'INSERT INT0 {} (' .format(table)
            for idx,col in enumerate(df.columns):
                if idx > 0:
                    qry += ','
                qry += col

            #make list of multiples

            qry += ') VALUES'
            #logger.warning('insert data query:%s', qry)
            return qry
        except Exception:
            logger.error('construct insert query',exc_info=True)


    def create_table(self, table, table_dict, cols, order_by='id'):
        try:
            self.client.execute('CREATE DATABASE IF NOT EXISTS amdattds;')
            logger.warning('AMDATTDS SUCCESSFULLY CREATED')
            qry = self.construct_create_query(table, table_dict, cols,order_by)
            self.client.execute(qry)
            #self.connection.execute(qry)
            logger.warning('{} SUCCESSFULLY CREATED:%s', table)
        except Exception:
            logger.error("Create table error", exc_info=True)


    def delete(self, item, type="table"):
        if type == 'table':
            self.client.execute("DROP TABLE IF EXISTS {}".format(item))
        logger.warning("%s deleted from clickhouse", item)



    def delete_data(self,start_range, end_range,table,col,
                    db='amdattds'):
        DATEFORMAT = "%Y-%m-%d %H:%M:%S"
        if not isinstance(start_range,str):
            start_range = datetime.strftime(start_range,DATEFORMAT)
        if not isinstance(end_range, str):
            end_range = datetime.strftime(end_range,DATEFORMAT)
        try:
            if 'timestamp' in col:
                qry = """ALTER TABLE {}.{} DELETE WHERE toDate({}) >= toDate('{}') AND 
                    toDate({}) <= toDate('{}')
                """.format(db,table,col,start_range,col,end_range)
            else:
                qry = """ALTER TABLE {}.{} DELETE WHERE {} >= {} and 
                                    {} <= {}
                                """.format(db, table, col, start_range, col, end_range)
            #logger.warning("DELETE QRY:%s",qry)
            self.client.execute(qry)
            #logger.warning("SUCCESSFUL DELETE OVER RANGE %s:%s",start_range,end_range)
        except Exception:
            logger.error("Delete_data", exc_info=True)

    def get_min_max(self,table,col):
        qry = "SELECT min({}), max({}) FROM frontend.{}".format(col,col,table)


    def insert_df(self,df,cols,table,db):
        try:

            cols = sorted(df.columns.tolist())
            #logger.warning("columns in df to insert:%s",df.columns.tolist())
            df = df[cols]  # arrange order of columns for
            #logger.warning('df:%s',df.head())
            #logger.warning('table:%s',table)
            affected_rows = pandahouse.to_clickhouse(df, table=table, connection=self.conn, index=False)
            logger.warning("DF UPSERTED:%s rows", affected_rows)

        except:
            logger.error("insert_df", exc_info=True)

    def upsert_df(self,df,cols,table,col,df_type='pandas',db='amdattds'):
        try:
            #logger.warning(" df at start of upsert :%s",df.head())
            if df_type == 'dask':
                df = df.compute()

            #logger.warning('timestamp col:%s',df[['hour','month']])

            """
            - get min max of range to use as start and end of range
            - delete data
            - insert data
            """
            #logger.warning('before upsert: %s',df.head(10))
            start_range = df[col].min()
            end_range = df[col].max()
            #logger.warning('upsert delete range: start:end %s:%s',start_range,end_range)
            self.delete_data(start_range,end_range,table,col=col)
            self.insert_df(df,cols=cols,table=table,db=db)

        except Exception:
            logger.error("Upsert df", exc_info=True)


