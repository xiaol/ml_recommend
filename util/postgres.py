# coding: utf-8
from psycopg2 import pool
import psycopg2.extras
from conf import DEBUG

__author__ = "Laite Sun"
__copyright__ = "Copyright 2016-2019, ShangHai Lie Ying"
__license__ = "Private"
__email__ = "sunlt@lieying.cn"
__date__ = "2017-03-17 11:35"

class Postgres(object):

    db_name = "BDP"
    db_user = "postgres"
    # db_host = "10.47.54.175"
    db_host = "120.27.162.201"
    db_password = "ly@postgres&2015"
    min_connections = 1
    max_connections = 5

    def __init__(self, db_host= "120.27.162.201"):
        self.pool = pool.SimpleConnectionPool(
            minconn=self.min_connections,
            maxconn=self.max_connections,
            database=self.db_name,
            user=self.db_user,
            host=self.db_host,
            password=self.db_password,
        )

    def run(self, sql):
        pass

    def query(self, sql):
        connection = self.pool.getconn()
        rows = list()
        try:
            cursor = connection.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
        except Exception, e:
            connection.rollback()
        finally:
            self.pool.putconn(connection)
        return rows

    def query_dict_cursor(self, sql):
        connection = self.pool.getconn()
        rows = list()
        try:
            cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute(sql)
            rows = cursor.fetchall()
        except Exception, e:
            connection.rollback()
        finally:
            self.pool.putconn(connection)
        return rows

    def insert(self, sql):
        connection = self.pool.getconn()
        ret = False
        try:
            cursor = connection.cursor()
            cursor.execute(sql)
            connection.commit()
            ret = True
        except Exception, e:
            connection.rollback()
        finally:
            self.pool.putconn(connection)
            return ret

    def get_cur(self):
        connection = self.pool.getconn()
        cur = connection.cursor()
        return connection, cur
if DEBUG:
    postgres_read_only = Postgres()
else:
    postgres_read_only = Postgres(db_host='10.47.54.175')

# postgres_write_only = Postgres(db_host="120.27.163.25")
