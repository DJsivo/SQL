from contextlib import contextmanager

import psycopg2
import mysql.connector

from config import POSTGRES, MYSQL


@contextmanager
def pg_conn():
    conn = psycopg2.connect(
        host=POSTGRES.host,
        port=POSTGRES.port,
        dbname=POSTGRES.dbname,
        user=POSTGRES.user,
        password=POSTGRES.password,
    )
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def mysql_conn():
    conn = mysql.connector.connect(
        host=MYSQL.host,
        port=MYSQL.port,
        database=MYSQL.database,
        user=MYSQL.user,
        password=MYSQL.password,
    )
    try:
        yield conn
    finally:
        conn.close()
