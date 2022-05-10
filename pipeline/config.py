from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.ext.declarative import declarative_base
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
from dev import *
from prod import *


def initDb():
    config = {}
    if os.environ['dbenv'] == "dev":
        config = getDevCreds()
    if os.environ['dbenv'] == "prod":
        config = getProdCreds()

    db_name = 'collections'
    host = config['host']
    user = config['username']
    pass1 = config['password']

#Creating Connection String to Database
    conn = f'mysql+pymysql://{user}:{pass1}@localhost/{db_name}'
    db = SQLAlchemy()

    #Creating table if not exists
    dbconn = pymysql.connect(host=host, user=user, passwd=pass1, connect_timeout=10)
    with dbconn.cursor() as cur:
        cur.execute(f'Create DATABASE IF NOT EXISTS {db_name}')
        cur.execute(f'use {db_name}')


    print("Using Config: ",os.environ['dbenv']," ",config)
    return conn,db



initDb()