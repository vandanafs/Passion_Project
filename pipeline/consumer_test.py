import getpass
import os
from kafka import KafkaConsumer, TopicPartition
from json import loads
from pyspark import SparkContext
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import json
from flask import Flask
from flask import render_template
from flask_wtf import FlaskForm
from wtforms import StringField
from wtforms.validators import DataRequired
from flask_sqlalchemy import SQLAlchemy
import pymysql
import pandas as pd
import boto3
from pyspark.sql import SparkSession
import mysql.connector
import numpy as np
spark=SparkSession.builder.getOrCreate()
from retrive import *
from models import *
import uuid

#print('Test the type',type(get_secret()))
config=json.loads(get_secret())
print(config['username'],config['password'],config['host'])

#user = 'myuser'
#pass1 = 'myPass05'
db_name = 'collections'
#print(user,pass1)

host=config['host']
user=config['username']
pass1=config['password']
conn = f'mysql+pymysql://{user}:{pass1}@{host}/{db_name}'
'''
client =boto3.client('secretsmanager')

response = client.put_secret_value(
    SecretId='dev/home/myapp'
    
)
secretDict= json.loads(response['SecretString']) # converting string to dict
'''
'''mydb=mysql.connector.connect (
    host=secretDict['host'], #   mapping host name SM to host variable
    user=secretDict['username'],
    passwd=secretDict['password'],
    database=secretDict['dbname'],
)
mycursor=mydb.cursor()'''
'''
user=secretDict['username']
pass1=secretDict['password']
host=secretDict['host']
print(user)
'''


#conn = f'mysql+pymysql://{user}:{pass1}@database-1.cbrnhoat32p7.us-east-1.rds.amazonaws.com/{db_name}'
app = Flask(__name__)
app.config['SECRET_KEY'] = 'SuperSecretKey'
app.config['SQLALCHEMY_DATABASE_URI'] = conn
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False; 
db = SQLAlchemy(app)

Base = declarative_base()
engine=create_engine(f'mysql+pymysql://{user}:{pass1}@{host}/{db_name}')
#engine=create_engine(f'mysql+pymysql://{user}:{pass1}@database-1.cbrnhoat32p7.us-east-1.rds.amazonaws.com/{db_name}')
#engine=create_engine(f'mysql+pymysql://{user}:{pass1}@{host}/{db_name}')
DBSession = sessionmaker(bind=engine)




#items.__table__.create(bind=engine, checkfirst=True)
#Base.metadata.tables["items"].create(bind = engine)
Base.metadata.create_all(engine)
#db_engine=create_engine(f'mysql+pymysql://{user}:{pass1}@localhost')

'''with db_engine.connect() as connect:
    connect.execute(f'Create DATABASE IF NOT EXISTS {db_name}')
    connect.execute(f'use {db_name}')
    connect.execute(f'CREATE TABLE IF NOT EXISTS toys'
                    f'(id INTEGER PRIMARY KEY AUTO_INCREMENT, '
                    f'Title VARCHAR(250) NOT Null,'
                    f'Image VARCHAR(250),'
                    f'adeclarative VARCHAR(250), '
                    f'alinknormal_URL VARCHAR(250), '
                    f'asizebase  VARCHAR(250), '
                    f'aoffscreen VARCHAR(250), '
                    f'Price INTEGER, '
                    f'asizebase2 VARCHAR(250), '
                    f'arow VARCHAR(250), '
                    f'likes VARCHAR(250), '
                    f'scouponunclipped__spancontains_class_acolorbase  VARCHAR(250), '
                    f'arow3  VARCHAR(250), '
                    f'acolorbase VARCHAR(250))')
         
   
                    
engine=create_engine(f'mysql+pymysql://{user}:{pass1}@localhost/{db_name}')
Base = declarative_base(bind=engine)
'''

class ProductCatalogConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('READ_DATA',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            group_id = None,
            #enable.partition.eof = false
            value_deserializer=lambda m: loads(m.decode('utf-8')))
           
    def handleMessages(self):
        
         for message in self.consumer:
            message = json.loads(message.value)
            print(message.keys())
            df = pd.DataFrame(message) #converting message into dataframe 
            print(df.head())

            #df.replace(r'^\s*$', np.nan, regex=True, inplace = True)
            df = df[df['Image'].notna()]  #consider only not null values
            df.replace(r'^\s*$', np.nan, regex=True)
            df['CouponPrice'] = df['CouponPrice'].fillna("")
            print(df.head())  
            #print(df)
            # if if_exists='replace' then append
            
            df.to_sql('items',con=engine,if_exists='replace',index=False)  

            # to sort, processing 
            sparkDF=spark.createDataFrame(df)
            sparkDF.printSchema()

            print("spark dataframe")
            sparkDF.show()
            print("Print rating descending")
            #sparkDF.schema().dataType
            types=[f.dataType for f in sparkDF.schema.fields]
            print(types)
            sparkDF.orderBy(["Price"], ascending=False).show()
            break

          




      

if __name__ == "__main__":
    db.create_all(app=app)
    db.session.commit()
    Base.metadata.create_all(engine)
    c = ProductCatalogConsumer()
    c.handleMessages()