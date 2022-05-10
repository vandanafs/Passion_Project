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
db_name = 'collections'
#print(user,pass1)

host=config['host']
user=config['username']
pass1=config['password']
conn = f'mysql+pymysql://{user}:{pass1}@{host}/{db_name}'




app1 = Flask(__name__)
app1.config['SECRET_KEY'] = 'SuperSecretKey'
app1.config['SQLALCHEMY_DATABASE_URI'] = conn
app1.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False; 
db = SQLAlchemy(app1)

Base = declarative_base()
engine=create_engine(f'mysql+pymysql://{user}:{pass1}@{host}/{db_name}')

DBSession = sessionmaker(bind=engine)




#items.__table__.create(bind=engine, checkfirst=True)
#Base.metadata.tables["items"].create(bind = engine)
#Base.metadata.create_all(engine)




class ProductCatalogConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('READ_DATA',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            group_id = None,
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
    db.create_all(app=app1)
    db.session.commit()
    Base.metadata.create_all(engine)
    c = ProductCatalogConsumer()
    c.handleMessages()