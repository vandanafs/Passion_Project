import getpass
import os
from kafka import KafkaConsumer, TopicPartition
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
import numpy as np
import uuid
import random
#from models import *
from processData import *
from config import *

print("Consumer Starting")
conn,db=initDb()

app = Flask(__name__)
app.config['SECRET_KEY'] = 'SuperSecretKey'
app.config['SQLALCHEMY_DATABASE_URI'] = conn
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False; 
db.init_app(app)


class ProductCatalogConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('READ_DATA',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            group_id = None,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')))
           
    def handleMessages(self):
        
         for message in self.consumer:
            message = json.loads(message.value)
            print(message.keys())
            df = pd.DataFrame(message) #converting message into dataframe 
            print(df.head())

            df.reset_index()
            df['id']=np.arange(len(df))
            df.to_sql('items',con=conn,if_exists='replace',index=False)  
            break
            # to sort, processing 
            
          




      

if __name__ == "__main__":
    
    c = ProductCatalogConsumer()
    c.handleMessages()