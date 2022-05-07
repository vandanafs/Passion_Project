import getpass
import os
from kafka import KafkaConsumer, TopicPartition
from json import loads
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import json
import pandas as pd
from flask import Flask
from flask import render_template
from flask_wtf import FlaskForm
from flask_sqlalchemy import SQLAlchemy


db_name = 'retailor'
#user = getpass.getuser()
user='myuser'
#pass1 = getpass.getpass(stream=None)
pass1= 'myPass05'

#connection = pymysql.connect( 'database-1.cbrnhoat32p7.us-east-1.rds.amazonaws.com', 'myuser', 'myPass05')
#database-1.cbrnhoat32p7.us-east-1.rds.amazonaws.com
#conn = f'mysql+pymysql://{user}:{pass1}@localhost/{db_name}'
conn = f'mysql+pymysql://{user}:{pass1}@database-1.cbrnhoat32p7.us-east-1.rds.amazonaws.com/{db_name}'
#db_engine=create_engine(f'mysql+pymysql://{user}:{pass1}@localhost')


app = Flask(__name__)
app.config['SECRET_KEY'] = 'SuperSecretKey'
app.config['SQLALCHEMY_DATABASE_URI'] = conn
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False; 
db = SQLAlchemy(app)

Base = declarative_base()
#engine=create_engine(f'mysql+pymysql://{user}:{pass1}@localhost/{db_name}')
engine=create_engine(f'mysql+pymysql://{user}:{pass1}@database-1.cbrnhoat32p7.us-east-1.rds.amazonaws.com/{db_name}')

#engine=create_engine(f'mysql+pymysql://{user}:{pass1}@localhost/{db_name}')
DBSession = sessionmaker(bind=engine)


class items(db.Model):
    id = db.Column(db.Integer,primary_key=True)
    Title = db.Column(db.String(40960))
    Image = db.Column(db.String(40960))
    adeclarative = db.Column(db.String(40960))
    alinknormal_URL = db.Column(db.String(40960)) 
    asizebase = db.Column(db.String(40960))
    aoffscreen = db.Column(db.String(40960))
    Price = db.Column(db.String(40960))
    Price1 = db.Column(db.String(40960))
    asizebase2 = db.Column(db.String(40960))
    arow = db.Column(db.String(40960))
    Like = db.Column(db.String(40960))
    scouponunclipped__spancontains_class_acolorbase  = db.Column(db.String(40960))
    arow3 = db.Column(db.String(40960))
    acolorbase = db.Column(db.String(40960))

    def __init__(self,Title,Image,adeclarative,alinknormal_URL,asizebase,aoffscreen,Price,Price1,asizebase2,arow,Like,scouponunclipped__spancontains_class_acolorbase,arow3,acolorbase):
        self.Title = Title
        self.Image = Image
        self.adeclarative = adeclarative
        self.alinknormal_URL = alinknormal_URL
        self.asizebase = asizebase
        self.asizebase = asizebase
        self.Price = Price
        self.Price1 = Price1
        self.asizebase2 = asizebase2
        self.arow = arow
        self.Like = Like
        self.scouponunclipped__spancontains_class_acolorbase = scouponunclipped__spancontains_class_acolorbase
        self.arow3 = arow3
        self.acolorbase = acolorbase



create_str = "CREATE DATABASE IF NOT EXISTS %s ;" % ('collections')
engine.execute(create_str)
items.__table__.create(bind=engine, checkfirst=True)
#Base.metadata.tables["items"].create(bind = engine)
Base.metadata.create_all(engine)

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
'''         
   
                    
engine=create_engine(f'mysql+pymysql://{user}:{pass1}@localhost/{db_name}')
Base = declarative_base(bind=engine)


class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('READ_DATA',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            group_id = None,
            value_deserializer=lambda m: loads(m.decode('utf-8')))
            #enable.partition.eof = false
        
            #value_deserializer=lambda m: loads(m.decode('ascii')))
        print('am here')
       

    def handleMessages(self):
       
        for message in self.consumer:
            #message = json.loads(message.value)
            #message = message.value
            print('hi')
            #print('{} received'.format(message))
            #self.ledger[message['custid']] = message
            json_data = json.loads(message.value)
            # add message to the transaction table in your SQL usinf SQLalchemy
            for ord in json_data:
                print("title:", ord["Title"])
            with engine.connect() as connection:
               # connection.execute('insert into transactions ({0}, {1}, {2}, {3})'.format(message['custid'], message['type'], message['date'], message['amt']))
                connection.execute("insert into toys(Title,Image,adeclarative,alinknormal_URL,asizebase,aoffscreen,Price,asizebase2,arow,likes,scouponunclipped__spancontains_class_acolorbase,arow3, acolorbase)  values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                #(message['Title'],message['Image'],message['adeclarative'],message['alinknormal_URL'],message['asizebase'],message['aoffscreen'],message['Price'],message['asizebase2'],message['arow'],message['like'],message['scouponunclipped__spancontains_class_acolorbase'],message['arow3'],message['acolorbase']))
                (message['Title'],
                message['Image'],message['adeclarative'],message['alinknormal_URL'],message['asizebase'],message['aoffscreen'],message['Price'],message['asizebase2'],message['arow'],message['like'],message['scouponunclipped__spancontains_class_acolorbase'],message['arow3'],message['acolorbase']))
          
                
        
          




    def handleMessages1(self):
       print('hi1')
    


       for message in self.consumer:
           message = json.loads(message.value)
          # print(message.keys())
           print(message.keys())
           df = pd.DataFrame(message)
           df = df[df['Image'].notna()]
           print(df.head())
        
           df.to_sql('items',con=engine,if_exists='replace',index=False)

if __name__ == "__main__":
    #Base.metadata.create_all(engine)
    c = XactionConsumer()
    #c.handleMessages()
    c.handleMessages1()