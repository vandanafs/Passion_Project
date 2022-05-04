import getpass
import os
from kafka import KafkaConsumer, TopicPartition
from json import loads
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import json


db_name = 'retailer1'
user = getpass.getuser()
pass1 = getpass.getpass(stream=None)
db_engine=create_engine(f'mysql+pymysql://{user}:{pass1}@localhost')

with db_engine.connect() as connect:
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


class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('READ_DATA',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            group_id = None
            #enable.partition.eof = false
            )
            #value_deserializer=lambda m: loads(m.decode('ascii')))
        print('am here')
        #for msg in self.consumer:
         # print('am here too')
          #print("print topic name","Topic Name=%s,Message=%s"%(msg.topic,msg.value))

        ## These are two python dictionarys
        # Ledger is the one where all the transaction get posted

        #self.ledger = {}

        # custBalances is the one where the current blance of each customer
        # account is kept.

        #self.custBalances = {}

        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.

        #Go back to the readme.



    def handleMessages(self):
        print('hi1')
        #print(self.consumer)
        #for msg in self.consumer:
          #print("print topic name","Topic Name=%s,Message=%s"%(msg.topic,msg.value))


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
          
                
          #  if message['custid'] not in self.custBalances:
               # self.custBalances[message['custid']] = 0
            #if message['type'] == 'dep':
              #  self.custBalances[message['custid']] += message['amt']
            #else:
                #self.custBalances[message['custid']] -= message['amt']
            #print(self.custBalances)
          




      

if __name__ == "__main__":
    Base.metadata.create_all(engine)
    c = XactionConsumer()
    c.handleMessages()