from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.ext.declarative import declarative_base
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


#Credentials to Access Database
user = 'myuser'
pass1 = 'mypass'
db_name = 'collections'

#Creating Connection String to Database
conn = f'mysql+pymysql://{user}:{pass1}@localhost/{db_name}'

db = SQLAlchemy()

Base = declarative_base()
engine=create_engine(f'mysql+pymysql://{user}:{pass1}@localhost/{db_name}')
DBSession = sessionmaker(bind=engine)
#items.__table__.create(bind=engine, checkfirst=True)
Base.metadata.create_all(engine)
