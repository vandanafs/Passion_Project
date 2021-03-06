import csv
from time import sleep
from json import dumps
from kafka import KafkaProducer
import time
import random
import boto3
import pandas as pd
import s3fs
import json
import io
from s3fs.core import S3FileSystem  # pandas uses s3fs for s3 conn
import numpy as np
import locale
'''
 Passing each row entry of couponSummary column, price column to see price difference
 # save 10%,
 # save &6.00, these are 2 possible values in that column
'''
def create_value(row,price):
    
    if len(str(row))==0:
      return price
    if '%' in row and 'Save' in row:
      return price - (price * float(row.split(" ")[1].split("%")[0])/100)
    elif '$' in row:
       return price - float(row.split('$')[1])
    else:
        return price
        
def cleanData(path1):

    df=pd.read_csv(path1)
    print("Totals records on data set",df.shape)
    # limiting these columns
    new_df=df[['Title','Image','adeclarative','alinknormal_URL','asizebase','aoffscreen','Price1','arow','Like']] 
     # removing sponsored ad words from data set
    df1=new_df[~new_df.adeclarative.str.contains("Sponsored\n",na=False)]
    df1.head()
    

    df1=df1.rename(
        columns={
            'adeclarative':'Rating',
            'Image':'Image',
            'alinknormal_URL':'ProdLink',
            'asizebase':'TotalReviews',
            'aoffscreen':'Price',
            'Price1':'OrgPrice',
            'arow':'CouponsDesc',
            'Like':'CouponsSummery'

        })
    #removing the white space
    df = df1.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    
    #
    cols = df1.select_dtypes(['object']).columns 
     # each column header remove white space
    df1[cols] = df1[cols].apply(lambda x: x.str.strip()) 
    
    #
    df = df1[df1['Image'].notna()]
    df = df1[df1['ProdLink'].notna()]
    df = df1[df1['Title'].notna()]

    #Handling Nan in Total Reviews, replace with 0
    df['TotalReviews'] = df['TotalReviews'].replace(np.nan, 0)

    #Fill Missing Values of price and original price, if any of the column is null fill with vice versa
    df.Price.fillna(df.OrgPrice, inplace=True)
    df.OrgPrice.fillna(df.Price, inplace=True)

    #Print Missing Values for reference, null values from DS
    df[df.isnull().any(axis=1)]

    #Removing Commas in the  TotalReviews columns and converting them to float
    df['TotalReviews'] = df['TotalReviews'].str.replace(',', '').astype(float)

    #handle default data type, format
    locale.setlocale(locale.LC_ALL,'')
    df['Price']=df.Price.map(lambda x: locale.atof(str(x).strip('$')))
    df['OrgPrice']=df.OrgPrice.map(lambda x: locale.atof(str(x).strip('$')))

    # initially data type is object, converting to numeric
    df['Price'] = pd.to_numeric(df['Price'])
    df['OrgPrice'] = pd.to_numeric(df['OrgPrice'])

    df.dtypes
    #if this column doesn't have values fill null
    df['CouponsSummery'] = df['CouponsSummery'].fillna('')
    #passing row[couponSummary] and row[price] column price for Couponed price
    df['CouponPrice'] = df.apply(lambda row: create_value(row['CouponsSummery'],row['Price']), axis=1)
    df['PercentReduction'] = df.apply(lambda row: abs(((row.CouponPrice - row.OrgPrice)/row.OrgPrice) * 100), axis=1)
    df.head(10)


     #row which have empty values, removing '/" 
    df.replace(r'^\s*$', np.nan, regex=True, inplace = True)
    #print(df.head())
    return df        

class Producer:

    def __init__(self):
        
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092')
       
    def emit(self):
        #path1='s3://productscsv/toys.csv'
        #path1='s3://productscsv/hp_laptops.csv'
        path1='s3://productscsv/hp_laptops_500rows.csv'
        #df_toys=pd.read_csv(path1)
        df_toys=cleanData(path1)
        print(df_toys)
        #Converting into json
        df_toys2 = df_toys.to_json()
        for col in df_toys.columns:
            print('Column header', col)
        return json.dumps(df_toys2).encode("utf-8")

  

    def sendProductCatalog(self):
        #self.producer.send('READDATA_PROJECT', b'Hello, World!')
        #self.producer.send('READDATA_PROJECT', key=b'message-two', value=b'This is Kafka-Python')
        data = self.emit()
        #print('Test Data', data)
        #self.producer.send(topic='READ_LAPTOP', value=data)
        self.producer.send(topic='READ_DATA', value=data)
        #data5 = json.dumps(data)
        self.producer.flush()
        self.producer.close()
       

if __name__ == "__main__":
    p = Producer()
    p.sendProductCatalog()
