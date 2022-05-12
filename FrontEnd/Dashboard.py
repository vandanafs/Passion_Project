import streamlit as st
import pandas as pd
import time
import base64
import pymysql
import boto3
import pandas as pd
import numpy as np
import s3fs
import locale
import seaborn as sns
import matplotlib.pyplot as plt
from IPython.display import HTML
import plotly.graph_objects as px
import plotly.express as px2
from PIL import Image
from pathlib import Path
header=st.container()
dataset=st.container()
# Banner image
image = st.container()
def make_clickable(val):
    #return f'<a target="_blank" href="{val}">{val}</a>'
    return '<a href="{}">{}</a>'.format(val,val)  

def img_to_bytes(img_path):
    img_bytes = Path(img_path).read_bytes()
    encoded = base64.b64encode(img_bytes).decode()
    return encoded

with image:
    banner = Image.open('deals.jpeg')
    st.image(banner, width=810, use_column_width=True)

with st.sidebar:
    st.sidebar.header('Choose a category')
   
    select_platform = st.selectbox('Category Select', ['Toys',
                                                         'Laptops'])
   
with header:
    st.title('Welcome to Deals Tree!!')
    #st.image('/Users/vandana/myProj/Passion_Project/FrontEnd/deals.jpeg')
    header_html = "<img src='data:image/png;base64,{}' class='img-fluid' width=700 height=150>".format(
    img_to_bytes("deals.jpeg")
    )
    st.markdown(
    header_html, unsafe_allow_html=True,
    )
with dataset:
    st.header('Amazon:'+select_platform)
    #st.write(select_platform)
    if select_platform == 'Toys':
    #df = pd.read_csv('/Users/vandana/myProj/Passion_Project/FrontEnd/toys.csv')
        #st.write('here toys')
        df = pd.read_csv('/Users/vandana/myProj/Passion_Project/FrontEnd/toys.csv')

    if select_platform == 'Laptops':
        #st.write('here laptops')
        df = pd.read_csv('/Users/vandana/myProj/Passion_Project/FrontEnd/laptopdf.csv')
    df.drop(['No'],axis='columns', inplace=True)

    st.dataframe(df)    

    df2=df
    df2['bins']=pd.cut(df['PercentReduction'],bins=[0,30,45,65], labels=["0-30%","30-45%","45+%"])
    
    df3=df2
    #st.write(df3)
    
    

    bin_percent = pd.DataFrame(df2['bins'].value_counts(normalize=True)*100)
    plot = bin_percent.plot.pie(y='bins',figsize=(5,5), autopct='%1.1f%%')
    #st.write(plot)
   

    #st.write(df2.iloc[:,-1].value_counts().plot.pie(autopct="%1.1f%%"))
    #st.pyplot(bin_percent.iloc[:,-1].value_counts().plot.pie(autopct="%1.1f%%"))
    
    #st.write(bin_percent.head())
    st.header("Deals Distribution based on % Discount")
    #fig2=px2.pie(bin_percent,labels=bin_percent['index'], values='bins', names='bins')

    fig2=px2.pie(bin_percent, values='bins', names=bin_percent.index)
    
    st.plotly_chart(fig2)

   #reviews based
    df5=df
    df5['bins']=pd.cut(df['TotalReviews'],bins=[0,500,1000,100000], labels=["0-500","500-1000","1000+"])

    bin_percent2 = pd.DataFrame(df5['bins'].value_counts(normalize=True)*100)    
    st.header("Deals Distribution based on # of Reviews")
    fig3=px2.pie(bin_percent2, values='bins', names=bin_percent2.index)
    
    st.plotly_chart(fig3)


    #working
    perc = pd.DataFrame(df['PercentReduction']).head(20)
    st.bar_chart(perc)




def sidebar_bg(side_bg):
    side_bg_ext = 'jpeg'

    st.markdown(
        f"""
      <style>
      [data-testid="stSidebar"] > div:first-child {{
          background: url(data:image/{side_bg_ext};base64,{base64.b64encode(open(side_bg, "rb").read()).decode()});
          background-size: cover;
      }}
      </style>
      """,
        unsafe_allow_html=True,
    )
