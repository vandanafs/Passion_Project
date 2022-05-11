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


header=st.container()
dataset=st.container()

def make_clickable(val):
    #return f'<a target="_blank" href="{val}">{val}</a>'
    return '<a href="{}">{}</a>'.format(val,val)

with header:
    st.title('Welcome to page')
    st.image('/Users/vandana/myProj/Passion_Project/FrontEnd/deals.jpeg')
    st.sidebar.header('User Input Features')
with dataset:
    st.header('Amazon toys dataset')
    df = pd.read_csv('/Users/vandana/myProj/Passion_Project/FrontEnd/toys.csv')
    

    #df1.style.format({'url':})
    df.style.format({'ProdLink': make_clickable})
    #st.write(HTML(df.head().to_html(render_links=True,col_space='10px')))
    st.write(df.style.format({'deals': '{:.2f}'}), width=5000, height=1000)

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
