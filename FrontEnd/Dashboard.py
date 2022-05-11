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
#import plotly.graph_objects as px

header=st.container()
dataset=st.container()

def make_clickable(val):
    #return f'<a target="_blank" href="{val}">{val}</a>'
    return '<a href="{}">{}</a>'.format(val,val)

with header:
    st.title('Welcome to Deals Tree!!')
    st.image('/Users/vandana/myProj/Passion_Project/FrontEnd/deals.jpeg')
    st.sidebar.header('User Input Features')
with dataset:
    st.header('Amazon toys dataset')
    df = pd.read_csv('/Users/vandana/myProj/Passion_Project/FrontEnd/toys.csv')
    

    #df1.style.format({'url':})
    df.style.format({'ProdLink': make_clickable})
    #st.write(HTML(df.head().to_html(render_links=True,col_space='10px')))
    st.write(df.style.format({'deals': '{:.2f}'}), width=5000, height=1000)
    

    df2=df
    df2['bins']=pd.cut(df['PercentReduction'],bins=[0,30,45,65], labels=["0-30","30-45","45+"])
    df3=df2
    df3=df3.groupby(['PercentReduction','bins']).size().unstack(fill_value=0)
    bin_percent = pd.DataFrame(df2['bins'].value_counts(normalize=True)*100)
    plot = bin_percent.plot.pie(y='bins',figsize=(5,5), autopct='%1.1f%%')
    #print(plot)
    # st.write(plot)
    #st.pyplot(plot)
    #st.plotly_chart(plot)
    #input_col, pie_col = st.beta_columns(2)
    #st.write(df3)
    #df3 = df3.reset_index()
    #df3.columns = ['PercentReduction', 'bins']
    #fig = px.pie(df3, values = 'bins')
    #pie_col.write(fig)
   #st.plotly_chart(fig)
    
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
