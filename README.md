
##Deals Tree (Display the deals from Biggest retailers)
 
In day to today life we all shop around among biggest retailers like Amazon, Walmart and Target.Having an application to list items with smart prices(Coupons/discounts) will help many of us to save few dollars. Over a period, this application can potentially help to save several  hundred dollars.
This application can be extended to have personalized shopping wishlist and get aumoted notification to the individual registered user via airflow.
I have used Octoparse for webscrapping to get data from amazon for category like Toys,and laptop.Stored in AWS S3 bucket,used Kafka to stream  raw data to MYSQL database. As a next step data cleaning will be done by Panda's library to eliminate unnecessary data. Finally, a list of top 25 smart offers which are available.

# Technologies Used:
. Python
. AWS S3 and MYSQL database
. Kafka
. Pandas
. Jupyter Notebook
. Matplotlib/plotly
