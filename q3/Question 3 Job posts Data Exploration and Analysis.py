
# coding: utf-8

# # Task1 
# a) Reuse code from Q2 to download the following Kaggle dataset:
# Jobposts Data: https://www.kaggle.com/madhab/jobposts/
# 

# In[84]:


import kaggle
import subprocess
import os
import pandas as pd
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())
import pymysql
import nltk
import numpy as np
nltk.download('stopwords')


os.chdir("C:\\Users\\Administrator\\Desktop\\Kaggle\\q3")
os.system('kaggle datasets download -d madhab/jobposts')
initial_dataset=pd.read_csv("data job posts.csv")
initial_dataset.head(5)


# # Task 2
# b) Extract the following fields from the jobpost column:
#  1. Job Title
#  2. Position Duration
#  3. Position Location
#  4. Job Description
#  5. Job Responsibilities
#  6. Required Qualifications
#  7. Remuneration
#  8. Application Deadline
#  9. About Company

# In[16]:


import re
extract = pd.DataFrame()
#print (re.search(re.escape(start)+"(.*)"+re.escape(end),initial_dataset['jobpost']).group(1))
#initial_dataset[initial_dataset.jobpost.str.startswith('JOB TITLE')]

#1> 'Job Title'
extract['Job Title']=initial_dataset.jobpost.str.split('TITLE:').str[1].str.split('\r\n').str[0].str.strip()


#2> 'Position Duration'
extract['Position Duration']=initial_dataset.jobpost.str.split('DURATION:').str[1].str.split('\r\n').str[0].str.strip()

#3> 'Position Location'
extract['Position Location']=initial_dataset.jobpost.str.split('LOCATION:').str[1].str.split('\r\n').str[0].str.strip()


#4> Job Description
initial_dataset.jobpost.str.split('DESCRIPTION:').str[1].str.split('RESPONSIBILITIES:').str[0].str.strip()


#5> Job Responsibilities
extract['Job Responsibilities']=initial_dataset.jobpost.str.split('RESPONSIBILITIES:').str[1].str.split('QUALIFICATIONS:').str[0].str.strip()
extract['Job Responsibilities']=extract['Job Responsibilities'].str[1:]

#6> Required Qualifications
extract['Required Qualifications']=initial_dataset.jobpost.str.split('QUALIFICATIONS:').str[1].str.split(':').str[0].str.strip()


#7> Remuneration
extract['Remuneration']=initial_dataset.jobpost.str.split('REMUNERATION:').str[1].str.split('\r\n').str[0].str.strip()


#8>  Application Deadline
extract['Application Deadline']=initial_dataset.jobpost.str.split('DEADLINE:').str[1].str.split('\r\n').str[0].str.strip()


#9> About Company
extract['About Company']=initial_dataset.jobpost.str.split('COMPANY:').str[1].str.split('------------------').str[0].str.strip()


# # Task3
# 
# 
# c) Identify the company with the most number of job ads in the past 2 years

# In[5]:


mylist=initial_dataset['Year']
print(list(set(mylist)) )

df3=initial_dataset.loc[initial_dataset['Year'].isin(['2014','2015'])]
query1=""" Select Company,sum(total_openings) as total_openings from 
       (Select Year,Company,count(*) as total_openings from df3 group by Year,Company) 
       group by Company order by total_openings desc  limit 10 """
print(pysqldf(query1))
companies=pysqldf(query1)

query2=""" Select Company from companies where total_openings=(Select max(total_openings) from companies)   """
print("most openings is in "+ pysqldf(query2) )


# # Task4
# d) Identify the month with the largest number of job ads over the years
# 

# In[6]:


df4=initial_dataset
query3="""Select Year,Month,count(*) as total_opening_count from df4 group by Year,Month order by total_opening_count desc limit 5 """

openings=pysqldf(query3)

query4=""" Select Year,Month from openings where total_opening_count =(Select max(total_opening_count) from openings) """
pysqldf(query4)


# # Task5
# e) Clean text and generate new text from Job Responsibilities column: The new text
# shall not contain any stop words, and the plural words shall be converted into
# singular words.
# 

# In[90]:


from nltk.corpus import stopwords
from pattern.en import pluralize, singularize
from textblob import Word
stop = stopwords.words('english')

pat = r'\b(?:{})\b'.format('|'.join(stop))
extract['Responsibilities_without_stopwords'] = extract['Job Responsibilities'].str.replace(pat, '')
extract['Responsibilities_without_stopwords'] = extract['Responsibilities_without_stopwords'].str.replace(r'\s+', ' ')

s=extract['Responsibilities_without_stopwords'].replace(np.nan, '', regex=True)
extract['Responsibilities_without_stopwords_and_plural']=s.apply(lambda w: Word(w).singularize())
extract['Responsibilities_without_stopwords_and_plural'].head(5)


# # Task6
# f) Write functions to identify null/NA values and to replace null/NA values with a
# custom message in “Duration” column
# 

# In[154]:


df6=initial_dataset

def replace_val(df_name,col_name):
  df_name[col_name] = df_name[col_name].fillna(value="missing_value")

replace_val(df6,'Duration')
df6['Duration']


# # Task7
# g) Store the results in a new Dataframe/SQL table(s)
# 

# In[162]:


df7=df6
print(type(df7))
from sqlalchemy import create_engine
import pymysql

engine = create_engine("mysql://root:root@localhost/kaggle?charset=utf8")
connnection_name = engine.connect()
df7.to_sql(name='table_name',con=connnection_name, if_exists='append', index=False)


# # Task8
# h) Write the results to an S3 bucket

# In[164]:


import boto3
df7.to_csv('s3_copy.csv',index=False,sep=',',header=False,)
filename = 's3_copy.csv'
s3 = boto3.resource('s3',aws_access_key_id="XXXXXXXXXX",aws_secret_access_key="YYYYYYYYYYYYYY")
s3.Bucket('bucket_name').put_object(Key=filename, Body=open(filename,'rb'), ContentType='text/csv')

