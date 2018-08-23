
# coding: utf-8

# # Task1
# 
# a) Download test.csv from https://www.kaggle.com/rishisankineni/text-similarity/data
# 

# In[38]:


import kaggle
import subprocess
import os
import pandas as pd
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())
import pymysql
import nltk
import numpy as np


os.chdir("C:\\Users\\Administrator\\Desktop\\Kaggle\\q4")
os.system('kaggle datasets download -d rishisankineni/text-similarity')
initial_dataset=pd.read_csv("test.csv")
initial_dataset.head(5)


# # Task2 
# b) Load the data to a Spark/Pandas data frame
# 

# In[4]:


# Loading data into pandas dataframe
df2=initial_dataset
type(df2)


# # Task3
# c) Calculate similarity between description_x and description_y and store resultant
# scores in a new column
# 

# In[52]:


df3=initial_dataset
from difflib import SequenceMatcher

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

for index, row in df3.iterrows():
    val= similar(row[1],row[2]) 
    df3.loc[df3['description_x'] == row[1],"resultant_scores"]=val
df3.head(5)    


# # Task4 
# 
# d) Parallelise the matching process using SPARK environment
# 

# In[ ]:


df4=initial_dataset
from pyspark import SparkConf, SparkContext
from difflib import SequenceMatcher
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, NGram, HashingTF, MinHashLSH


conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

spDF = sqlContext.createDataFrame(df4)

X=spark.createDataFrame(df4['description_x']., "string").toDF("text")
X_parallelize== sc.parallelize(X)

Y=spark.createDataFrame(df4['description_y']., "string").toDF("text")
Y_parallelize== sc.parallelize(Y)

model = Pipeline(stages=[
    RegexTokenizer(
        pattern="", inputCol="text", outputCol="tokens", minTokenLength=1
    ),
    NGram(n=3, inputCol="tokens", outputCol="ngrams"),
    HashingTF(inputCol="ngrams", outputCol="vectors"),
    MinHashLSH(inputCol="vectors", outputCol="lsh")
]).fit(db)



db_hashed = model.transform(db)
query_hashed = model.transform(query)

model.stages[-1].approxSimilarityJoin(db_hashed, query_hashed, 0.75).show()


