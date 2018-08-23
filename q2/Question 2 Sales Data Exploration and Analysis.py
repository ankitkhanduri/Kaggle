
# coding: utf-8

# # Task 1
# a) Write code to download the following Kaggle dataset:
# Weekly Sales Transaction Data: https://www.kaggle.com/crawford/weekly-sales-transactions
# 

# In[137]:


import kaggle
import subprocess
import os
import pandas as pd
import numpy as np
from numpy import median
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())
import pymysql
from dateutil.relativedelta import relativedelta

os.chdir("C:\\Users\\Administrator\\Desktop\\Kaggle\\q2")
os.system('kaggle datasets download -d crawford/weekly-sales-transactions')
initial_dataset=pd.read_csv("Sales_Transactions_Dataset_Weekly.csv")
initial_dataset.head(5)


# # Task2 
# 
# b) Find median, mean, min and max values for each product
# 

# In[469]:


#df=initial_dataset.iloc[:, 0:53]
#median=df.groupby("Product_Code")[['W'+ str(x) for x in range(1,52)]].apply(np.median)
#df['mean'] = df.mean(axis=1)
#df['median'] = df.median(axis=1)
#df['min'] = df.min(axis=1)
#df['max'] = df.max(axis=1)
#print(df)


# In[467]:


#getting the subset of the data to calculate the asked values
df=initial_dataset.iloc[:, 0:53]
df['mean'] = df.mean(axis=1)
df['median'] = df.median(axis=1)
df['min'] = df.min(axis=1)
df['max'] = df.max(axis=1)
print(df)


# # Task3 
# c) Identify the best performing product (based on volume)

# In[180]:


df1=initial_dataset.iloc[:, 0:53]
df1['total_sold']=df1.sum(axis=1)
query1="""Select product_code,total_sold from df1 group by product_code,total_sold order by total_sold desc limit 50"""
best_performance=pysqldf(query1)
print(best_performance)


# # Task4
# d) Identify the most promising product (emerging product)
# 

# In[191]:


df2=initial_dataset.iloc[:, 0:53]
df2['mode']=df2.mode(axis=1)[0]
query2="""Select product_code,mode from df2 group by product_code,mode order by mode desc limit 50"""
promising_product=pysqldf(query2)
print(promising_product)


# # Task5
# e) Identify the top 5 worst performing products on a biweekly basis

# In[449]:


df3=initial_dataset.iloc[:, 0:53]
df3.iloc[:, 1:2].sum(axis=1).sort_values( ascending=True).head(5)
paired_weeks=[ l[i:i+n] for i in range(0, len(l), n) ]
for idx,item in enumerate(paired_weeks):
    df3["biw_"+str(idx)] = df3.apply(lambda row: row[item[0] : item[1]].sum(),axis=1)
newpd=df3.iloc[:,53:]
newpd["product"]=df3.iloc[:,0]
output = pd.DataFrame()
for i in newpd[newpd.columns.difference(['product'])]:
    print(newpd[['product',i]].sort_values(by=i,ascending=True).head(5))
    


# #  Task6
# f) Identify outliers from the data and output the corresponding week numbers

# In[453]:


from scipy import stats
def get_outliers(sales_data):
    print('Outliers by week \n')
    for index,row in sales_data.loc[:, sales_data.columns[:53]].iterrows():
        outlier = row[1:][stats.zscore(row[1:])>3]
        if(len(outlier.index)):
            print('Product',row[0],',Outlier -> ',','.join(outlier.index.values.astype(str)),',Value -> ',','.join(outlier.values.astype(str)))
        else:
            print('Product',row[0],'No outlier')

get_outliers(df3)


# # Task7 
# g) On the next page is some code that is used find outliers for the question above.
# Refactor the code to improve its readability. Bonus points for improving its speed
# 
# 
# 

# In[458]:


import time
def get_outliers(sales_data):
    start_time = time.clock()
    print('Outliers by week \n')
    sales_np_array = np.array(sales_data.loc[:, sales_data.columns[1:53]])
    print("\n")
    for row in sales_np_array:
        mean = np.mean(row)
        std = np.std(row)
        scores = [(i-mean) / std for i in row]
        index_data = np.array(np.where(np.abs(scores) > 3)).tolist()
        print(sales_data.columns[[i for index in index_data for i in index]])
    print (time.clock() - start_time, "seconds")
get_outliers(df3)


# In[464]:


import time
from scipy import stats
def get_outliers(sales_data):
    start_time = time.clock()
    print('Outliers by week \n')
    for index,row in sales_data.loc[:, sales_data.columns[:53]].iterrows():
        outlier = row[1:][stats.zscore(row[1:])>3]
        if(len(outlier.index)):
            print(row[0],','.join(outlier.index.values.astype(str)))
    print (time.clock() - start_time, "seconds")

get_outliers(df3)

