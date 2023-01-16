#!/usr/bin/env python
# coding: utf-8

# In[2]:


import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) #This is used to format output tables better
spark


# In[3]:


#import the dataset
titanic_df = spark.read.csv("train.csv", header = True, inferSchema = True)
titanic_df


# In[4]:


#Identify the datatypes of columns
titanic_df.printSchema()


# In[5]:


#shown the datas of 5 raw
titanic_df.limit(5)


# In[6]:


#select specific columns
titanic_df.select('PassengerId', 'Survived')


# In[7]:


#select specific area of data
titanic_df.select('PassengerId', 'Survived').limit(5)


# In[8]:


#select specific area of data
titanic_df.where((titanic_df.Age > 25) & (titanic_df.Survived == 1)).limit(5)


# In[9]:


#Calculate the average of Fare
titanic_df.agg({'Fare':'avg'})


# In[11]:


#Calculate the average of Pclass in decending order
titanic_df.groupBy('Pclass').agg({'Fare':'avg'}).orderBy('Pclass', ascending = False)


# In[13]:


#Filter the Age > 25 Passengers average of Fare
titanic_df.filter(titanic_df.Age > 25).agg({'Fare':'avg'})


# In[18]:


#Error-PicklingError: Could not serialize object: IndexError: tuple index out of range
#This code cannot run because my laptop has the Python 3.11.0 version, and I think this error is with the Python version.

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

def round_float_down(x):
    return int(x)

round_float_down_udf = udf(round_float_down, IntegerType())

titanic_df.select(round_float_down_udf('Fare'))


# In[19]:


titanic_df.createOrReplaceTempView("Titanic")


# In[21]:


spark.sql('select * from Titanic')


# In[ ]:




