#!/usr/bin/env python
# coding: utf-8

# In[71]:


from pyspark.sql import SparkSession


# In[72]:


spark = SparkSession.builder \
.appName("Test Transformation Script") \
.getOrCreate()


# In[73]:


print(spark)


# In[74]:


df = spark.read.json("issues.json")


# In[75]:


df.printSchema()
from pyspark.sql.functions import col, to_timestamp


# In[76]:


def select_col(df):
    selected_df = df.select(
        col("id"),
        col("reporter.name"),
        col("created_at"),
        col("updated_at"),
        col("status"),
        col("summary"),
        col("description"),
        col("lat"),
        col("lng"),
        col("address"),
        col("url"),
        col("reporter.role"),
        col("request_type.organization"),
        col("request_type.title"),
        col("request_type.url"),
        col("private_visibility"),
        col("rating")
    )
    
    return selected_df


# In[77]:


df = select_col(df)
df.printSchema()


# In[78]:


df.printSchema()
from pyspark.sql import functions as F
from pyspark.sql import DataFrame


# In[79]:


def rename_columns(df: DataFrame) -> DataFrame:

    r_dict = {
        "id": "id",
        "name": "reporter_name",
        "created_at": "issue_created_at",
        "updated_at": "issue_updated_at",
        "status": "issue_status",
        "summary": "issue_summary",
        "description": "issue_description",
        "lat": "latitude",
        "lng": "longitude",
        "address": "issue_address",
        "url": "url",
        "role": "reporter_role",
        "organization": "organization",
        "title": "title",
        "private_visibility": "is_private",
        "rating": "rating"
    }

    for old_name, new_name in r_dict.items():
        df = df.withColumnRenamed(old_name, new_name)
    
    return df


# In[80]:


df1 = rename_columns(df)


# In[81]:


df1.printSchema()


# In[88]:


df1.show(1)


# In[103]:


from pyspark.sql.functions import col, to_timestamp, to_date
from pyspark.sql.types import LongType, StringType, DoubleType, BooleanType

def convert_data_types(df: DataFrame) -> DataFrame:
    # Convert to appropriate data types
    df = df.withColumn("created_at", to_timestamp(col("created_at"))) \
           .withColumn("updated_at", to_timestamp(col("updated_at")))

    return df



# In[108]:


df2 = df1.withColumnRenamed(df1.columns[9], "issue_url")
df2.printSchema()
df2 = convert_data_types(df2)
df2.show(1)


# In[106]:


df2.printSchema()
from pyspark.sql.functions import col, isnan, when, count


# In[110]:


df = df2.drop("reporter_url")


# In[111]:


df.printSchema()


# In[112]:


df.show(2)


# In[113]:


from pyspark.sql.functions import sum


# In[115]:


missing_values_count = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
missing_values_count.show()


# In[116]:





# In[ ]:




