
# coding: utf-8

# In[1]:


from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
conf = SparkConf()
conf.setMaster('local[2]')
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from the above spark context with interval size 1 seconds
ssc = StreamingContext(sc, 1)
# setting a checkpoint to allow RDD recovery ssc.checkpoint("checkpoint_TwitterApp")
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("localhost",9010)


# In[2]:


def sum_tags_count(new_values, total_sum): 
    return sum(new_values) + (total_sum or 0)
def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']
def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time)) 
    try:
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
        
        # create a DF from the Row RDD
        hashtags_df = sql_context.createDataFrame(row_rdd)
        # Register the dataframe as table 
        hashtags_df.registerTempTable("hashtags")

        # get the top 10 hashtags from the table using SQL and print them
        hashtag_counts_df = sql_context.sql("select hashtag, hashtag_count from hashtags where hashtag='#HEATCulture' or hashtag='#PureMagic' or hashtag='#Hornets30' or hashtag='#Rockets⁠' or hashtag='#NBA' or hashtag='#NewYorkForever' or hashtag='#HereTheyCome' or hashtag='#KiaTipOff18' or hashtag='#ThisIsWhyWePlay' or hashtag='#WeTheNorth' order by hashtag_count desc limit 10")
       # hashtag_counts_df.show()
    except:
        e = sys.exc_info()[0]
        print(e)
    finally:
        hashtag_counts_df.show()
    


# In[3]:


# split each tweet into words
words = dataStream.flatMap(lambda line: line.split(" "))
# filter the words to get only hashtags, then map each hashtag tobe a pair of (hashtag,1)
hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1)) 
# adding the count of each hashtag to its last count
tags_totals = hashtags.updateStateByKey(sum_tags_count)
# do processing for each RDD generated in each interval 
tags_totals.foreachRDD(process_rdd)
# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()
print("finished")


# In[4]:


ssc.stop()

