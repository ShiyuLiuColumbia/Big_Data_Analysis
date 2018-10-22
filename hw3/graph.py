
# coding: utf-8

# In[1]:


import findspark
findspark.init()


# In[2]:


from pyspark import SQLContext
from pyspark import SparkContext
sc = SparkContext()
sqlContext = SQLContext(sc)
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import HashingTF, IDF


# In[3]:


from pyspark.sql import SparkSession
# Build a SparkSession; SparkSession provides a single point of entry to interact with underlying Spark functionality
spark = SparkSession    .builder    .appName("similairityExample")    .getOrCreate()


# In[12]:


df = sqlContext.read.json('/home/sl4401/AA/wiki_**')
df = df.limit(1005)


# In[13]:


regexTokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="[^A-Za-z]+", toLowercase=True)
tokenized_data = regexTokenizer.transform(df)
stopWordsRemover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
filtered_data = stopWordsRemover.transform(tokenized_data)
hashingTF = HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=20)
featurizedData = hashingTF.transform(filtered_data)
idf= IDF(inputCol="raw_features", outputCol="features")
idfModel = idf.fit(featurizedData)
featurized_data = idfModel.transform(featurizedData)


# In[14]:


from pyspark.ml.feature import Normalizer
normalizer = Normalizer(inputCol="features", outputCol="norm")
data = normalizer.transform(featurized_data)


# In[15]:


import math
import pyspark.sql.functions as psf
from pyspark.sql.types import DoubleType
dot_udf = psf.udf(lambda x,y: float(x.dot(y)), DoubleType())
s=data.alias("i").join(data.alias("j"), psf.col("i.id") < psf.col("j.id"))      .select(
          psf.col("i.id").alias("src"), 
          psf.col("j.id").alias("dst"), 
          dot_udf("i.norm", "j.norm").alias("relationship"))\
      .sort("src", "dst")\


# In[ ]:


#run in the spark shell
v = featurized_data.select("id","features")
e = s.filter("relationship > 0.8")
from graphframes import *
g= GraphFrame(v,e)
g.vertices.show()
g.edges.show()
results = g.pageRank(resetProbability=0.15, maxIter=10)#pagerank
results.vertices.select("id","pagerank").show()
results.edges.select("src","dst","weight").show()
results = g.triangleCount()#triangelCount
results.select("id","count")
sc.setCheckpointDir("_checkpoint")
results = g.connectedComponents()#connectedComponents
results.show()

