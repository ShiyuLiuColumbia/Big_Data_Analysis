
# coding: utf-8

# In[ ]:


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
spark = SparkSession    .builder    .appName("clusterExample")    .getOrCreate()


# In[4]:


df1 = sqlContext.read.json('AA/wiki_**')
df2 = sqlContext.read.json('BB/wiki_**')
df3 = sqlContext.read.json('CC/wiki_**')


# In[5]:


import pandas as pd


# In[6]:


from pyspark.sql.functions import lit


# In[7]:


df1 = df1.withColumn("label", lit(1))
df2 = df2.withColumn("label", lit(2))
df3 = df3.withColumn("label", lit(3))


# In[8]:


df = df1.union(df2)
df = df.union(df3)


# In[9]:


regexTokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="[^A-Za-z]+", toLowercase=True)
tokenized_data = regexTokenizer.transform(df)


# In[10]:


stopWordsRemover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
filtered_data = stopWordsRemover.transform(tokenized_data)


# In[11]:


hashingTF = HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=20)
featurizedData = hashingTF.transform(filtered_data)


# In[12]:


idf= IDF(inputCol="raw_features", outputCol="features")
idfModel = idf.fit(featurizedData)
featurized_data = idfModel.transform(featurizedData)


# In[15]:


featurized_data.show()


# In[16]:


from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Loads data.

# Trains a k-means model.
kmeans = KMeans().setK(3).setSeed(1)
model = kmeans.fit(featurized_data)


# In[17]:


wssse = model.computeCost(featurized_data)
print("Within Set Sum of Squared Errors = " + str(wssse))
# Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)


# In[ ]:


from pyspark.ml.clustering import LDA


# Trains a LDA model.
lda = LDA(k=3, maxIter=10)
model = lda.fit(featurized_data)
ll = model.logLikelihood(featurized_data)
lp = model.logPerplexity(featurized_data)


# In[21]:


print("The lower bound on the log likelihood of the entire corpus: " + str(ll))
print("The upper bound on perplexity: " + str(lp))

# Describe topics.
topics = model.describeTopics(3)
print("The topics described by their top-weighted terms:")
topics.show()

# Shows the result
transformed = model.transform(featurized_data)
transformed.show()


# In[22]:


from pyspark.ml.clustering import GaussianMixture



gmm = GaussianMixture().setK(3).setSeed(538009335)
model = gmm.fit(featurized_data)

print("Gaussians shown as a DataFrame: ")
model.gaussiansDF.show(truncate=False)

