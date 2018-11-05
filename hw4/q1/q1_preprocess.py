import findspark
findspark.init()
from pyspark import SQLContext
from pyspark import SparkContext
sc = SparkContext()
sqlContext = SQLContext(sc)
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import HashingTF, IDF
from pyspark.sql import SparkSession
# Build a SparkSession; SparkSession provides a single point of entry to interact with underlying Spark functionality
spark = SparkSession\
    .builder\
    .appName("clusterExample")\
    .getOrCreate()

#data can not be huge, otherwise the html can not load
#wiki_** can be downloaded from wikipedia and use xml2json to process is.
df1 = sqlContext.read.json('/home/sl4401/AA/wiki_**')
df1 = df1.limit(500)
df2 = sqlContext.read.json('/home/sl4401/BB/wiki_**')
df2 = df2.limit(500)
df3 = sqlContext.read.json('/home/sl4401/CC/wiki_**')
df3 = df3.limit(500)
import pandas as pd
from pyspark.sql.functions import lit
df1 = df1.withColumn("label", lit(1))
df2 = df2.withColumn("label", lit(2))
df3 = df3.withColumn("label", lit(3))
df = df1.union(df2)
df = df.union(df3)

#=================================
#do the tf-idf precess(the same as spark tutorial)
regexTokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="[^A-Za-z]+", toLowercase=True)
tokenized_data = regexTokenizer.transform(df)
stopWordsRemover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
filtered_data = stopWordsRemover.transform(tokenized_data)
hashingTF = HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=20)
featurizedData = hashingTF.transform(filtered_data)
idf= IDF(inputCol="raw_features", outputCol="features")
idfModel = idf.fit(featurizedData)
featurized_data = idfModel.transform(featurizedData)
#=================================



#=================================
#1. save id, features, label in cluster_ground_truth.csv file. The header is ('ID', '_2', '_3'....., '_21', 'label').

def extract(row):
    return (row.id, ) + tuple(row.features.toArray().tolist())+(row.label,)
df_new=featurized_data.rdd.map(extract).toDF(["ID"])
df_new.toPandas().to_csv("cluster_ground_truth.csv", header=True, index=False)
#=================================



#=================================
#2. save id, features, label in cluster_prediction.csv file. The header is ('ID', '_2', '_3'....., '_21', 'label').
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Trains a k-means model.
kmeans = KMeans().setK(3).setSeed(1)
model = kmeans.fit(featurized_data)
predictions = model.transform(featurized_data)

def extract(row):
    return (row.id, ) + tuple(row.features.toArray().tolist())+(row.prediction,)

df_new=predictions.rdd.map(extract).toDF(["ID"])  # Vector values will be named _2, _3, ...
df_new.toPandas().to_csv("cluster_prediction.csv", header=True, index=False)
#===================================


#=================================
#3. save id, features, label in cluster_random.csv file. The header is ('ID', '_2', '_3'....., '_21', 'label').
import numpy as np
mean1=100
mean2=200
mean3=300
featureNum=20
dataNum=120
data1=np.random.multivariate_normal([mean1]*featureNum,np.identity(featureNum),dataNum)
data2=np.random.multivariate_normal([mean2]*featureNum,np.identity(featureNum),dataNum)
data3=np.random.multivariate_normal([mean3]*featureNum,np.identity(featureNum),dataNum)
list1=data1.tolist()
list2=data2.tolist()
list3=data3.tolist()
for l in list2:
    list1.append(l)
for l in list3:
    list1.append(l) 

df1 = sc.parallelize(list1).toDF(['feature1','feature2','feature3','feature4','feature5','feature6','feature7','feature8','feature9','feature10','feature11','feature12','feature13','feature14','feature15','feature16','feature17','feature18','feature19','feature20'])
import pandas as pd
#cluster_random.csv do not have id and label!
df1.toPandas().to_csv("cluster_random.csv", header=True, index=False)
