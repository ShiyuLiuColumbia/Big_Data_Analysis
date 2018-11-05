#================================#
####run in spark graph shell#####
#================================#
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
    .appName("similairityExample")\
    .getOrCreate()

df = sqlContext.read.json('/home/sl4401/AA/wiki_**')
df = df.limit(1000)
regexTokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="[^A-Za-z]+", toLowercase=True)
tokenized_data = regexTokenizer.transform(df)
stopWordsRemover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
filtered_data = stopWordsRemover.transform(tokenized_data)
hashingTF = HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=20)
featurizedData = hashingTF.transform(filtered_data)
idf= IDF(inputCol="raw_features", outputCol="features")
idfModel = idf.fit(featurizedData)
featurized_data = idfModel.transform(featurizedData)
from pyspark.ml.feature import Normalizer
normalizer = Normalizer(inputCol="features", outputCol="norm")
data = normalizer.transform(featurized_data)
import math
import pyspark.sql.functions as psf
from pyspark.sql.types import DoubleType
dot_udf = psf.udf(lambda x,y: float(x.dot(y)), DoubleType())
s=data.alias("i").join(data.alias("j"), psf.col("i.id") < psf.col("j.id"))\
      .select(
           psf.col("i.id").alias("src"), 
           psf.col("j.id").alias("dst"), 
           dot_udf("i.norm", "j.norm").alias("relationship"))\
      .sort("src", "dst")


v = featurized_data.select("id","features")
e = s.filter("relationship > 0.95")
from graphframes import *
g= GraphFrame(v,e)
pg = g.pageRank(resetProbability=0.15, maxIter=10)#pagerank
cc = g.connectedComponents()#connectedComponents


#==============================
#deal with edges
g.edges.toPandas().to_csv("edge.csv", header=True, index=False)
f = pandas.read_csv("edge.csv")
keep=['src','dst']  #Fields we want to save
new = f[keep]
new.to_csv("edge2.csv", header=True, index=False)

new_df = pg.vertices.join(cc, on=['id'], how='inner') #join pagerank and connected components on 'id'
new_df.toPandas().to_csv("node.csv", header=True, index=False)
f = pandas.read_csv("node.csv")
keep = ["id","pagerank","component"] #Fields we want to save
new = f[keep]
new.to_csv("node2.csv", header=True, index=False)
