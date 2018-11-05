# This is the last homework for EECS E6893 Big Data Analysis.
### Q1 Visualizing Clustering 
- Install Spark.
- Download wiki data in https://dumps.wikimedia.org/ and use. https://github.com/attardi/wikiextractor to convert .xml to .csv file.
```sh
$ python q1_preprocess.py #generated .csv in spark folder
$ python3 -m http.server 5000 #run in index.html folder
```
- open browser in http://localhost:5000.
### Q2 Visualizing Streams 
```sh
$ wget -qO- https://deb.nodesource.com/setup_8.x | sudo -E bash -
$ sudo apt-get install -y nodejs
$ npm init
$ npm install twit socket.io express --save
$ node app.js 
```
- open browser in http://localhost:8000.

### Q3 Visualizing Graphs 
```sh
$ cd spark
$ ./bin/pyspark --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11

```
- run the code of q3_preprocess.py in the spark graph shell.
- change the files of node2.csv to be 'ID','PageRank', 'Component'.
- change the files of edge2.csv to be 'source', 'target'
```sh
$ python3 -m http.server 5000 #run in index.html folder
```
- open browser in http://localhost:5000.