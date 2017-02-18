# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark import SparkConf
import os
import libtp
import sys
sys.path.insert(0, './PyORT')
import ort

#Do various things for spark
SPARK_PATH = os.environ['SPARK_PATH']
logFile = SPARK_PATH + "/logfile.txt"
sconf = SparkConf().set("spark.executor.memory", "7g").setAppName("processor").setMaster("local")
sc = SparkContext(conf=sconf)
#logData = sc.textFile(logFile).cache()

#Define what files we are going to use
#In the case of this code, we are targetting our tweet directory, and joining the files together
#Then we pass into spark for the map-reduce

#files = "/home/jadixon/Documents/tweets/" + (",/home/jadixon/Documents/tweets/".join(os.listdir("/home/jadixon/Documents/tweets")))
#textFile = sc.textFile(files)

textFile = sc.textFile("/home/jadixon/Documents/Senior-Design/tweets/example.txt")

#Map Reduce to count hashtags
hashtagMessages = textFile.filter(libtp.hasHashtag)\
                .flatMap(libtp.parseHashtags)\
                .map( lambda word: (word[0], [ word[1] ] ) )\
                .reduceByKey(lambda a, b: a+b)
                #.map( lambda word: (word[0], [ word[1] ] ) )\


test = sc.parallelize(hashtagMessages.collect()).map(lambda x: (x[0], x[1]))
test.saveAsTextFile('/home/jadixon/Documents/Senior-Design/seq')
print(sc.textFile('/home/jadixon/Documents/Senior-Design/seq').collect())
sc.stop()
