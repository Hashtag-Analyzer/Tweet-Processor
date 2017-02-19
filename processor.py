# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark import SparkConf
import os
import libtp
import shutil

#Do various things for spark
SPARK_PATH = os.environ['SPARK_PATH']
logFile = SPARK_PATH + "/logfile.txt"
sconf = SparkConf().set("spark.executor.memory", "7g").setAppName("processor").setMaster("local")
sc = SparkContext(conf=sconf)
#logData = sc.textFile(logFile).cache()

def println(item):
    print(item)
    print('\n')

#Define what files we are going to use
#In the case of this code, we are targetting our tweet directory, and joining the files together
#Then we pass into spark for the map-reduce

files = "/home/jadixon/Documents/Senior-Design/tweets/" + (",/home/jadixon/Documents/Senior-Design/tweets/".join(os.listdir("/home/jadixon/Documents/Senior-Design/tweets")))
textFile = sc.textFile('/home/jadixon/Documents/Senior-Design/tweets/file456.txt')

#textFile = sc.textFile("/home/jadixon/Documents/Senior-Design/tweets/example.txt")
print ('Removing old sequence files...')
shutil.rmtree('/home/jadixon/Documents/Senior-Design/seq')
shutil.rmtree('/home/jadixon/Documents/Senior-Design/obscLoc')

#Map Reduce to count hashtags
hashtagMessages = textFile.filter(libtp.hasHashtag)\
                .flatMap(libtp.parseHashtags)\
                .map( lambda x: (x[0], str(libtp.ort.rankText(x[1], False)) + ',' ) )\
                .reduceByKey(lambda a, b: a+b)\
                .saveAsSequenceFile('/home/jadixon/Documents/Senior-Design/seq')

analysisByLocations = sc.sequenceFile('/home/jadixon/Documents/Senior-Design/seq')\
                .flatMap(libtp.unrollList)\
                .map(lambda x: (x[0], str(((sum(x[1])/len(x[1]), len(x[1])), (sum(x[2])/len(x[2]) if x[2] else 0.0, len(x[2])) ))))\
                .reduceByKey(libtp.combineStrAvg)\
                .saveAsSequenceFile('/home/jadixon/Documents/Senior-Design/obscLoc')

analysisAsAWhole = sc.sequenceFile('/home/jadixon/Documents/Senior-Design/obscLoc')\
                .flatMap(libtp.unrollAvg)\
                .map(libtp.revertAvg)\
                .reduceByKey(libtp.combineStrAvg)

print analysisAsAWhole.count()
sc.stop()
