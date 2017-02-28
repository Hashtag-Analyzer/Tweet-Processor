# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark import SparkConf
import os
import sys
import libtp
import shutil
sys.path.insert(0, './Tweet-Sentiment')
import sentiment

#Do various things for spark
SPARK_PATH = os.environ['SPARK_PATH']
logFile = SPARK_PATH + "/logfile.txt"
sconf = SparkConf().set("spark.executor.memory", "7g").setAppName("processor").setMaster("local")
sc = SparkContext(conf=sconf)


#Define what files we are going to use
#In the case of this code, we are targetting our tweet directory, and joining the files together
#Then we pass into spark for the map-reduce
#files = "/home/jadixon/Documents/Senior-Design/tweets/" + (",/home/jadixon/Documents/Senior-Design/tweets/".join(os.listdir("/home/jadixon/Documents/Senior-Design/tweets")))
#textFile = sc.textFile(files)

print ('Removing old sequence files...')
#shutil.rmtree('/home/jadixon/Documents/Senior-Design/seq')

#relevants = sc.sequenceFile('/home/jadixon/Documents/Senior-Design/Archive/relevantHashtags').collect()
#contents = dict([ tuple(x) for x in relevants ])


#locationWithHashTags = sc.sequenceFile('/home/jadixon/Documents/Senior-Design/relevantTweets')\
#                        .flatMap(lambda x: libtp.parseHashtags(x[0]))\
#                        .filter( lambda x: contents.has_key(x[0].split(',')[0]) )\
#                        .map( lambda x: (x[0], x[1] + '\n') )\
#                        .reduceByKey(lambda a, b: a+b)\
#                        .saveAsSequenceFile('/home/jadixon/Documents/Senior-Design/seq')


documents = sc.sequenceFile('/home/jadixon/Documents/Senior-Design/seq').collect() #1968 location_hashtags
#i = 0

print sentiment.getSentiment(documents[0][1])
print sentiment.getEmotion(documents[0][1])
#with open("sentimentEmotion.txt", "w") as myfile:
#    myfile.write('')
#while i < 990:
#    test = (documents[i][0], libtp.ort.rankDocument(documents[i][1]), 2, 3, 34)
#    with open("sentimentEmotion.txt", "a") as myfile:
#        myfile.write(str(test) + '\n')
#    i = i + 1


sc.stop()
