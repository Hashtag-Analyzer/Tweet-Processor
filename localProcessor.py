# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark import SparkConf
import os
import sys
import libtp
import shutil
sys.path.insert(0, './Tweet-Sentiment')
import sentiment
import re

#Do various things for spark
SPARK_PATH = os.environ['SPARK_PATH']
logFile = "./logfile.txt"
sconf = SparkConf().set("spark.executor.memory", "7g").setAppName("processor").setMaster("local")
sc = SparkContext(conf=sconf)


#Define what files we are going to use
#In the case of this code, we are targetting our tweet directory, and joining the files together
#Then we pass into spark for the map-reduce
#files = "/home/jadixon/Documents/Senior-Design/tweets/" + (",/home/jadixon/Documents/Senior-Design/tweets/".join(os.listdir("/home/jadixon/Documents/Senior-Design/tweets")))
#textFile = sc.textFile(files)

#print ('Removing old sequence files...')
#shutil.rmtree('/home/jadixon/Documents/Senior-Design/seq')



relevants = sc.sequenceFile('/home/jadixon/Documents/Senior-Design/Archive/relevantHashtags').collect()
contents = dict([ tuple(x) for x in relevants ])

#relevantTweets = textFile.filter(libtp.hasHashtag)\
#               .filter(libtp.removeTargets)\
#               .filter(lambda x: libtp.removeAgain(x, contents))\
#               .map(lambda x: (x, 0))\
#               .saveAsSequenceFile('/home/jadixon/Documents/Senior-Design/relevantTweets')

locationWithHashTags = sc.sequenceFile('/home/jadixon/Documents/Senior-Design/relevantTweets')\
                        .flatMap(lambda x: libtp.parseHashtags(x[0]))\
                        .filter( lambda x: contents.has_key(x[0].split(',')[0]) )\
                        .map( lambda x: (x[0], x[1] + '\n') )\
                        .reduceByKey(lambda a, b: a+b)\
                        .saveAsSequenceFile('/home/jadixon/Documents/Senior-Design/seq')

#relevantHashtags --> hashtags
#relevantTweets --> tweets
#seq --> tweetsSubset


#414
#821
#928
#1276
#1303
#1507



#Used without spark
#documents = sc.sequenceFile('/home/jadixon/Documents/Senior-Design/seq').collect() #1968 location_hashtags
#i = 0
#print documents[1508][0]
#while i < 1968:
#    test = (documents[i][0],  sentiment.getSentiment(documents[i][1]), libtp.ort.rankDocument(documents[i][1]), sentiment.getEmotion(documents[i][1]))
#    with open("sentimentEmotion2.txt", "a") as myfile:
#        myfile.write(str(test) + '\n')
#    i = i + 1


sc.stop()
