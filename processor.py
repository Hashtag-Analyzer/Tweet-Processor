from pyspark import SparkContext
import os
import libtp

#Do various things for spark
SPARK_PATH = os.environ['SPARK_PATH']
logFile = SPARK_PATH + "/logfile.txt"
sc = SparkContext("local", "processor")
logData = sc.textFile(logFile).cache()

#Define what files we are going to use
#In the case of this code, we are targetting our tweet directory, and joining the files together
#Then we pass into spark for the map-reduce
files = "/home/jadixon/Documents/tweets/" + (",/home/jadixon/Documents/tweets/".join(os.listdir("/home/jadixon/Documents/tweets")))
#textFile = sc.textFile(files)
textFile = sc.textFile("/home/jadixon/Documents/tweets/example.txt")

#Map Reduce to count hashtags
hashtagCounts = textFile.filter(libtp.hasHashtag)\
                .flatMap(libtp.parseHashtags)\
                .map( lambda word: (word[0], [ word[1] ]) )\
                .reduceByKey(lambda a, b: a+b)

#passOne = sc.sequenceFile(hashtagCounts.collect())
#Print our data
print(hashtagCounts.collect())

sc.stop()
