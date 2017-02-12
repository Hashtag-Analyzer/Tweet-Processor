from pyspark import SparkContext
import os
import libtp

SPARK_PATH = os.environ['SPARK_PATH']

logFile = SPARK_PATH + "/logfile.txt"
sc = SparkContext("local", "Processor")
logData = sc.textFile(logFile).cache()

count = 0
files = "/home/jadixon/Documents/tweets/" + (",/home/jadixon/Documents/tweets/".join(os.listdir("/home/jadixon/Documents/tweets")))

textFile = sc.textFile(files)
print('There are %s tweets with a hashtag' % textFile.filter(libtp.hasHashtag).count())

sc.stop()
