from pyspark import SparkContext
import os

SPARK_PATH = os.environ['SPARK_PATH']

logFile = SPARK_PATH + "/logfile.txt"
sc = SparkContext("local", "Processor")
logData = sc.textFile(logFile).cache()

count = 0
files = "/home/jadixon/Documents/tweets/" + (",/home/jadixon/Documents/tweets/".join(os.listdir("/home/jadixon/Documents/tweets")))

textFile = sc.textFile(files)
print('There are %s tweets' % textFile.count())

sc.stop()
