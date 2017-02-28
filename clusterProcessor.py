from __future__ import print_function

import sys
import json
import re
import shutil
import os

from pyspark.sql import SQLContext
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark_cassandra import CassandraSparkContext

def normalize(datum):
	return re.sub('[^A-Za-z .0-9]+', '', datum)

def ParseData(datum):
	data = datum.split(", \"")
	more = data[1].split("\", ")
	del data[1]
	data = data + more
	hashtag = str(normalize(data[0].split(',')[0])[1:])
	location = str(normalize(data[0].split(',')[1]))
	obscenityOverall = str(normalize(data[3].split(', ')[0]))
	obscenityPure = str(normalize(data[3].split(', ')[1]))
	emotion = str(data[1][:-2].replace("u", ""))
	sentiment = str(data[2].replace("u", ""))
	return [(hashtag, location, obscenityOverall, obscenityPure, emotion, sentiment)]

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print ("ERROR NOT ENOUGH ARGS")
		exit(-1)

	conf = SparkConf().setAppName("Processor")
	sc = CassandraSparkContext(conf=conf)
	sqlContext = SQLContext(sc)

	#shutil.rmtree('~/orderedTweets')
	#shutil.rmtree('~/relevantHashtags')
	#shutil.rmtree('~/relevantTweets')

	files = "./tweets/" + ("./tweets/".join(os.listdir("./tweets")))
	textFile = sc.textFile(files)

	orderedTweets = textFile.filter(libtp.hasHashtag)\
	                .filter(libtp.removeTargets)\
	                .flatMap(libtp.parseHashtags)\
	                .map( lambda x: (x[0].split(',')[0], 1))\
	                .reduceByKey(lambda a, b: a+b)\
	                .sortBy(lambda x: x[1], ascending=False)\
	                .saveAsSequenceFile('orderedTweets')

	smallest = sc.sequenceFile('orderedTweets').takeOrdered(40, key = lambda x: -x[1])[39][1]

	relevantHashtags = sc.sequenceFile('orderedTweets')\
	                .filter(lambda x: x[1] >= smallest)\
	                .map(lambda x: (x[0], x[1]))\
	                .sortBy(lambda x: x[1], ascending=False)\
	                .saveAsSequenceFile('relevantHashtags')

	relevants = sc.sequenceFile('relevantHashtags').collect()
	contents = dict([ tuple(x) for x in relevants ])

	relevantTweets = textFile.filter(libtp.hasHashtag)\
	               .filter(libtp.removeTargets)\
	               .filter(lambda x: libtp.removeAgain(x, contents))\
	               .map(lambda x: (x, 0))\
	               .saveAsSequenceFile('relevantTweets')

    hashtagData = sc.textFile(sys.argv[1])\
		.flatMap(ParseData)\
		.map(lambda row: {'hashtag': row[0],
				  'location': row[1],
				  'obscenityoverall': row[2],
				  'obscenitypure': row[3],
				  'emotion': row[4],
				  'sentiment': row[5]}).collect()

#	sc.parallelize(hashtagData).saveToCassandra(keyspace='database_t', table='test')
