# -*- coding: utf-8 -*-
from __future__ import print_function

import sys
import json
import re
import shutil
import os
from ast import literal_eval as make_tuple


sys.path.insert(0, '/home/cs179g/Tweet-Processor')
import libtp


from pyspark.sql import SQLContext
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark_cassandra import CassandraSparkContext


with open('/home/cs179g/Tweet-Processor/filteredTweets.txt') as d:
    contents = d.readlines()
contents = dict([ tuple(x.strip().split(',')) for x in contents ])

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


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Poor mans map reduce stuff~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def removeTargets(target):
    isolatedHashtags = parseHashtagsOnly(target)
    for hashtag in isolatedHashtags:
        if contents.has_key(hashtag):
            return False
    return True


def removeAgain(tweet, relevants):
    isolatedHashtags = parseHashtagsOnly(tweet)
    for hashtag in isolatedHashtags:
        if relevants.has_key(hashtag):
            return True
    return False

def parseHashtagsOnly(tweet):
    beg = tweet.find('"hashTags"') + 13
    end = tweet.find('"full_name"') - 3
    hashtags = tweet[beg:end].replace('"', '').split(',')
    return [hashtag.lower() for hashtag in hashtags]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~USED IN FIRST PASS MAP REDUCE~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Detects the presence of a hashtag for the spark filter
#@Param tweet object
#@Return true if there is a hashtag
def hasHashtag(tweet):
    if tweet.find('"hashTags":[]') == -1:
        return True
    elif tweet.find('"hashTags":[]') != -1:
        return False

#Parses the tweet, returning a sequence of tweets
#@Param tweet object
#@Return sequence of pairs (hashtag, tweet_message)
def parseHashtags(tweet):
    beg = tweet.find('"hashTags"') + 13
    end = tweet.find('"full_name"') - 3
    hashtags = tweet[beg:end].replace('"', '').split(',')
    beg = tweet.find('"message"') + 10
    end = tweet.find('"timestamp"') - 1
    message = tweet[beg:end]
    message = message.replace('\n', ' ')
    beg = tweet.find('"full_name"') + 13
    end = tweet.find('"possibly_sensitive"') - 2
    location = tweet[beg:end].split(',')
    if len(location) == 1 or len(location) == 0:
        location = 'Not enough information'
    elif len(location[1].strip()) == 2:
        if location[1].strip() in states[1]:
            location = states[1][location[1].strip()] + ' (' + location[1].strip() + ')'
        else:
            location = 'Non-USA'
    else:
        if location[0].strip() in states[0]:
            location = location[0] + ' (' + states[0][location[0].strip()] + ')'
        else:
            location = 'Non-USA'
    return list(map(lambda hashtag: (hashtag.lower() + ',' + location, message), hashtags))



def loadDictionary(path):
    contents = ''
    with open(path) as d:
        contents = d.readlines()
    invcontent = [tuple((content.split(',')[1].strip(), content.split(',')[0].strip())) for content in contents]
    states = dict([ tuple(x.strip().split(',')) for x in contents ])
    statesinv = dict(invcontent)
    return (states, statesinv)


states = loadDictionary('/home/cs179g/Tweet-Processor/states.txt')
#Load the states into a bidirectional dictionary for ease of access and data formatting
#states = loadDictionary('/home/cs179g/Tweet-Processor/states.txt')



######################################################MAIN##########################################################



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

	files = "/home/cs179g/tweets/" + (",/home/cs179g/tweets/".join(os.listdir("/home/cs179g/tweets")))
	textFile = sc.textFile(files)

	orderedTweets = textFile.filter(hasHashtag)\
	                .filter(removeTargets)\
	                .flatMap(parseHashtags)\
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

	relevantTweets = textFile.filter(hasHashtag)\
	               .filter(removeTargets)\
	               .filter(lambda x: removeAgain(x, contents))\
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

	sc.parallelize(hashtagData).saveToCassandra(keyspace='database_t', table='test')
