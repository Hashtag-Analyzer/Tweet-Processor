# -*- coding: utf-8 -*-
from ast import literal_eval as make_tuple



with open('/home/cs179g/Tweet-Processor/filteredTweets.txt') as d:
    contents = d.readlines()
contents = dict([ tuple(x.strip().split(',')) for x in contents ])

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
            location = location[0] + ' (' + states[location[0]] + ')'
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
