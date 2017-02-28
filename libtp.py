# -*- coding: utf-8 -*-
import sys
sys.path.insert(0, './PyORT')
import ort
from bidict import bidict
from ast import literal_eval as make_tuple



with open('/home/jadixon/Documents/Senior-Design/Tweet-Processor/filteredTweets.txt') as d:
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
        if location[1].strip() in states.inv:
            location = states.inv[location[1].strip()] + ' (' + location[1].strip() + ')'
        else:
            location = 'Non-USA'
    else:
        if location[0].strip() in states:
            location = location[0] + ' (' + states[location[0]] + ')'
        else:
            location = 'Non-USA'
    return list(map(lambda hashtag: (hashtag.lower() + ',' + location, message), hashtags))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~USED IN SECOND PASS MAP REDUCE~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Unrolls a hashtags str list of values into a real list so the parsing can be done easier
#@Param: a piece of data, containing a hashtag and a str of values formatted as a list
#@Return: list -> [(hashtagname, obscenity values with 0, obscenity values without 0)]
def unrollList(data):
    globalObscenity = [float(value) for value in data[1].split(',')[:-1]]
    pureObscenity = [float(value) for value in data[1].split(',')[:-1] if value != '0.0']
    return [(data[0] + ',collective', globalObscenity, pureObscenity)]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~USED IN THIRD PASS MAP REDUCE~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def unrollAvg(data):
    dTup = make_tuple(data[1])
    hashtag = data[0].split(',')[0]
    return [(hashtag, dTup)]

def revertAvg(data):
    x = data[1]
    lTup = (x[0][0] * x[0][1], x[0][1])
    rTup = (x[1][0] * x[1][1], x[1][1])
    return (data[0], str((lTup, rTup)))


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~USED COLLECTIVELY~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Takes in tuples formatted as a string and adds the respective elements together
def combineStrAvg(a, b):
    aTup = make_tuple(a)
    bTup = make_tuple(b)
    lTup = (aTup[0][0] + bTup[0][0] , aTup[0][1] + bTup[0][1])
    rTup = (aTup[1][0] + bTup[1][0], aTup[1][1] + bTup[1][1])
    return str((lTup,rTup))




def loadDictionary(path):
    content = ''
    with open(path) as d:
        content = d.readlines()
    return dict([ tuple(x.strip().split(',')) for x in content ])
#Load the states into a bidirectional dictionary for ease of access and data formatting
states = loadDictionary('/states.txt')
states = bidict(states)
