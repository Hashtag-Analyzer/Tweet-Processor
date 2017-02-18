# -*- coding: utf-8 -*-
import sys
sys.path.insert(0, './PyORT')
import ort
from bidict import bidict

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
    beg = tweet.find('"hashTags"') + 12
    end = tweet.find('"full_name"') - 2
    hashtags = tweet[beg:end].split(',')
    beg = tweet.find('"message"') + 10
    end = tweet.find('"timestamp"') - 1
    message = tweet[beg:end]
    beg = tweet.find('"full_name"') + 13
    end = tweet.find('"possibly_sensitive"') - 2
    location = tweet[beg:end].split(',')
    if len(location[1].strip()) == 2:
        location = states.inv[location[1].strip()] + '(' + location[1].strip() + ')'
    else:
        location = states[location[0]] + '(' + location[0] + ')'
    return list(map(lambda hashtag: ((hashtag, location), message), hashtags))




#Load the states into a bidirectional dictionary for ease of access and data formatting
states = ort.loadDictionary('/home/jadixon/Documents/Senior-Design/Tweet-Processor/states.txt')
states = bidict(states)
