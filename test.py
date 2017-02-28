# -*- coding: utf-8 -*-
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
        print states[0]
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

parseHashtags('{"favorites":0,"country":"United States","replied_to":"N/A","hashTags":[],"full_name":"Arlington, MA","possibly_sensitive":false,"place_type":"city","name":"katelizleary","location":"N/A","language":"en","message":"New Bachelor drinking game: Take a shot every time Nick cries","timestamp":"Mon Feb 13 18:58:48 PST 2017"}')
parseHashtags('{"favorites":0,"country":"United States","replied_to":"N/A","hashTags":[],"full_name":"Missouri, USA","possibly_sensitive":false,"place_type":"admin","name":"MissKaleighLane","location":"N/A","language":"en","message":"There are few things I want more in this life more than Connie Britton\'s hair 😍","timestamp":"Mon Feb 13 18:58:50 PST 2017"}')
