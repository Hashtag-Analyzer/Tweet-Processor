#Detects the presence of a hashtag for the spark filter
def hasHashtag(tweet):
    if tweet.find('"hashTags":[]') == -1:
        return False
    elif tweet.find('"hashTags":[]') != -1:
        return True

#Parses the tweet, returning a sequence of tweets
def parseHashtags(tweet):
    
