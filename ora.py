#A library used to detect obscenity, though in a very minimal and primative manner
import difflib

#Loads the obscenity dictionary from the directory
def loadDictionary():
    content = ''

    with open('dictionary.txt') as d:
        content = d.readlines()
    content = [ x.strip() for x in content ]

    return list(map(lambda word: (word.split(',')[0], float(word.split(',')[1])), content))

dictionary = loadDictionary()

#Performs the obscenity ranking
#@Param: text to be ranked in obscenity
#@Return: the ranking in obscenity of the text
def rankText(text):
    tokens = text.split(' ')
    matches = list(map( lambda token: (token, closeMatches(token)), tokens))
    refinedMatches = [''.join(confirmObscene(term)) for term in matches if confirmObscene(term)]
    print(refinedMatches)

#Gets the close matches of a word
#@Param: word to be analyzed
#@Return: a sequence of obscene words that re best matches
def closeMatches(word):
    return difflib.get_close_matches(word, list(map(lambda term: term[0], dictionary)))

#Confirsm thge presence of an obscene word and returns tge true word
#@Param: a word and a sequence of closest matching words as a tuple
#@Return: the proper word if an obscenity is detected
def confirmObscene(entity):
    word = entity[0]
    return [term for term in entity[1] if round(difflib.SequenceMatcher(None, word.lower(), term).ratio(), 5) > .90000]

def wordRank(word):
    #FIXME Write code to return the obscene ranking
rankText("You little fuck face fagggot")
