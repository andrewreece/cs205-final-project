
from __future__ import division
import numpy as np
import pyspark
from operator import add
import collections
import time
import os
import json

sc = pyspark.SparkContext(appName = "Spark1")

def make_json(tweet):
    ''' Get stringified JSOn from Kafka, attempt to convert to JSON '''
    try:
        return json.loads(tweet.decode('utf-8'))
    except:
        return "error"+str(tweet.decode('utf-8'))

def filter_tweets(item,pattern):
    ''' Filters out the tweets we do not want.  Filters include:
            * No retweets 
            * No geolocation or location field (do we really care about this?)
            * English language only
            * No tweets with links
                - We need to check both entities and media fields for this (is that true?)
    '''
    return (('delete' not in item.keys()) and
            ('retweeted_status' not in item.keys())                           and 
            (item['lang']=='en')                       and
            (len(item['entities']['urls'])==0)                   and
            ('media' not in item['entities'].keys()) and
            (re.search(pattern,item['text'],re.I) is not None)
           )

def get_relevant_fields(item):
    ''' Reduce the full set of metadata down to only those we care about, including:
            * timestamp
            * username
            * text of tweet 
            * hashtags
            * geotag coordinates (if any)
            * location (user-defined in profile, not necessarily current location)
    '''
    return (item['id'], 
            {"timestamp":  item['created_at'],
             "username":    item['user']['screen_name'],
             "text":        item['text'],
             "hashtags":    [el['text'] for el in item['entities']['hashtags']],
             "geotag":    item['geo'],
             "user_loc":    item['user']['location']
            }
           )

def update_tz(d,dtype):
    
    def convert_timezone(item):
        from_zone = tz.gettz('UTC')
        to_zone = tz.gettz('America/New_York')
        dt = parser.parse(item['timestamp'])
        utc = dt.replace(tzinfo=from_zone)
        return utc.astimezone(to_zone)
    
    if dtype == "sql":
        return Row(id=d[0], time=convert_timezone(d[1]))
    elif dtype == "pandas":
        return convert_timezone(d[1])


def getTweet(string):
    substring = '"tweet"'
    matchs = []
    for ii in xrange(len(string)-len(substring)):
        if string[ii:ii+len(substring)] == substring:
            matchs.append(ii)
    assert len(matchs) > 0, "no tweet"
    start = matchs[0] + 8
    while string[start:start+10] != '"hashtags"':
        start = start + 1
    return string[matchs[0]+9:start-2]

def getLogLik(allWords,nTopics,wordTopicCounters,topicCounters,beta):
    uniqueWords = np.unique(allWords)
    nVocab = len(uniqueWords)
    logLik = nTopics*(logGamma2(nVocab*beta)-nVocab*logGamma2(beta))
    for ii in xrange(0,nTopics):
        logLik = logLik - logGamma2(topicCounters[ii]+nVocab*beta)
        assert topicCounters[ii] >= 0, "topic counter error"
        for jj in uniqueWords:
            logLik = logLik + logGamma2(wordTopicCounters[jj][ii]+beta)
            assert wordTopicCounters[jj][ii] >= 0, "word counter error"
    return logLik

def logGamma2(num):
    return np.log(np.sqrt(2*np.pi*num)) + num*np.log(num/np.exp(1))

def findWord(wordArray,target):
    index = []
    for ii in xrange(0,len(wordArray)):
        if wordArray[ii] == target:
            index.append(ii)
    return index

def findWord2(words,target):
    index = []
    for ii in xrange(0,len(words)-len(target)):
        if words[ii:ii+len(target)] == target:
            index.append(ii)
    return index
"""
def preProcessWords(texts):
   
    wordDocDict = {}
    wordDocDictNU = {}
    nDocs = len(texts)
    
    for ii in xrange(0,nDocs):
        textString = texts[ii].lower().split()
        for jj in xrange(0,len(textString)):
            if jj == 0:
                tempText = textString[jj][1:]
            elif jj == len(textString)-1:
                tempText = textString[jj][:-1]
            else:
                tempText = textString[jj]
            if tempText in wordDocDict:
                wordDocDict[tempText].add(ii)
                wordDocDictNU[tempText].append(ii)
            else:
                wordDocDict[tempText] = set([ii])
                wordDocDictNU[tempText] = [ii]
    uniqueWords = np.array(wordDocDict.keys())
    uniqueWordDocLabels = np.array(wordDocDict.values())
    nuWordDocLabels = np.array(wordDocDictNU.values())
    nDocPerWord = np.array(map(lambda x: len(x),uniqueWordDocLabels))
   
    # only keep words that appear in at least 5 tweets
    uniqueWords = uniqueWords[nDocPerWord > 4]
    nuWordDocLabels = nuWordDocLabels[nDocPerWord > 4]
    nInstances = np.array(map(lambda x: len(x),nuWordDocLabels))
   
    allWords = []
    docLabel = []

    f = open('/home/hadoop/stop_words.txt')
    stopWords = f.read()
    f.close()
    stopWords = stopWords.split()
   
    remW = set(stopWords+[''])
   
    filter1 = np.array([True]*len(uniqueWords))
    for ii in xrange(0,len(uniqueWords)):
        if uniqueWords[ii] in remW:
            filter1[ii] = False
   
    uniqueWords = uniqueWords[filter1]
    nuWordDocLabels = nuWordDocLabels[filter1]
    nInstances = nInstances[filter1]
   
    for ii in xrange(0,len(uniqueWords)):
        allWords = allWords + [uniqueWords[ii]]*len(nuWordDocLabels[ii])
        docLabel = docLabel + nuWordDocLabels[ii]
       
    return (np.array(allWords), np.array(docLabel))
"""

def preProcessWords(texts):
   
    #texts = texts[0]
    wordDocDict = {}
    wordDocDictNU = {}
    nDocs = len(texts)
    
    for ii in xrange(0,nDocs):
        textString = texts[ii].lower().split()
        for jj in xrange(0,len(textString)):
            if jj == 0:
                tempText = textString[jj][1:]
            elif jj == len(textString)-1:
                tempText = textString[jj][:-1]
            else:
                tempText = textString[jj]
            if tempText in wordDocDict:
                wordDocDict[tempText].add(ii)
                wordDocDictNU[tempText].append(ii)
            else:
                wordDocDict[tempText] = set([ii])
                wordDocDictNU[tempText] = [ii]
    uniqueWords = np.array(wordDocDict.keys())
    uniqueWordDocLabels = np.array(wordDocDict.values())
    nuWordDocLabels = np.array(wordDocDictNU.values())
    nDocPerWord = np.array(map(lambda x: len(x),uniqueWordDocLabels))
   
    # only keep words that appear in at least 5 tweets
    uniqueWords = uniqueWords[nDocPerWord > 0]
    nuWordDocLabels = nuWordDocLabels[nDocPerWord > 0]
    nInstances = np.array(map(lambda x: len(x),nuWordDocLabels))
   
    allWords = []
    docLabel = []
   
    remW = set([ii.lower() for ii in ['@', 'me', 'my', '-', 'the', 'is', 'it', 'in', 'just',\
     'for', 'was', 'no', 'when', 'not', 'that', 'and', 'take',\
     'get',  'I', 'on', 'of', 'with', 'at', 'you', 'all', 'to',\
     "I'm", 'a', "don't",'The', 'are', 'back', 'be', 'up', 'go',\
     'from', 'about', 'this', 'do', 'out', 'have',\
     'so', 'will', 'like', '&amp;', 'but','']])
   
    filter1 = np.array([True]*len(uniqueWords))
    for ii in xrange(0,len(uniqueWords)):
        if uniqueWords[ii] in remW:
            filter1[ii] = False
   
    uniqueWords = uniqueWords[filter1]
    nuWordDocLabels = nuWordDocLabels[filter1]
    nInstances = nInstances[filter1]
   
    for ii in xrange(0,len(uniqueWords)):
        allWords = allWords + [uniqueWords[ii]]*len(nuWordDocLabels[ii])
        docLabel = docLabel + nuWordDocLabels[ii]
       
    return (np.array(allWords), np.array(docLabel))


def drawTopicInit(allWords,docLabel,nTopics,alpha,beta):
   
    vocab = np.unique(allWords)
    nVocab = len(vocab)

    wordTopicCounters = {}
    docTopicCounters = {}
    topicCounters = {}
    docCounters = {}
    for ii in vocab:
        wordTopicCounters[ii] = {}
        for jj in xrange(nTopics):
            wordTopicCounters[ii][jj] = 0
    for ii in np.unique(docLabel):
        docTopicCounters[ii] = {}
        for jj in xrange(nTopics):
            docTopicCounters[ii][jj] = 0
        docCounters[ii] = 0
    for ii in xrange(nTopics):
        topicCounters[ii] = 0
   
    topicVector = np.zeros(len(allWords))
   
    for ii in xrange(0,len(allWords)):
        currentWord = allWords[ii]
        currentDoc = docLabel[ii]
       
        probVector = np.zeros(nTopics)
        for jj in xrange(0,nTopics):
            probVector[jj] = (wordTopicCounters[currentWord][jj]+beta)/(topicCounters[jj] + nVocab*beta)
            probVector[jj] = probVector[jj]*(docTopicCounters[currentDoc][jj]+alpha)/(docCounters[currentDoc] + nTopics*alpha)
       
        probVector = probVector/sum(probVector)
        probCumSum = np.cumsum(probVector)
        randInt = np.random.uniform(0,1,1)

        currentTopic = sum(probCumSum < randInt)
        topicVector[ii] = currentTopic
       
        wordTopicCounters[currentWord][currentTopic] += 1
        docTopicCounters[currentDoc][currentTopic] += 1
        topicCounters[currentTopic] += 1
        docCounters[currentDoc] += 1 
   
    return topicVector, wordTopicCounters, docTopicCounters, topicCounters, docCounters

def drawTopic(wordIndex,allWords,topicLabel,docLabel,nTopics,wordTopicCounters, docTopicCounters, \
                                                                      topicCounters, docCounters,alpha,beta):
    nVocab = len(wordTopicCounters.keys())
    probVector = np.zeros(nTopics)
   
    currentWord = allWords[wordIndex]
    currentDoc = docLabel[wordIndex]
    currentTopic = topicLabel[wordIndex]
   
    for ii in xrange(0,nTopics):
        if ii == currentTopic:
            probVector[ii] = (wordTopicCounters[currentWord][ii]+beta-1)/(topicCounters[ii] + nVocab*beta - 1)
            probVector[ii] = probVector[ii]*(docTopicCounters[currentDoc][ii]+alpha-1)/(docCounters[currentDoc] + \
                                                                                        nTopics*alpha - 1)
        else:
            probVector[ii] = (wordTopicCounters[currentWord][ii]+beta)/(topicCounters[ii] + nVocab*beta)
            probVector[ii] = probVector[ii]*(docTopicCounters[currentDoc][ii]+alpha)/(docCounters[currentDoc] + \
                                                                                        nTopics*alpha)
                                                                                                                      
    probVector = probVector/sum(probVector)
    probCumSum = np.cumsum(probVector)
    randInt = np.random.uniform(0,1,1)
   
    newTopic = sum(probCumSum < randInt)
   
    if newTopic != currentTopic:
        wordTopicCounters[currentWord][newTopic] += 1
        wordTopicCounters[currentWord][currentTopic] -= 1
        docTopicCounters[currentDoc][newTopic] += 1
        docTopicCounters[currentDoc][currentTopic] -= 1
        topicCounters[newTopic] += 1
        topicCounters[currentTopic] -= 1
   
    return newTopic, wordTopicCounters, docTopicCounters, topicCounters

def verifyCounts(WordDocVec_OrderedSet):
    uniqueWords = WordDocVec_OrderedSet[3]
    topicLabel = WordDocVec_OrderedSet[5]
    allWords = WordDocVec_OrderedSet[10]
    allWordCountsS = WordDocVec_OrderedSet[6]
    
    for ii in uniqueWords:
        for jj in xrange(nTopics):
            if allWordCountsS[ii][jj] < sum((topicLabel == jj) & (allWords == ii)):
                print "sync outside counts: ",allWordCounts[ii][jj]," inside counts: ",allWordCountsS[ii][jj] ," present value: ",sum((topicLabel == jj) & (allWords == ii))
                asdf

def stationaryLDA(WordDocVec_OrderedPair):

    allWords = WordDocVec_OrderedPair[0]
    docLabel = WordDocVec_OrderedPair[1]
    nTopics = 10 
    uniqueWords = np.unique(allWords)
    uniqueDocs = np.unique(docLabel)
    nVocab = len(uniqueWords)
    nDocs = len(uniqueDocs)
   
    theta = np.zeros((nDocs,nTopics))
    phi = np.zeros((nTopics,nVocab))
   
    # just need allWords, docLabel, topicLabel
    alpha = 0.001
    beta = 0.01
   
    nIterations = 1
    count = 0
   
    topicLabel, wordTopicCounters, docTopicCounters, topicCounters, docCounters = \
                                                                    drawTopicInit(allWords,docLabel,nTopics,alpha,beta)
                                                                                              
    logLik = np.zeros(nIterations+1)
    logLik[0] = getLogLik(allWords,nTopics,wordTopicCounters,topicCounters,beta)
   
    while count < nIterations:
        for ii in xrange(0,len(allWords)):
            topic, wordTopicCounters, docTopicCounters, topicCounters = drawTopic(ii,allWords,\
                              topicLabel,docLabel,nTopics,wordTopicCounters, docTopicCounters, \
                                                                                  topicCounters, docCounters,alpha,beta)
            topicLabel[ii] = topic
        # update phi and theta here, calculate P(w|z)
        for ii in xrange(0,nTopics):
            sumCurr = topicCounters[ii]
            for jj in xrange(0,nVocab):
                phi[ii,jj] = (wordTopicCounters[uniqueWords[jj]][ii] + beta)/(sumCurr + nVocab*beta)
        for ii in xrange(0,nDocs):
            sumCurr = docCounters[uniqueDocs[ii]]
            for jj in xrange(0,nTopics):
                theta[ii,jj] = (docTopicCounters[uniqueDocs[ii]][jj] + alpha)/(sumCurr + nTopics*alpha)
        logLik[count+1] = getLogLik(allWords,nTopics,wordTopicCounters,topicCounters,beta)
        #print "first time: ",logLik
        count = count + 1
    """
    for ii in uniqueWords:
        for jj in xrange(nTopics):
            if wordTopicCounters[ii][jj] != sum((topicLabel == jj) & (allWords == ii)):
                print "first time topic counts: ",wordTopicCounters[ii][jj] ," present value: ",sum((topicLabel == jj) & (allWords == ii))
                asdf
    """

    return (theta, phi, logLik, uniqueWords, uniqueDocs, topicLabel,\
                                            wordTopicCounters, topicCounters, docTopicCounters, docCounters,allWords,docLabel)

def stationaryLDA_post(WordDocVec_OrderedSet):

    theta = WordDocVec_OrderedSet[0]
    phi = WordDocVec_OrderedSet[1]
    logLik = WordDocVec_OrderedSet[2]
    uniqueWords = WordDocVec_OrderedSet[3]
    uniqueDocs = WordDocVec_OrderedSet[4]
    topicLabel = WordDocVec_OrderedSet[5]
    wordTopicCounters = WordDocVec_OrderedSet[6]
    topicCounters = WordDocVec_OrderedSet[7]
    docTopicCounters = WordDocVec_OrderedSet[8]
    docCounters = WordDocVec_OrderedSet[9]
    allWords = WordDocVec_OrderedSet[10]
    docLabel = WordDocVec_OrderedSet[11]

    nTopics = 10 

    """
    for ii in uniqueWords:
        for jj in xrange(nTopics):
            if wordTopicCounters[ii][jj] < sum((topicLabel == jj) & (allWords == ii)):
                print "post outside counts: ",allWordCounts[ii][jj]," inside counts: ",wordTopicCounters[ii][jj] ," present value: ",sum((topicLabel == jj) & (allWords == ii))
                asdf
    """
   
    # just need allWords, docLabel, topicLabel
    alpha = 0.001
    beta = 0.01
   
    nIterations = 1
    count = 0
    nVocab = len(uniqueWords)
    nDocs = len(uniqueDocs)
                                                                                         
    logLik = np.zeros(nIterations+1)
    logLik[0] = getLogLik(allWords,nTopics,wordTopicCounters,topicCounters,beta)

    while count < nIterations:
        for ii in xrange(0,len(allWords)):
            if wordTopicCounters[allWords[ii]][topicLabel[ii]] <= 0:
                print "word index: ", ii
                print "word: ", allWords[ii]
                print "topic label: ",topicLabel[ii]
                print "topic count: ", allWordCounts[allWords[ii]][topicLabel[ii]]
                print "value outside: ", allWordCounts[allWords[ii]]#[topicLabel[ii]]
                print "value inside: ", wordTopicCounters[allWords[ii]]#[topicLabel[ii]]
                asdf
            topic, wordTopicCounters, docTopicCounters, topicCounters = drawTopic(ii,allWords,\
                              topicLabel,docLabel,nTopics,wordTopicCounters, docTopicCounters, \
                                                                                  topicCounters, docCounters,alpha,beta)
            topicLabel[ii] = topic
        # update phi and theta here, calculate P(w|z)
        for ii in xrange(0,nTopics):
            sumCurr = topicCounters[ii]
            for jj in xrange(0,nVocab):
                phi[ii,jj] = (wordTopicCounters[uniqueWords[jj]][ii] + beta)/(sumCurr + nVocab*beta)
        for ii in xrange(0,nDocs):
            sumCurr = docCounters[uniqueDocs[ii]]
            for jj in xrange(0,nTopics):
                theta[ii,jj] = (docTopicCounters[uniqueDocs[ii]][jj] + alpha)/(sumCurr + nTopics*alpha)
        logLik[count+1] = getLogLik(allWords,nTopics,wordTopicCounters,topicCounters,beta)
        #print "second time: ",logLik
        count = count + 1

    return (theta, phi, logLik, uniqueWords, uniqueDocs, topicLabel,\
                                            wordTopicCounters, topicCounters, docTopicCounters, docCounters,allWords,docLabel)

def synchronizeCounts(WordDocVec_OrderedSet,allWordCountsL,allTopicCountsL):
    
    uniqueWords = WordDocVec_OrderedSet[3]
    topicLabel = WordDocVec_OrderedSet[5]
    allWords = WordDocVec_OrderedSet[10]
    wordTopicCountCurr = WordDocVec_OrderedSet[6]
    # after extracting the 4 variables above for the current partition, compare the partition's 
    # word-topic counts dictionary, "wordTopicCountCurr", to the accumulated word topic counts "allWordCountsL"
    for ii in uniqueWords:
        for jj in xrange(nTopics):
            if allWordCountsL[ii][jj] < sum((topicLabel == jj) & (allWords == ii)):
                print "sync counts: ",allWordCountsL[ii][jj],"old counts: ",wordTopicCountCurr[ii][jj]," present value: ",sum((topicLabel == jj) & (allWords == ii))                
                asdf
      
    return (WordDocVec_OrderedSet[0],WordDocVec_OrderedSet[1],WordDocVec_OrderedSet[2],\
            WordDocVec_OrderedSet[3],WordDocVec_OrderedSet[4],WordDocVec_OrderedSet[5],\
            allWordCountsL.copy(),allTopicCountsL.copy(),WordDocVec_OrderedSet[8],WordDocVec_OrderedSet[9],\
            WordDocVec_OrderedSet[10],WordDocVec_OrderedSet[11])

f = open('search-terms.json','r')
j = f.readlines()
f.close()
searchj = json.loads(j[0])

demoDict = searchj['candidates']['democrat']
gopDict = searchj['candidates']['gop']

demoCand = demoDict.keys()
gopCand = gopDict.keys()

termList = []
candList = []

for ii in demoCand:
    for jj in demoDict[ii]:
        termList.append(jj)
        candList.append(ii)

for ii in gopCand:
    for jj in gopDict[ii]:
        termList.append(jj)
        candList.append(ii)

termList = np.array(termList)
candList = np.array(candList)

def getCandidate(tweet):
    tweetWords = tweet.split()
    for ii in xrange(len(tweetWords)):
        currWord = tweetWords[ii].lower()
        countCand = 0
        for kk in termList:
            if np.array(kk.lower()) != np.array("rand"):
                if len(kk) <= len(currWord):
                    for ll in xrange(len(currWord)-len(kk)):
                        if np.array(kk.lower()) == np.array(currWord[ll:ll+len(kk)]):
                            return candList[countCand]
            else:
                if np.array(kk.lower()) == np.array(currWord):
                    return candList[countCand]
            countCand = countCand + 1
    return 'general'

numPart = 8
numIt = 300
nTopics = 10


data = open('zipTweets_candidate_even.txt','r')
tweets = data.read()
data.close()

tweets = tweets.split('&&&&&')[:-1]

tweets = sc.parallelize([tweet[:-1] for tweet in tweets])



allTweets = tweets.zipWithIndex().map(lambda x: (x[1]%numPart,x[0])).partitionBy(numPart)

allTweets = allTweets.groupByKey().mapValues(list).cache() # (partitionNum, ['tweet1a tweet2a ...','tweet1b tweet2b ...','tweet1c tweet2c ...',...])

op = allTweets.collect()

"""
for hh in xrange(numPart):
    #for ii in xrange(len(op[hh][1])):
        #print op[0][1][ii]
        #print hh,len(op[hh][1][ii].split())
    print hh,sum([len(op[hh][1][ii].split()) for ii in xrange(len(op[hh][1]))])
asdf
"""

partitionCand = [{} for ipart in xrange(numPart)]

for ipart in xrange(numPart):
    countC = 0
    for doc in op[ipart][1]:
        firstString = doc[:200]
        candidateL = getCandidate(firstString)
        partitionCand[ipart][countC] = candidateL
        countC += 1


allTweets = allTweets.mapValues(preProcessWords).cache() # (partitionNum, (wordVec,docVec))

"""
vals = allTweets.collect()[0][1]

print vals[0]
print len(vals[0])
print np.unique(vals[0])
print len(np.unique(vals[0]))

asdf
"""

#print allTweets.collect()[0][1][0][:20]

startT = time.time()
allTweets = allTweets.mapValues(stationaryLDA).cache() # (partitionNum, (theta,phi,logLik,uWords,uDocs,topicLabel,\
                                               # wordTopicCounters, topicCounters, docTopicCounters, docCounters, allWords, docLabel))

orderedPairs = allTweets.collect()

# accumulate word-topic and topic counts across partitions
allWordCounts = {}
allTopicCounts = {}
for ii in xrange(nTopics):
    allTopicCounts[ii] = 0

logLikMat = np.zeros((numIt*2,numPart))
for ii in xrange(numPart):
    # current partition's word-topic counts dictionary
    currWordTopicCounter = orderedPairs[ii][1][6]
    logLikMat[0:2,ii] = orderedPairs[ii][1][2]
    for jj in currWordTopicCounter.keys():
        if jj not in allWordCounts:
            allWordCounts[jj] = {}
            for uu in xrange(nTopics):
                allWordCounts[jj][uu] = 0
        for kk in currWordTopicCounter[jj].keys():
            allWordCounts[jj][kk] += currWordTopicCounter[jj][kk]
            allTopicCounts[kk] += currWordTopicCounter[jj][kk]

# code currently break here in "synchronizeCounts", see "synchronizeCounts" above
#allTweets = allTweets.mapValues(lambda x: synchronizeCounts(x,allWordCounts.copy(),allTopicCounts.copy()))
allTweets = allTweets.mapValues(lambda x: (x[0],x[1],x[2],x[3],x[4],x[5],allWordCounts.copy(),\
                                            allTopicCounts.copy(),x[8],x[9],x[10],x[11])).cache()

allTweets.count()
allWordCounts0 = allWordCounts.copy()

"""
vocabulary = allWordCounts0.keys()

print vocabulary
print len(vocabulary)

asdf
"""

for numIt in xrange(numIt-1):
    #print "iteration: ",numIt
    allTweets = allTweets.mapValues(stationaryLDA_post).cache()

    orderedPairs = allTweets.collect()
    allWordCounts = {}
    allTopicCounts = {}
    for ii in xrange(nTopics):
        allTopicCounts[ii] = 0
    
    for ii in xrange(numPart):
        currWordTopicCounter = orderedPairs[ii][1][6]
        logLikMat[2*(numIt+1):2*(numIt+1)+2,ii] = orderedPairs[ii][1][2]
        for jj in currWordTopicCounter.keys():
            if jj not in allWordCounts:
                allWordCounts[jj] = {}
                for uu in xrange(nTopics):
                    allWordCounts[jj][uu] = 0
            for kk in currWordTopicCounter[jj].keys():
                allWordCounts[jj][kk] += currWordTopicCounter[jj][kk]

    for ii in allWordCounts.keys():
        for jj in allWordCounts[ii].keys():
            allWordCounts[ii][jj] -= (numPart-1)*allWordCounts0[ii][jj]
    for ii in allWordCounts.keys():
        for jj in allWordCounts[ii].keys():
            allTopicCounts[jj] += allWordCounts[ii][jj]

    allWordCounts0 = allWordCounts.copy()
    allTweets = allTweets.mapValues(lambda x: (x[0],x[1],x[2],x[3],x[4],x[5],allWordCounts.copy(),\
                                            allTopicCounts.copy(),x[8],x[9],x[10],x[11])).cache()


allDocCounts = {}
allDocs = {}
for ii in xrange(numPart):
    currDocTopicCounter = orderedPairs[ii][1][8]
    for jj in currDocTopicCounter.keys():
        candTemp = partitionCand[ii][jj]
        if candTemp not in allDocCounts:
            allDocCounts[candTemp] = {}
            for uu in xrange(nTopics):
                allDocCounts[candTemp][uu] = 0
        for kk in currDocTopicCounter[jj].keys():
            allDocCounts[candTemp][kk] += currDocTopicCounter[jj][kk]
            if candTemp not in allDocs:
                allDocs[candTemp] = currDocTopicCounter[jj][kk]
            else:
                allDocs[candTemp] += currDocTopicCounter[jj][kk]

uniqueWords = np.array(allWordCounts.keys())
nVocab = len(uniqueWords)
phi = np.zeros((nTopics,nVocab))
beta = 0.01
alpha = 0.001
for ii in xrange(0,nTopics):
    sumCurr = allTopicCounts[ii]
    for jj in xrange(0,nVocab):
        phi[ii,jj] = (allWordCounts[uniqueWords[jj]][ii] + beta)/(sumCurr + nVocab*beta)

uniqueCand = np.array(allDocs.keys())
nCand = len(uniqueCand)
theta = np.zeros((nCand,nTopics))
for ii in xrange(0,nCand):
    sumCurr = allDocs[uniqueCand[ii]]
    for jj in xrange(0,nTopics):
        theta[ii,jj] = (allDocCounts[uniqueCand[ii]][jj] + alpha)/(sumCurr + nTopics*alpha)

for ii in xrange(nTopics):
    print uniqueWords[np.argsort(phi[ii,:])[-10:]]

for ii in xrange(nCand):
    topTopic = np.argsort(theta[ii,:])[-1]
    print uniqueCand[ii], ': ',uniqueWords[np.argsort(phi[topTopic,:])[-10:]]

endT = time.time()
print "total time: ",endT-startT
np.savetxt('logLik_candidate_even.txt',logLikMat)







