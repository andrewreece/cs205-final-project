import os, re, codecs, boto3
import numpy as np 
from os.path import expanduser

''' This is a modified version of Andy Reagan's code at 
    https://github.com/andyreagan/labMT-simple/blob/master/labMTsimple/storyLab.py 

    Took out the HTML/PDF functions
    Added standard deviation output to emotion() '''

def emotionFileReader(stopval=1.0,lang="english",min=1.0,max=9.0,returnVector=False):
  """Load the dictionary of sentiment words.
  
  Stopval is our lens, $\Delta _h$, read the labMT dataset into a dict with this lens (must be tab-deliminated).

  With returnVector = True, returns tmpDict,tmpList,wordList. Otherwise, just the dictionary."""

  labMT1flag = False
  scoreIndex = 1 # second value

  s3 = boto3.resource('s3')
  bucket_name  = 'cs205-final-project'
  key_name  = 'scripts/labmt.txt'
  f = s3.Object(bucket_name,key_name).get()['Body'].read().split('\n')
  # skip the first line
  f = f[1:]

  tmpDict = dict([(line.split(u'\t')[0].rstrip(u'"').lstrip(u'"'),
        [x.rstrip(u'"') for x in line.split(u'\t')[1:]]) for line in f])

  
  # remove words
  stopWords = []

  for word in tmpDict:
    # start the index at 0
    if labMT1flag:
      tmpDict[word][0] = int(tmpDict[word][0])-1
    if abs(float(tmpDict[word][scoreIndex])-5.0) < stopval:
      stopWords.append(word)
    else:
      if float(tmpDict[word][scoreIndex]) < min:
        stopWords.append(word)
      else:
        if float(tmpDict[word][scoreIndex]) > max:
          stopWords.append(word)
  
  for word in stopWords:
    del tmpDict[word]

  return tmpDict

def emotion(tmpStr,someDict,scoreIndex=1,shift=False,happsList=[],return_scores=False):
  """Take a string and the happiness dictionary, and rate the string.

     If shift=True, will return a vector (also then needs the happsList)."""
  
  total_word_ct = len(tmpStr.split(" "))
  scoreList = []
  wordList = []
  unusedList = []

  replaceStrings = ['---','--','\'\'']
  for replaceString in replaceStrings:
        tmpStr = tmpStr.replace(replaceString,' ')
  words = [x.lower() for x in re.findall(r"[\w\@\#\'\&\]\*\-\/\[\=\;]+",tmpStr,flags=re.UNICODE)]

  for word in words:
    if word in someDict:
      scoreList.append(float(someDict[word][scoreIndex]))
      wordList.append(word)
    else:
      unusedList.append(word)

  prop_words_used = len(scoreList)/float(total_word_ct)

  happs_avg = np.mean(scoreList) if len(scoreList) > 0 else None
  happs_std = np.std(scoreList) if len(scoreList) > 0 else None
  
  if return_scores:
    return (happs_avg, happs_std, scoreList, wordList, unusedList, total_word_ct, prop_words_used)
  else:
    return (happs_avg, happs_std)



