import os
import re
import codecs
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

  path = expanduser("~")
  if path == "/home/hadoop":
    path += '/scripts/'
  else:
    path += '/git-local/'

  fileName = path+'labmt.txt'

  try:
    f = codecs.open(fileName,'r','utf8')
  except IOError:
    relpath = os.path.abspath(__file__).split(u'/')[1:-1]
    relpath.append('data')
    relpath.append(fileName)
    fileName = '/'+'/'.join(relpath)
    f = codecs.open(fileName,'r','utf8')
  except:
    raise('could not open the needed file')

  # skip the first line
  f.readline()

  tmpDict = dict([(line.split(u'\t')[0].rstrip(u'"').lstrip(u'"'),
        [x.rstrip(u'"') for x in line.split(u'\t')[1:]]) for line in f])

  f.close()
  
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

  # build vector of all scores
  f = codecs.open(fileName,'r','utf8')
  f.readline()
  tmpList = [float(line.split(u'\t')[2]) for line in f]
  f.close()
  # build vector of all words
  f = codecs.open(fileName,'r','utf8')
  f.readline()
  wordList = [line.split(u'\t')[0] for line in f]
  f.close()

  if returnVector:
    return tmpDict,tmpList,wordList
  else:
    return tmpDict

def emotion(tmpStr,someDict,scoreIndex=1,shift=False,happsList=[]):
  """Take a string and the happiness dictionary, and rate the string.

  If shift=True, will return a vector (also then needs the happsList)."""
  scoreList = []

  replaceStrings = ['---','--','\'\'']
  for replaceString in replaceStrings:
        tmpStr = tmpStr.replace(replaceString,' ')
  words = [x.lower() for x in re.findall(r"[\w\@\#\'\&\]\*\-\/\[\=\;]+",tmpStr,flags=re.UNICODE)]

  for word in words:
    if word in someDict:
      scoreList.append(float(someDict[word][scoreIndex]))

  happs_avg = np.mean(scoreList) if len(scoreList) > 0 else None
  happs_std = np.std(scoreList) if len(scoreList) > 0 else None
  
  return (happs_avg, happs_std)



