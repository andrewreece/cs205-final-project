
# coding: utf-8


import numpy as np
import pandas as pd
import twython
from twython import TwythonStreamer
import re
from requests_oauthlib import OAuth1
import urllib

import sys
import ast
import json

import findspark
findspark.init('/usr/local/Cellar/apache-spark/1.5.1/libexec')

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import requests

import threading
import Queue
import time

APP_KEY = "bv6mnYBiFeEVKvPEZlg"
APP_SECRET = "nQZk9Ca8qqJxc1Za07WyW0VPZ6gtAUSF3oPD5sun0"
OAUTH_TOKEN = "606525030-ilOtJstbRvFCjUNMtOu8DP2HQKGWpQvmUsF6fblE"
OAUTH_TOKEN_SECRET = "xSVE47qVOFxxZm1oqKwL6zwLVMWpzxCUYGmLJ6CVHR0mZ"

config_token = OAuth1(APP_KEY,
                      client_secret=APP_SECRET,
                      resource_owner_key=OAUTH_TOKEN,
                      resource_owner_secret=OAUTH_TOKEN_SECRET)

config_url = 'https://stream.twitter.com/1.1/statuses/filter.json'

search_terms = np.loadtxt("search-terms.txt",delimiter="\n",dtype=object)
search_terms = ','.join(search_terms)
search_terms = urllib.urlencode({"track":search_terms}).split("=")[1]

BATCH_INTERVAL = 60  # How frequently to update (seconds)
BLOCKSIZE = 50  # How many tweets per update


def main():
    threads = []
    q = Queue.Queue()
    # Set up spark objects and run
    sc  = SparkContext('local[4]', 'Twitter Stream')
    ssc = StreamingContext(sc, BATCH_INTERVAL)
    threads.append(threading.Thread(target=spark_stream, args=(sc, ssc, q)))
    [t.start() for t in threads]
    
def spark_stream(sc, ssc, q):
    """
    Establish queued spark stream.
    For a **rough** tutorial of what I'm doing here, check this unit test
    https://github.com/databricks/spark-perf/blob/master/pyspark-tests/streaming_tests.py
    * Essentially this establishes an empty RDD object filled with integers [0, BLOCKSIZE).
    * We then set up our DStream object to have the default RDD be our empty RDD.
    * Finally, we transform our DStream by applying a map to each element (remember these
        were integers) and setting the next element to be the next element from the Twitter
        stream.
    * Afterwards we perform the analysis
        1. Convert each string to a literal python object
        2. Filter by keyword association (sentiment analysis)
        3. Convert each object to just the coordinate tuple
    :param sc: SparkContext
    :param ssc: StreamingContext
    """
    # Setup Stream
    rdd = ssc.sparkContext.parallelize([0])
    stream = ssc.queueStream([], default=rdd)
    stream = stream.transform(tfunc)
    '''# Analysis
    coord_stream = stream.map(lambda line: ast.literal_eval(line)) \
                        .filter(filter_posts) \
                        .map(get_coord)

    # Convert to something usable....
    coord_stream.foreachRDD(lambda t, rdd: q.put(rdd.collect()))
    
    '''
    stream.foreachRDD(lambda t, rdd: q.put(rdd.collect()))
    # Run!
    ssc.start()
    ssc.awaitTermination()


def stream_twitter_data():
    """
    Only pull in tweets with location information
    :param response: requests response object
        This is the returned response from the GET request on the twitter endpoint
    """
    data      = [('language', 'en'), ('track', search_terms)]
    query_url = config_url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in data])
    response  = requests.get(query_url, auth=config_token, stream=True)
    print(query_url, response) # 200 <OK>
    count = 0
    for line in response.iter_lines():  # Iterate over streaming tweets
        try:
            if count > BLOCKSIZE:
                break
            post     = json.loads(line.decode('utf-8'))
            contents = [post['text'], post['user']['screen_name'], post['user']['location']]
            count   += 1
            if not isinstance(contents,int):
                print(str(contents))
            yield str(contents)
        except:
            print(line)


def tfunc(t, rdd):
    """
    Transforming function. Converts our blank RDD to something usable
    :param t: datetime
    :param rdd: rdd
        Current rdd we're mapping to
    """
    return rdd.flatMap(lambda x: stream_twitter_data())

if __name__ == '__main__':
    sys.exit(main())

