
from kafka import SimpleProducer, KafkaClient

import numpy as np
import pandas as pd
import re
import requests
from requests_oauthlib import OAuth1
import urllib
import datetime
import time
import json
import sys


kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)


APP_KEY = "bv6mnYBiFeEVKvPEZlg"
APP_SECRET = "nQZk9Ca8qqJxc1Za07WyW0VPZ6gtAUSF3oPD5sun0"
OAUTH_TOKEN = "606525030-ilOtJstbRvFCjUNMtOu8DP2HQKGWpQvmUsF6fblE"
OAUTH_TOKEN_SECRET = "xSVE47qVOFxxZm1oqKwL6zwLVMWpzxCUYGmLJ6CVHR0mZ"

config_token = OAuth1(APP_KEY,
					  client_secret=APP_SECRET,
					  resource_owner_key=OAUTH_TOKEN,
					  resource_owner_secret=OAUTH_TOKEN_SECRET)

config_url = 'https://stream.twitter.com/1.1/statuses/filter.json'

search_terms = np.loadtxt("/Users/andrew/git-local/search-terms.txt",delimiter="\n",dtype=object)
search_terms = ','.join(search_terms)
search_terms = urllib.urlencode({"track":search_terms}).split("=")[1]

data      = [('language', 'en'), ('track', search_terms)]
query_url = config_url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in data])
response  = requests.get(query_url, auth=config_token, stream=True)

BATCH_INTERVAL = 60  # How frequently to update (seconds)
BLOCKSIZE = 50  # How many tweets per update

year   = time.localtime().tm_year
month  = time.localtime().tm_mon
day    = time.localtime().tm_mday
hour   = time.localtime().tm_hour
minute = time.localtime().tm_min + 2

if response.status_code == 200:
	print "Reponse Code = 200"
	ct = 0
	timesup = datetime.datetime(year,month,day,hour,minute).strftime('%s')

	for line in response.iter_lines():  # Iterate over streaming tweets
		if int(timesup) > time.time():
			#print(line.decode('utf8'))
			producer.send_messages('tweets', line)
			ct+=1
		else:
			break
else:
	print("ERROR Response code:{}".format(response.status_code))
	producer.send_messages('tweets', "ERROR Response code:{}".format(response.status_code))
response.close()

	


