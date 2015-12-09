
from kafka import SimpleProducer, KafkaClient
from os.path import expanduser
import requests
from requests_oauthlib import OAuth1
import urllib, datetime, time, json, sys, boto3
import creds # we made this module for importing twitter api creds

client = boto3.client('emr')
s3res  = boto3.resource('s3')

# how many minutes should the stream stay open?
minutes_forward = int(s3res.Object('cs205-final-project','setup/stream_duration.txt').get()['Body'].read())
minutes_forward = 10
# we will reset duration to this default after the script finishes running (see end of script)
DURATION_DEFAULT = 5

path = expanduser("~")
on_cluster = (path == "/home/hadoop")


''' list_clusters() is used here to find the current cluster ID
	WARNING: this is a little shaky, as there may be >1 clusters running in production
			 better to search by cluster name as well as state
'''

''' We need to know if we're on an EMR cluster or a local machine.

	- If we are on a cluster:
		* We can't set 'localhost' for the kafka hostname, because other 
	  	  nodes will have their own localhosts. 
	 	* We can determine the private IP address of the master node (where Kafka runs), and
	      use that instead of localhost.
	    * We set the location for search-terms.txt on s3
	- If we are on a local machine, no cluster:
		* We set Kafka's hostname to localhost.
	    * We set the location for search-terms.txt in our local directory
'''


if on_cluster:
	path += '/scripts/'
	clusters = client.list_clusters(ClusterStates=['RUNNING','WAITING','BOOTSTRAPPING'])['Clusters']
	cid = clusters[0]['Id']
	master_instance = client.list_instances(ClusterId=cid,InstanceGroupTypes=['MASTER'])
	hostname 		= master_instance['Instances'][0]['PrivateIpAddress']
else:
	import findspark
	findspark.init()
	path += '/git-local/'
	hostname = 'localhost'

search_terms_fname = path + 'search-terms.txt'

# kafka can have multiple ports if multiple producers, be careful
kafka_port     = '9092'
kafka_host = ':'.join([hostname,kafka_port])

kafka = KafkaClient(kafka_host)
producer = SimpleProducer(kafka)

APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET = creds.get_twitter_creds()

config_token = OAuth1(APP_KEY,
					  client_secret=APP_SECRET,
					  resource_owner_key=OAUTH_TOKEN,
					  resource_owner_secret=OAUTH_TOKEN_SECRET)

config_url = 'https://stream.twitter.com/1.1/statuses/filter.json'

f = open(search_terms_fname,'r')
search_terms = f.read().replace("\n",",")
f.close()

# some search terms have apostrophes, maybe other chars? need to be url-encoded for query string
search_terms = urllib.urlencode({"track":search_terms}).split("=")[1]
# Query parameters to Twitter Stream API
data      = [('language', 'en'), ('track', search_terms)]
query_url = config_url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in data])
# Use stream=True switch on requests.get() to pull in stream indefinitely
response  = requests.get(query_url, auth=config_token, stream=True)


def set_end_time(minutes_forward=minutes_forward):
	''' This function is only for initial test output. We'll probably delete it soon.
		It defines the amount of minutes we keep the tweet stream open for ingestion.
		In production this will be open-ended, or it will be set based on when the debate ends.
	'''
	year   = time.localtime().tm_year
	month  = time.localtime().tm_mon
	day    = time.localtime().tm_mday
	hour   = time.localtime().tm_hour
	minute = time.localtime().tm_min
	newmin = (minute + minutes_forward) % 60 # if adding minutes_forward goes over 60 min, take remainder
	if newmin < minute:
		hour = hour + 1
		minute = newmin
	else:
		minute += minutes_forward

	return {"year":year,"month":month,"day":day,"hour":hour,"minute":minute}

end_time = set_end_time()

''' Twitter API returns a number of different status codes. 
	We want status_code == 200.
	See https://dev.twitter.com/streaming/overview/connecting for more.

	NOTE: Twitter has 'etiquette' guidelines for how to handle 503, 401, etc. We should follow them!
		  Right now we don't do anything about this, other than to report the error to stdout.
'''
print "END TIME:",end_time

if response.status_code == 200:
	print "Reponse Code = 200"
	ct = 0
	''' We will almost certainly not keep this code.

		timesup just picks an end point (currently 2 minutes ahead) to stop ingesting tweets.
		In production, we'd keep ingesting until either an error was thrown or the debate ended.

		For that matter, we also need better error handling here, like how long to wait before 
		reconnecting if the stream drops or rate limits out? 
	'''
	timesup = datetime.datetime(end_time['year'],
								  end_time['month'],
								  end_time['day'],
								  end_time['hour'],
								  end_time['minute']).strftime('%s')
			  
	for line in response.iter_lines():  # Iterate over streaming tweets
		if int(timesup) > time.time():
			#print(line.decode('utf8'))
			try:
				producer.send_messages('tweets', line)
			except:
				time.sleep(1)
				producer.send_messages('tweets', line)
			ct+=1
		else:
			break
else:
	print("ERROR Response code:{}".format(response.status_code))
	try:
		producer.send_messages('tweets', "ERROR Response code:{}".format(response.status_code))
	except: 
		time.sleep(1)
		producer.send_messages('tweets', "ERROR Response code:{}".format(response.status_code))
print "END twitter-in.py"
response.close()

# restore duration default if changed
if minutes_forward != DURATION_DEFAULT:
	s3res.Object('cs205-final-project','setup/stream_duration.txt').put(Body=str(DURATION_DEFAULT))
	


