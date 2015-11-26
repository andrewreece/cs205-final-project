import time, datetime, json, boto3

# Streaming Spark splits data into separate RDDs every BATCH_DURATION seconds
BATCH_DURATION = 5 
# kafka can have multiple ports if multiple producers, be careful
kafka_port     = '9092'

client = boto3.client('emr')

''' list_clusters() is used here to find the current cluster ID
	WARNING: this is a little shaky, as there may be >1 clusters running in production
			 better to search by cluster name as well as state
'''
clusters = client.list_clusters(ClusterStates=['RUNNING','WAITING','BOOTSTRAPPING'])['Clusters']

''' We need to know if we're on an EMR cluster or a local machine.

	- If we are on a cluster:
		* We can't set 'localhost' for the kafka hostname, because other 
	  	  nodes will have their own localhosts. 
	 	* We can determine the private IP address of the master node (where Kafka runs), and
	      use that instead of localhost.
	- If we are on a local machine, no cluster:
		* We set Kafka's hostname to localhost.
		* We need to import findspark before loading pyspark.
'''
if len(clusters) > 0:

	cid 			= clusters[0]['Id']
	master_instance = client.list_instances(ClusterId=cid,InstanceGroupTypes=['MASTER'])
	hostname 		= master_instance['Instances'][0]['PrivateIpAddress']

else:

	import findspark
	findspark.init()
	hostname = 'localhost'

kafka_host = ':'.join([hostname,kafka_port])

import pyspark
from pyspark.streaming import StreamingContext
''' NOTE: The KafkaUtils library comes with pyspark, but the .jar needed to make it work does not!
		  
		  We have the .jar saved in the main directory on both EMR clusters and /git-local, its name is:

		  	spark-streaming-kafka-assembly_2.10-1.5.2.jar

		  This needs to be added to the spark-submit call using the --jars flag. See run-main.sh 
'''
from pyspark.streaming.kafka import KafkaUtils

sc = pyspark.SparkContext()
ssc = StreamingContext(sc, BATCH_DURATION) # second arg is num seconds per DStream-RDD


def make_json(ix):
	''' Get stringified JSOn from Kafka, attempt to convert to JSON '''
	try:
		return lambda x: json.loads(x[ix].decode('utf-8'))
	except:
		#return x[1]
		return lambda x: "error"+str(x[ix].decode('utf-8'))

def filter_tweets(_):
	''' Filters out the tweets we do not want.  Filters include:
			* No retweets 
			* No geolocation or location field (do we really care about this?)
			* English language only
			* No tweets with links
				- We need to check both entities and media fields for this (is that true?)
	'''
	return lambda t: (('retweeted_status' not in t.keys()) 						  and 
					  ((t['geo'] is not None) or (t['user']['location'] is not None)) and
					  (t['user']['lang']=='en') 					  and
					  (len(t['entities']['urls'])==0) 				  and
					  ('media' not in t['entities'].keys())
					  )

def get_relevant_fields(_):
	''' Reduce the full set of metadata down to only those we care about, including:
			* timestamp
			* username
			* text of tweet 
			* hashtags
			* geotag coordinates (if any)
			* location (user-defined in profile, not necessarily current location)
	'''
	return lambda t:(t['id'], 
					   {"timestamp":  t['created_at'],
						"username":	t['user']['screen_name'],
						"text":		t['text'],
						"hashtags":	[el['text'] for el in t['entities']['hashtags']],
						"geotag":	t['geo'],
						"user_loc":	t['user']['location']
						}
					)

def write_to_db(iterator, table_name="tweettest"):
	''' Write output to AWS SimpleDB table after analysis is complete 
			- Uses boto3 and credentials file. (If AWS cluster, credentials are associated with creator.)
			- UTF-8 WARNING!
				* SDB does not like weird UTF-8 characters, including emojis. 
				* Currently we remove them entirely with .encode('utf8').decode('ascii','ignore')
				* If we actually want to use emojis (or even reprint tweets accurately), we'll need to 
				  figure out a way to preserve UTF weirdness. 
				* This is not only emojis, some smart quotes and apostrophes too, and other characters. 
	'''

	''' NOTE: We ran into issues when we had a global import for boto3 in this script.
			  Assuming this has something to do with child nodes running this function but not the whole
			  script?  
			  When we import boto3 inside this function, everything works.
	'''
	import boto3 			# keep local boto import!
	client = boto3.client('sdb', region_name='us-east-1')

	''' write_to_db() is called by foreachPartition(), which passes in an iterator object automatically.

		The iterator rows are each entry (for now, that means "each tweet") in the dataset. 
		Below, we use the implicitly-passed iterator to loop through each data point and write to SDB.

		NOTE: Keep an eye on the UTF mangling needed. If you don't mangle, it barfs.
			  * The standard solutions (simple encode/decode conversions) do NOT work. 
			  * See the process book (somewhere around NOV 21) for a few links discussing this problem.
			  * It's actually an issue with the way SDB has its HTTP headers set up, and it's fixable if you
			    hack the Ruby source code, but since we're using Boto3 it seems we can't get at the headers.
			  * You added a comment on the Boto3 source github page where this issue was being discussed,
			    make sure to check and see if the author has answered you!
	'''
	for row in iterator:
		k,v = row
		attrs = []
		try:
			for k2,v2 in v.items():
				# If v2 IS A LIST: join as comma-separated string
				if isinstance(v2,list):
					v2 = ','.join([val for val in v2]) if len(v2)>0 else ''
				# If v2 IS EMPTY: convert to empty string
				elif v2 is None:
					v2 = ''
				# Get rid of all UTF-8 weirdness, including emojis.
				v2 = v2.encode('utf8').decode('ascii','ignore')
				attrs.append( {'Name':k2,'Value':v2,'Replace':True} )
		except Exception, e:
			print str(e)
			print v 
		try:
			# write row of data to SDB
			client.put_attributes(
				DomainName=	table_name,
				ItemName  =	str(k),
				Attributes=	attrs 
			) 
		except Exception, e:
			print str(e)
			print attrs
			print

def set_end_time(minutes_forward=2):
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

	return {"year":year,"month":month,"day":day,"hour":hour,"minute":minute}

#time = set_end_time()
#old time code: timesup = datetime.datetime(year,month,day,hour,minute).strftime('%s')
#				while int(timesup) > time.time():

# create kafka streaming context
kstream = KafkaUtils.createDirectStream(ssc, ["tweets"], {"bootstrap.servers": kafka_host})

(
kstream .map(make_json(1)) 
		.filter(filter_tweets(1))
		.map(get_relevant_fields(1))
		.foreachRDD(lambda rdd: rdd.foreachPartition(write_to_db))
		#.pprint()
		#.saveAsTextFiles("tw","json")
)
ssc.start()
ssc.awaitTermination() # we should figure out how to set a termination marker (NOV 26)

