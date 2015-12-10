from utils import *
from os.path import expanduser
import boto3


s3res  = boto3.resource('s3')

bucket_name  = 'cs205-final-project'
settings_key = 'setup/bake-defaults.json'


''' We need to know if we're on an EMR cluster or a local machine.

	- If we are on a cluster:
		* We can't set 'localhost' for the kafka hostname, because other 
		  nodes will have their own localhosts. 
		* We can determine the private IP address of the master node (where Kafka runs), and
		  use that instead of localhost.
	- If we are on a local machine, no cluster:
		* We set Kafka's hostname to localhost.
		* We need to import findspark before loading pyspark.'''

path = expanduser("~")
on_cluster = (path == "/home/hadoop")

if on_cluster:
	path += '/scripts/'
	hostname = get_hostname()	
else:
	import findspark
	findspark.init()
	path += '/git-local/'
	hostname = 'localhost'

# kafka can have multiple ports if multiple producers, be careful
kafka_port     = '9092'
kafka_host = ':'.join([hostname,kafka_port])



settings = json.loads(s3res.Object(bucket_name,settings_key).get()['Body'].read())
party_of_debate = settings['Topic_Tracked']['val']
# Streaming Spark splits data into separate RDDs every BATCH_DURATION seconds
BATCH_DURATION = int(settings['Batch_Duration']['val'])
# how many minutes should the stream stay open?
STREAM_DURATION = int(settings['Stream_Duration']['val'])


import pyspark
from pyspark.streaming import StreamingContext
''' NOTE: The KafkaUtils library comes with pyspark, but the .jar needed to make it work does not!
		  We have the .jar saved in the main directory on both EMR clusters and /git-local, its name is:
			spark-streaming-kafka-assembly_2.10-1.5.2.jar
		  This needs to be added to the spark-submit call using the --jars flag. See run-main.sh '''
from pyspark.streaming.kafka import KafkaUtils

sc = pyspark.SparkContext()
ssc = StreamingContext(sc, BATCH_DURATION) # second arg is num seconds per DStream-RDD

quiet_logs(sc)

#time = set_end_time()
#old time code: timesup = datetime.datetime(year,month,day,hour,minute).strftime('%s')
#				while int(timesup) > time.time():

# create kafka streaming context
kstream = KafkaUtils.createDirectStream(ssc, ["tweets"], {"bootstrap.servers": kafka_host})


search_json_fname = path+'search-terms.json'
# Load nested JSON of search terms
jdata = get_search_json(search_json_fname)
# Collect all search terms in JSON into search_terms list
search_terms = pool_search_terms(jdata)

filtered = (kstream.map(lambda data: make_json(data,BATCH_DURATION)) 
				.filter(lambda tweet: filter_tweets(tweet[1],search_terms))
				.map(lambda tweet: get_relevant_fields(tweet,jdata,party_of_debate))
				.cache()
		   )

# writes individual tweets to sdb domain: tweets
filtered.foreachRDD(lambda rdd: rdd.foreachPartition(write_to_db))
# writes analysis output (sentiment, lda) to sdb doman: sentiment
filtered.foreachRDD(lambda rdd: process(rdd,jdata,party_of_debate))


ssc.start()
ssc.awaitTermination() # we should figure out how to set a termination marker (NOV 26)

