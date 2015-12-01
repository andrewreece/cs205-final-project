from utils import *

data_path 			= 's3n://cs205-final-project/tweets/gardenhose/gop/sep16/2015-09-16-20*.gz'
path 				= '/Users/andrew/git-local/'




''' IMPORTANT: 
				party_of_debate needs to be set automatically when the cluster starts up 

	And how will this affect our ability to have a generic, non-debate version?
	We'd need to pass in a flag word like 'notdebate' along with the user-defined search terms.
'''

party_of_debate 	= 'gop'




search_json_fname 	= path+'search-terms.json'

# Load nested JSON of search terms
jdata = get_search_json(search_json_fname)

# Collect all search terms in JSON into search_terms list
search_terms = pool_search_terms(jdata)

# Streaming Spark splits data into separate RDDs every BATCH_DURATION seconds
BATCH_DURATION = 30 

# kafka can have multiple ports if multiple producers, be careful
kafka_port     = '9092'

''' We need to know if we're on an EMR cluster or a local machine.

	- If we are on a cluster:
		* We can't set 'localhost' for the kafka hostname, because other 
		  nodes will have their own localhosts. 
		* We can determine the private IP address of the master node (where Kafka runs), and
		  use that instead of localhost.
	- If we are on a local machine, no cluster:
		* We set Kafka's hostname to localhost.
		* We need to import findspark before loading pyspark.'''

cluster_running, cid = is_cluster_running()

if cluster_running:
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

filtered = (kstream.map(make_json) 
				.filter(lambda tweet: filter_tweets(tweet,search_terms))
				.map(lambda tweet: get_relevant_fields(tweet,jdata,party_of_debate))
				.cache()
		)

filtered.foreachRDD(lambda rdd: process(rdd,jdata,party_of_debate))
#kstream.foreachRDD(process)
#kstream.foreachRDD(lambda rdd: rdd.foreachPartition(write_to_db))
ssc.start()
ssc.awaitTermination() # we should figure out how to set a termination marker (NOV 26)

