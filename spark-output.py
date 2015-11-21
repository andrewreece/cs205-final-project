import time, datetime, json, boto3

#import findspark
#findspark.init()
import pyspark
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

BATCH_DURATION = 5

client = boto3.client('emr')
clusters = client.list_clusters(ClusterStates=['RUNNING','WAITING','BOOTSTRAPPING'])['Clusters']

if len(clusters) > 0:
	cid = clusters[0]['Id']
	master_instance = client.list_instances(ClusterId=cid,InstanceGroupTypes=['MASTER'])
	master_ip = master_instance['Instances'][0]['PrivateIpAddress']
	kafka_host = master_ip + ':' + '9092'
	search_terms_fname = '/home/hadoop/scripts/search-terms.txt'

	from pyspark.streaming import StreamingContext
	from pyspark.streaming.kafka import KafkaUtils
	sc = pyspark.SparkContext()
	ssc = StreamingContext(sc, BATCH_DURATION) # second arg is num seconds per DStream-RDD

else:
	kafka_host = 'localhost:9092'
	search_terms_fname = '/Users/andrew/git-local/search-terms.txt'

	import findspark
	findspark.init()
	import pyspark
	from pyspark.streaming import StreamingContext
	from pyspark.streaming.kafka import KafkaUtils

	#start spark streaming context with sc
	#note: you need to create sc if you're not running the ispark setup in ipython notebook
	sc = pyspark.SparkContext()
	ssc = StreamingContext(sc, BATCH_DURATION) # second arg is num seconds per DStream-RDD
	


def make_json(ix):
	try:
		return lambda x: json.loads(x[ix].decode('utf-8'))
	except:
		#return x[1]
		return lambda x: "error"+str(x[ix].decode('utf-8'))

def filter_tweets(_):
	return lambda t: (('retweeted_status' not in t.keys()) and 
					  ((t['geo'] is not None) or (t['user']['location'] is not None)) and
					  (t['user']['lang']=='en') and
					  (len(t['entities']['urls'])==0) and
					  ('media' not in t['entities'].keys())
					  )

def get_relevant_fields(_):
	return lambda t:(t['id'], 
					   {"timestamp":	t['created_at'],
						"username":		t['user']['screen_name'],
						"text":			t['text'],
						"hashtags":		[el['text'] for el in t['entities']['hashtags']],
						"geotag":		t['geo'],
						"user_loc":		t['user']['location']
						}
					)

def write_to_db(iterator):
	import boto.sdb
	table_name="test"
	conn = boto.sdb.connect_to_region(
					'us-east-1',
					aws_access_key_id='AKIAJIDIX4MKTPI4Y27A',
					aws_secret_access_key='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')
	table = conn.get_domain(table_name)
	for row in iterator:
		k,v = row
		table.put_attributes(k,v) # make sure this handles multiple records per insert

#example code:
#items = {'item1':{'attr1':'val1'},'item2':{'attr2':'val2'}}
#dstream.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))




host_port = kafka_host # this is set in Kafka server config, make sure it is the same!

# create kafka streaming context
kstream = KafkaUtils.createDirectStream(ssc, ["tweets"], {"bootstrap.servers": host_port})

year   = time.localtime().tm_year
month  = time.localtime().tm_mon
day    = time.localtime().tm_mday
hour   = time.localtime().tm_hour
minute = time.localtime().tm_min + 2

#timesup = datetime.datetime(year,month,day,hour,minute).strftime('%s')

#while int(timesup) > time.time():
(
kstream .map(make_json(1))
		#.map(lambda x: ("Retweeted:{}".format('retweeted_status' in x.keys()),(x['geo'],x['user']['location'],x['entities']['urls'])))
		#.pprint()
		.filter(filter_tweets(1))
		.map(get_relevant_fields(1))
		.pprint()
		#.foreachRDD(lambda rdd: rdd.foreachPartition(write_to_db))
		#.pprint()
		#.saveAsTextFiles("tw","json")
)
ssc.start()
ssc.awaitTermination()

print
print "finished"
print