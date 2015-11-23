import time, datetime, json, boto3

BATCH_DURATION = 5

client = boto3.client('emr')

# this is a little shaky, as there may be >1 clusters running in production
# maybe better to search by cluster name as well?
clusters = client.list_clusters(ClusterStates=['RUNNING','WAITING','BOOTSTRAPPING'])['Clusters']

if len(clusters) > 0:
	import pyspark
	from pyspark.streaming import StreamingContext
	from pyspark.streaming.kafka import KafkaUtils

	cid 			= clusters[0]['Id']
	master_instance = client.list_instances(ClusterId=cid,InstanceGroupTypes=['MASTER'])
	master_ip 		= master_instance['Instances'][0]['PrivateIpAddress']
	kafka_host 		= master_ip + ':' + '9092'

	sc = pyspark.SparkContext()
	ssc = StreamingContext(sc, BATCH_DURATION) # second arg is num seconds per DStream-RDD

else:
	import findspark
	findspark.init()
	import pyspark
	from pyspark.streaming import StreamingContext
	from pyspark.streaming.kafka import KafkaUtils

	kafka_host 	= 'localhost:9092'

	#start spark streaming context with sc
	#note: you need to create sc if you're not running the ispark setup in ipython notebook
	sc = pyspark.SparkContext()
	ssc = StreamingContext(sc, BATCH_DURATION) # second arg is num seconds per DStream-RDD


#start spark streaming context with sc
#note: you need to create sc if you're not running the ispark setup in ipython notebook
#sc = pyspark.SparkContext()
#ssc = StreamingContext(sc, BATCH_DURATION) # second arg is num seconds per DStream-RDD

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
	import boto3
	table_name="tweettest"
	client = boto3.client('sdb', region_name='us-east-1')
	for row in iterator:
		k,v = row
		attrs = []
		try:
			for k2,v2 in v.items():
				if isinstance(v2,list):
					v2 = ','.join([val for val in v2]) if len(v2)>0 else ''
				elif v2 is None:
					v2 = ''
				# mytext = u'<some string containing 4-byte chars>'
				v2 = v2.encode('utf8').decode('ascii','ignore')
				attrs.append( {'Name':k2,'Value':v2,'Replace':True} )
		except Exception, e:
			print str(e)
			print v 
		try:
			client.put_attributes(
				DomainName=	table_name,
				ItemName  =	str(k),
				Attributes=	attrs 
			) 
		except Exception, e:
			print str(e)
			print attrs
			print

# create kafka streaming context
kstream = KafkaUtils.createDirectStream(ssc, ["tweets"], {"bootstrap.servers": kafka_host})

year   = time.localtime().tm_year
month  = time.localtime().tm_mon
day    = time.localtime().tm_mday
hour   = time.localtime().tm_hour
minute = time.localtime().tm_min
newmin = (minute + 2) % 60
if newmin < minute:
	hour = hour + 1
	minute = newmin

#timesup = datetime.datetime(year,month,day,hour,minute).strftime('%s')

#while int(timesup) > time.time():
(
kstream .map(make_json(1))
		#.map(lambda x: ("Retweeted:{}".format('retweeted_status' in x.keys()),(x['geo'],x['user']['location'],x['entities']['urls'])))
		#.pprint()
		.filter(filter_tweets(1))
		.map(get_relevant_fields(1))
		#.pprint()
		.foreachRDD(lambda rdd: rdd.foreachPartition(write_to_db))
		#.pprint()
		#.saveAsTextFiles("tw","json")
)
ssc.start()
ssc.awaitTermination()

