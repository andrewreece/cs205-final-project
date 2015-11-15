import findspark
findspark.init()
import pyspark

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import time, datetime, json





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

def create_db_conn():
	import boto.sdb
	conn = boto.sdb.connect_to_region(
					'us-east-1',
					aws_access_key_id='AKIAJIDIX4MKTPI4Y27A',
					aws_secret_access_key='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')
	return conn

def get_table(c,tname):
	import boto.sdb
	table = c.get_domain(tname)
	return table

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

#start spark streaming context with sc
#note: you need to create sc if you're not running the ispark setup in ipython notebook
sc = pyspark.SparkContext()
ssc = StreamingContext(sc, 5)
# create kafka streaming context
kstream = KafkaUtils.createDirectStream(ssc, ["tweets"], {"bootstrap.servers": "localhost:9092"})

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
		#.pprint()
		.foreachRDD(lambda rdd: rdd.foreachPartition(write_to_db))
		#.pprint()
		#.saveAsTextFiles("tw","json")
)
ssc.start()
ssc.awaitTermination()

print
print "finished"
print