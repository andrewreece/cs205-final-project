import findspark
findspark.init()
import pyspark

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import time, datetime, json

def make_json(ix):
	try:
		return lambda x: json.loads(x[ix].decode('utf8'))
	except:
		#return x[1]
		return lambda x: "error"+str(x[ix].decode('utf8'))

#start spark streaming context with sc
#note: you need to create sc if you're not running the ispark setup in ipython notebook
sc = pyspark.SparkContext()
ssc = StreamingContext(sc, 1)
# create kafka streaming context
kstream = KafkaUtils.createDirectStream(ssc, ["tweets"], {"bootstrap.servers": "localhost:9092"})

hour = time.localtime().tm_hour
minute = time.localtime().tm_min + 2

timesup = datetime.datetime(2015,11,11,hour,minute).strftime('%s')

#while int(timesup) > time.time():
kstream.map(make_json(1)).map(lambda x: (x['user']['screen_name'],x['text'])).pprint()

ssc.start()
ssc.awaitTermination()

print
print "finished"
print