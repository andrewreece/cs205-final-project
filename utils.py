import time, json, boto3, re
from dateutil import parser, tz
from datetime import datetime, timedelta
from sentiment import *
from pyspark.sql import SQLContext, Row
import pyspark.sql.functions as sqlfunc
from pyspark.sql.types import *

search_terms = []
n_parts = 10

def get_search_json(fname):
	f = open(fname,'r')
	rawdata = f.readlines()
	f.close()
	jdata = json.loads(rawdata[0])
	return jdata

def pool_search_terms(j):
    ''' Short recursive routine to pull out all search terms in search-terms.json '''
    if isinstance(j,dict):
        for j2 in j.values():
            pool_search_terms(j2)
    else:
        search_terms.extend( j )
    return search_terms

def is_cluster_running():
    import boto3
    client = boto3.client('emr')

    ''' list_clusters() is used here to find the current cluster ID
        WARNING: this is a little shaky, as there may be >1 clusters running in production
                 better to search by cluster name as well as state
    '''
    clusters = client.list_clusters(ClusterStates=['RUNNING','WAITING','BOOTSTRAPPING'])['Clusters']

    clusters_exist = len(clusters) > 0
    if clusters_exist:
        cid = clusters[0]['Id']
    else:
        cid = None
    return clusters_exist, cid


def update_tz(d,dtype,only_tstamp=False):
    ''' Updates time zone for date stamp to US EST (the time zone of the debates) '''
    if only_tstamp:
        tstamp = d 
    else:
        tstamp = d[1]
    def convert_timezone(item,item_is_only_tstamp=False):
        if item_is_only_tstamp:
            dt = item 
        else:
            ts = item['timestamp']
            dt = parser.parse(ts)
        from_zone = tz.gettz('UTC')
        to_zone = tz.gettz('America/New_York')
        utc = dt.replace(tzinfo=from_zone)
        return utc.astimezone(to_zone)
    if dtype == "sql":
        return Row(id=d[0], time=convert_timezone(tstamp))
    elif dtype == "pandas":
        return convert_timezone(tstamp,only_tstamp)


def make_json(tweet,interval):
    ''' Get stringified JSOn from Kafka, attempt to convert to JSON '''
    try:
        dt           = datetime.now()
        tstamp       = datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute,interval*(dt.second // interval))
        local_tstamp = update_tz(tstamp,"pandas",only_tstamp=True)
        batchtime    = local_tstamp.strftime('%s')
        return (batchtime, json.loads(tweet[1].decode('utf-8')))
    except:
        return "error on make_json"


def filter_tweets(item,terms):

    ''' Filters out the tweets we do not want.  Filters include:
            * No non-tweets (eg. delete commands)
            * No retweets 
            * English language only
            * No tweets with links
                - We need to check both entities and media fields for this (is that true?) 
            * Matches at least one of the provided search terms '''

    # Define regex pattern that covers all search terms
    pattern = '|(\s|#|@)'.join(terms)

  
    try:
        return (isinstance(item,dict) and 
                ('delete' not in item.keys()) and
                ('retweeted_status' not in item.keys())   and
                (item['lang']=='en')                       and
                (len(item['entities']['urls'])==0)                   and
                ('media' not in item['entities'].keys()) and
                (re.search(pattern,item['text'],re.I) is not None)
               )
    except Exception, e:   
        print 
        print "This item is funny.  Funny how?" 
        print str(e)
        print 'here is the item'
        print item
        print 


def get_relevant_fields(item,json_terms,debate_party):
    ''' Reduce the full set of metadata down to only those we care about, including:
            * timestamp
            * username
            * text of tweet 
            * hashtags
            * geotag coordinates (if any)
            * location (user-defined in profile, not necessarily current location)
    '''

    the_tweet = item[1]
    batchtime = item[0]
    cands     = json_terms['candidates'][debate_party]
    mentioned = []

    # loop over candidates, check if tweet mentions each one
    for name, terms in cands.items():
        p = '|(\s|#|@)'.join(terms) # regex allows for # hashtag, @ mention, or blank space before term
        rgx = re.search(p,the_tweet['text'],re.I)
        if rgx: # if candidate-specific search term is matched
            mentioned.append( name ) # add candidate surname to mentioned list

    if len(mentioned) == 0: # if no candidates were mentioned specifically
        mentioned.append( "general" ) # then tweet must be a general reference to the debate

    tweet_timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(the_tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
    try:
        return (the_tweet['id'], 
                {"timestamp":       tweet_timestamp,
                 "batchtime":       batchtime,
                 "username":        the_tweet['user']['screen_name'],
                 "text":            the_tweet['text'].encode('utf8').decode('ascii','ignore'),
                 "hashtags":        [el['text'].encode('utf8').decode('ascii','ignore') for el in the_tweet['entities']['hashtags']],
                 "first_term":      mentioned[0],
                 "search_terms":    mentioned,
                 "multiple_terms":  len(mentioned) > 1
                }
               )
    except Exception,e:
        print "this error is coming from get_relevant_fields"
        print str(e)
        print "this is item:"
        print item 
        print 

def make_row(d,doPrint=False):
    tid = d[0]
    tdata = d[1]

    return Row(id             =tid,
               username       =tdata['username'],
               timestamp      =tdata['timestamp'],
               batchtime      =tdata['batchtime'],
               hashtags       =tdata['hashtags'] if tdata['hashtags'] is not None else '',
               text           =tdata['text'],
               search_terms   =tdata['search_terms'],
               multiple_terms =tdata['multiple_terms'],
               first_term     =tdata['first_term']
              )

def process(rdd,json_terms,debate_party,domain_name='sentiment',n_parts=10):

    rdd.cache()


    candidate_dict = {}
    candidate_names = json_terms['candidates'][debate_party].keys()
    candidate_names.append( 'general' )        

    for candidate in candidate_names:
        candidate_dict[candidate] =  {'party':debate_party if candidate is not 'general' else 'general',
                                      'batchtime':'',
                                      'num_tweets':'0',
                                      'sentiment_avg':'',
                                      'sentiment_std':'',
                                      'highest_sentiment_tweet':'',
                                      'lowest_sentiment_tweet':''
                                     }

    # default settings remove words scored 4-6 on the scale (too neutral). 
    # adjust with kwarg stopval, determines 'ignore spread' out from 5. eg. default stopval = 1.0 (4-6)
    labMT = emotionFileReader() 

    # Get the singleton instance of SQLContext
    sqlContext = getSqlContextInstance(rdd.context)

    schema = StructType([StructField("batchtime",       StringType()           ),
                         StructField("first_term",      StringType()           ),
                         StructField("hashtags",        ArrayType(StringType())),
                         StructField("id",              IntegerType()          ),
                         StructField("multiple_terms",  BooleanType()          ),
                         StructField("search_terms",    ArrayType(StringType())),
                         StructField("text",            StringType()           ),
                         StructField("timestamp",       StringType()           ),
                         StructField("username",        StringType()           )
                        ]
                       )
    # Convert RDD[String] to RDD[Row] to DataFrame
    row_rdd = rdd.map(lambda data: make_row(data))
    df = sqlContext.createDataFrame(row_rdd, schema)

    # how many tweets per candidate per batch?
    df2 = (df.groupBy("first_term")
             .count()
             .alias('df2')
          )

    counts = (df2.map(lambda row: row.asDict() )
                 .map(lambda row: (row['first_term'],row['count']))
              )
    #print 'counts collect'
    #print counts.collect()

    cRdd = rdd.context.parallelize( candidate_names, n_parts )

    def update_dict(d):
        data = d[0]
        data['num_tweets'] = str(d[1]) if d[1] is not None else data['num_tweets']
        return data 

    tmp = (cRdd.map( lambda c: (c, candidate_dict[c]), preservesPartitioning=True )
               .leftOuterJoin( counts, numPartitions=n_parts )
               .map( lambda data: (data[0], update_dict(data[1])) )
               .collect()
          )
    candidate_dict = { k:v for k,v in tmp }

    # Register as table
    df.registerTempTable("tweets")
    # loop over candidates, check if tweet mentions each candidate
    for candidate in candidate_names:
        print
        print 'CANDIDATE NAME:'
        print candidate 
        print
        try:
            accum = rdd.context.accumulator(0)

            query = "SELECT batchtime, text FROM tweets WHERE first_term='{}'".format(candidate)

            result = sqlContext.sql(query)

            scored = result.map( lambda x: (x.batchtime, (emotion(x.text,labMT), x.text)) ).cache()


            scored.foreach(lambda x: accum.add(1))

            batchtime = scored.first()[0]


            if accum.value > 0:
                accum2 = rdd.context.accumulator(0)

                scored = scored.filter(lambda score: score[1][0][0] is not None).cache()
           
            scored.foreach(lambda x: accum2.add(1))
            if accum2.value > 1: # we want at least 2 tweets for highest and lowest scoring
  
                high_parts = scored.takeOrdered(1, key = lambda x: -x[1][0][0])[0][1]
                high_scores, high_tweet = high_parts
                
                high_avg = str(high_scores[0])
                high_tweet = high_tweet.encode('utf8').decode('ascii','ignore')
                
                low_parts  = scored.takeOrdered(1, key = lambda x:  x[1][0][0])[0][1]
                
                low_scores, low_tweet = low_parts
                low_avg = str(low_scores[0])
                low_tweet = low_tweet.encode('utf8').decode('ascii','ignore')

            else:
                high_avg = low_avg = high_tweet = low_tweet = ''


            candidate_dict[candidate]['highest_sentiment_tweet'] = '_'.join([high_avg,high_tweet])
            candidate_dict[candidate]['lowest_sentiment_tweet']  = '_'.join([low_avg,low_tweet])  

            sentiment = (result.map(lambda x: (1,x.text))
                                    .reduceByKey(lambda x,y: ' '.join([str(x),str(y)]))
                                    .map( lambda text: emotion(text[1],labMT) )
                                    .collect()
                             )


            sentiment_avg, sentiment_std = sentiment[0]


            candidate_dict[candidate]['sentiment_avg'] = str(sentiment_avg)
            candidate_dict[candidate]['sentiment_std'] = str(sentiment_std) 
            candidate_dict[candidate]['batchtime'] = batchtime

        except Exception,e:
            print "Looks like this candidate doesn't have any data"
            print str(e)
            continue


    import boto3,json
    client = boto3.client('sdb')

    for cname,cdata in candidate_dict.items():
        attrs = []
        attrs.append( {'Name':"data",'Value':json.dumps(candidate_dict[cname]),'Replace':False} )
        attrs.append( {"Name":"timestamp", "Value": batchtime, "Replace":False}  )
        attrs.append( {"Name":"candidate", "Value": cname, "Replace":False}  )

        item_name = '_'.join([cname,batchtime])

        print 
        print "We are ready to store in db"
        print item_name
        #print attrs 

        try:
            # write row of data to SDB
            client.put_attributes(
                DomainName= domain_name,
                ItemName  = item_name,
                Attributes= attrs 
            ) 
        except Exception,e:
            print 'sdb write error: {}'.format(str(e))

        #rdd.foreachPartition(lambda p: write_to_db(p,level='group'))
    #except Exception, e:
    #    print 
    #    print 'THERE IS AN ERROR!!!!'
    #    print str(e)
    #    print
    #    pass




# From Thouis 'Ray' Jones CS205
def quiet_logs(sc):
    ''' Shuts down log printouts during execution '''
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.WARN)
    logger.LogManager.getLogger("amazonaws").setLevel(logger.Level.WARN)



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
    else:
        minute += 2

    return {"year":year,"month":month,"day":day,"hour":hour,"minute":minute}


# from docs: http://spark.apache.org/docs/latest/streaming-programming-guide.html#dataframe-and-sql-operations
def getSqlContextInstance(sparkContext):
    ''' Lazily instantiated global instance of SQLContext '''
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']


def write_to_db(iterator,level='tweet',domain_name='tweets'):
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
    import boto3            # keep local boto import!
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
                # If v2 is BOOL: convert to string
                elif isinstance(v2,bool):
                    v2 = str(v2)
                # If v2 IS EMPTY: convert to empty string
                elif v2 is None:
                    v2 = ''
                # Get rid of all UTF-8 weirdness, including emojis.
                if k2 != "batchtime":
                    v2 = v2.encode('utf8').decode('ascii','ignore')
                attrs.append( {'Name':k2,'Value':v2,'Replace':False} )
        except Exception, e:
            print 'This error is from write_to_db'
            print str(e)
            print v 
        try:
            # write row of data to SDB
            client.put_attributes(
                DomainName= domain_name,
                ItemName  = str(k),
                Attributes= attrs 
            ) 
        except Exception, e:
            print "This error is from write_to_db"
            print str(e)
            print attrs
            print


