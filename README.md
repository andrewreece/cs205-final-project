## Gauging Debate: Tracking Sentiment for the 2016 US Elections
### Authors: Daniel Rajchwald, Andrew Reece

[GaugingDebate.com](http://gaugingdebate.com)   
[Intro video (2 min)](https://www.youtube.com/watch?v=Vj6o-z_ekT8)

####Introduction   
This is the codebase for a real-time Twitter sentiment tracker, focusing on the 2016 US presidential candidates. It provides live and historical analysis for the entire 2016 electoral cycle.  

This work is the authors' final project for [CS205](http://cs205.org), Fall semester 2015, at Harvard University.   
This course, titled "Computing Foundations for Computational Science", emphasized the use of parallelism for making things scalable and efficient.  In part, this work is an exercise in building proficiency with [Apache Spark](http://spark.apache.org/) as a scalable distributed computing platform.  In particular, we utilized [Spark's streaming capabilities](http://spark.apache.org/docs/latest/streaming-programming-guide.html) to process incoming Twitter data in realtime. 

All content is licensed under the MIT Open Source License (see below).


####For CS205 Graders  
See [our final report](https://docs.google.com/document/d/14FZ1wTJc4o78O6IW_lG1xerzGrCdHfYXJ8xAQKVSR0w/edit?usp=sharing).  You can also review [our process journal](https://docs.google.com/document/d/1ncgcKObu8FmFr2-T6JLUhg-GArKaeCCcC7qfIMB1dbc/edit?usp=sharing) for all the step-by-step gory details.  
The app normally runs on a streaming AWS cluster, and we will set up a cron job for it to run automatically when actual debates take place.  In the meantime, we will keep a local Streaming Spark instance running for the next few days while grading takes place.   
<b style="color:red;">Important!</b> The web app automatically detects whether a cluster is currently serving content.  There will likely not be a cluster running when you are reviewing our project for grading, as it's too costly for us to keep an on-demand dedicated cluster running.  That means if you click the Live Stream option on the website, it will tell you no cluster is found.  We've provided a button "Try Tracking Anyway" - click this and the app will read off of the data being stored via our local instance.  We'll do our best to keep the local instance up and running for the next few days, although we may need to shut it down from time to time for machine memory constraints.  
If you want to run a local instance yourself, follow the instructions in the final report.
  
####Data Pipeline  
Streaming data travels across several components in order to get from the raw Twitter stream to the app's web interface.  This is a rough diagram of how it happens, more detail below:  
  
#####    Twitter stream -> Kafka -> Spark Streaming -> Spark SQL -> SimpleDB -> Flask -> Web

<b>Twitter stream</b>  
We can access the Twitter stream through the Twitter developer's API, which provides free access to a small portion of the entire stream of tweets.  Since we were only attempting to acquire a small portion of all tweets anyway (ie. only candidate- or debate-related tweets), the app collects most (but not all) of its target tweets with this free access tier.

<b>Kafka</b>  
[Apache Kafka](http://kafka.apache.org/) is a distributed publish-subscribe messaging system that is built to work with the rest of the Apache ecosystem. It serves as a broker for incoming streams of data, and for outgoing requests. Spark Streaming has a native Kafka connector.  (Actually, Spark's Java and Scala versions have native connectors for the Twitter stream, but we developed in PySpark, which does not yet have this feature.)  

<b>Spark Streaming</b>  
[Spark Streaming](http://spark.apache.org/docs/latest/streaming-programming-guide.html) works more-or-less like normal Spark.  The main abstraction is the "DStream", but with a few I/O exceptions you can basically treat these like regular RDDs. There is a start, await, and exit sequence that is set to tell the stream when to open and close, and otherwise it's Spark as usual.  

<b>Spark SQL</b>  
[Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html) offers Pandas-y data frames and Hive query functionality on RDDs.  This is nice for conducting groupby operations when groupByKey() is not feasible.  In our case, it came in handy for grouping data by both timestamp and candidates.  It's worth noting that aggregate functions for groupby objects are still quite primitive, and (at least in PySpark) don't yet offer the degree of customization for aggregating functions that you might expect from, say, Pandas.  In fact, we used Pandas for analyzing the historical data, as it was much easier to get the data in the shape we needed.  

<b>Simple DB</b>  
Even though the goal was to stream live analytics, we still wanted the ability to (a) keep a buffer of recent past analysis, and (b) make it easy to access both live and historical data. This, on top of the fact that we're not Node.js experts, led us to a storage-based solution, wherein stream data is written to a database, and the front end then queries the most recent records for display.  Simple static databases don't work well with parallelized writes, so platforms like MySQL and SQLite were unavailable to us. Amazon offers a number of database options, and we chose [Simple DB](https://github.com/boto/boto3), a schema-less key-value storage system - mainly because it had a low learning overhead, and we didn't need very sophisticated querying.  SimpleDB can handle concurrent reads and writes, and we can interface with it in Python through [Boto](https://github.com/boto/boto3), which is a truly excellent module.

<b>Flask</b>
[Flask](http://flask.pocoo.org/) is a Python framework for serving web content. We used it to do all the heavy lifting between the backend and the web interface. It works in conjunction with [Jinja2 templating](http://flask.pocoo.org/).  Just about anything that isn't boilerplate on the website is served through Flask in one way or another.  

<b>Web</b>
The front-end of [Gauging Debate](http://gaugingdebate.com) relies heavily on a handful of Javascript libraries: [jQuery](http://oboejs.com/), [D3](http://oboejs.com/), [Oboe](http://oboejs.com/), and [Plotly](https://plot.ly/javascript). jQuery and D3 are probably familiar to most readers.  <b>Oboe</b> is a great little library that collects large data requests in little bits, kind of like a pseudo-stream. We used this for the historical debate charts - the entire ~3 hours of debate data is a lot to load all at once, so we use D3 to load the first few minutes and render quickly, and in the background Oboe loads the rest.  When it finishes loading, the chart is refreshed with the full dataset.  <b>Plotly</b> is a charting library built on top of D3, which was just recently (as of Dec 2015) open-sourced.  It's great.  We initially started with [Rickshaw](http://code.shutterstock.com/rickshaw/), but that project is dead and wading through highly idiosyncratic source code with little documentation proved to be a terrible idea.  

The main feature of the front-end is a chart of average tweet sentiment, per candidate, updated once every 30 seconds. (More on that time interval below in Analysis.)  The chart has a lot of built-in customization, including panning and zooming on both axes, error bars, and the ability to add or remove candidates. You can also save any chart view as an image file for download.  (We can't take credit for these great features, that's all Plotly.)  

The web interface allows users to choose either streaming or historical analysis.  In order for streaming data to appear, there needs to be either an AWS cluster running which is processing realtime data, or a Spark instance on someone's local computer which is doing the same.  

There is also an administrator dashboard which allows admins to start up Spark clusters for streaming functionality. The address of this dashboard is not public - if you're on the CS205 staff you should have received this address in an email.  

<b>S3</b>  
This isn't part of the data pipeline, per se, but we ended up storing almost all our configurations, settings, credentials, and scripts on [S3](https://aws.amazon.com/s3/).  This made it easy for us not to worry about file paths when switching between local and cluster instances, and it interfaces well with the AWS ecosystem.  Most, if not all, of the configuration files we use are not hosted here on GitHub, but are on S3 instead.

####Analysis  
This software analyzes tweets related to the 2016 US Presidential Debates.   
It gauges the sentiment (ie. level of happiness) towards each candidate, and towards the election in general.  Sentiment is analyzed with unigram (word-by-word) averaging, using the [LabMT sentiment dictionary](http://journals.plos.org/plosone/article?id=10.1371/journal.pone.0026752).  Unigram analysis is a relatively crude take on analyzing sentiment, as it is largely context-ignorant.  As such, you need to have a decent chunk of words (at least 1,000) before you can start to be confident that their averaged value is giving a reliable signal.  Based on analysis of past debates, we found that each candidate gets enough tweets to meet this limit every 30-60 seconds.  (The less popular candidates take longer than that.) Fast and frequent updates were also a priority here, as the whole point of a streaming analytics engine is that it continuously delivers content.  
Taking all this into account, we decided to offer updated analysis in 30-second intervals.  That means every 30 seconds, the chart on [GaugingDebate.com](http://gaugingdebate.com) adds new sentiment scores.

Topical content is determined using [a parallelized adaptation](http://www.datalab.uci.edu/papers/distributed_topic_modeling.pdf) of [Latent Dirichlet Allocation](http://machinelearning.wustl.edu/mlpapers/paper_files/BleiNJ03.pdf).  LDA is, if anything, more demanding than unigram sentiment analysis, in terms of the amount of content it needs to provide stable results. We discovered over the course of our work that it doesn't really work to run this algorithm in a streaming context, as it requires both (a) many words per document and (b) many separate documents. We tried clumping all tweets per candidate together as documents (many words per document, few documents), as well as treating each tweet as a document (few words per document, many documents). Neither one gave very satisfactory results, so, in the end, we provided the code (which works for both static and streaming Spark), but we removed it from the app itself.  See [our LDA report](https://docs.google.com/document/d/1L2Li_40lXpNFRJbz40idSyCXfB6yQdNHaa_MrdW0XWE/edit?usp=sharing) for details on implementation and results.  
  
####File Tree
Files are categorized as either site/ and streaming/   

├── site    
    ├── \_\_init\_\_.py  
    ├── baker.py  
    ├── nocache.py  
    ├── passenger_wsgi.py  
    ├── run.py  
    ├── static  
        ├── css  
            ├── admini.css  
            └── main.css  
        ├── images  
            └── twitter-flag.png  
        └── js  
            ├── admini.js  
            ├── instance-types.txt  
            ├── main.js  
            └── oboe-browser.min.js  
    ├── templates  
        ├── admini.html  
        ├── experimental.html  
        └── index.html  
    └── utils.py  
└── streaming  
    
    ├── bootstrap\_actions   
        ├── install-basics.sh  
        ├── install-kafka.sh  
        ├── install-zookeeper.sh  
        ├── server.properties.aws  
        ├── start-kafka-server.sh  
        └── start-kafka-topic.sh  
    └── jobs  
        ├── __init__.py  
        ├── creds.py  
        ├── jars  
            └── spark-streaming-kafka-assembly_2.10-1.5.2.jar  
        ├── run-main.sh  
        ├── sentiment.py  
        ├── spark-output.py  
        ├── twitter-in.py  
        └── utils.py
        
        
    
    
    

MIT Open Source License  
Copyright (c) 2015 Daniel Rajchwald, Andrew Reece  

Permission is hereby granted, free of charge, to any person obtaining a copy  
of this software and associated documentation files (the "Software"), to deal  
in the Software without restriction, including without limitation the rights  
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell  
copies of the Software, and to permit persons to whom the Software is  
furnished to do so, subject to the following conditions:  

    Cite the authors (Andrew Reece, Daniel Rajchwald) when adapting code for non-commerical applications.
    
The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.