## Gauging Debate: Tracking Sentiment for the 2016 US Elections
### Authors: Daniel Rajchwald, Andrew Reece

[GaugingDebate.com](http://gaugingdebate.com/)   
[Intro video (2 min)](https://www.youtube.com/watch?v=Vj6o-z_ekT8)
[Full report](http://gaugingdebate.com/about)  


####Introduction   
This is the codebase for a real-time Twitter sentiment tracker application called Gauging Debate.  The app focuses on the 2016 US presidential candidates. Currently in beta (as of Dec 2015), the goal of the app is to provide both live-streaming and historical analysis for the entire 2016 electoral cycle.  

This work is the authors' final project for [CS205](http://cs205.org), Fall semester 2015, at Harvard University.   
This course, titled "Computing Foundations for Computational Science", emphasized the use of parallelism for making things scalable and efficient.  In part, this work is an exercise in building proficiency with [Apache Spark](http://spark.apache.org/) as a scalable distributed computing platform.  In particular, we utilized [Spark's streaming capabilities](http://spark.apache.org/docs/latest/streaming-programming-guide.html) to process incoming Twitter data in realtime.  For more information, see our [full report](http://gaugingdebate.com/about).

All content is provided under the MIT Open Source License (see below).

  
####Directory Tree
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