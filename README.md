## Gauging Debate: Tracking Sentiment for the 2016 US Elections
### Authors: Daniel Rajchwald, Andrew Reece

####Website  
[GaugingDebate.com](http://gaugingdebate.com)   

####Introduction   
This is the codebase for a real-time Twitter sentiment tracker that focuses on US presidential candidates. It's designed to provide live and historical analysis for the 2016 electoral cycle.  

This work is the authors' final project for [CS205](http://cs205.org), Fall semester 2015, at Harvard University.  
All content is licensed under the MIT Open Source License (see below).

Watch [the video](https://www.youtube.com/watch?v=Vj6o-z_ekT8) for a broad overview of our motivation and design.  

####For Graders  
See [the project summary] and user manual.  You can also view [our process journal](https://docs.google.com/document/d/1ncgcKObu8FmFr2-T6JLUhg-GArKaeCCcC7qfIMB1dbc/edit?usp=sharing">process book) for all the gory details.
  
####The Big Picture  
The idea is to have a real-time sentiment tracker that can give immediate feedback on how people are feeling during a live debate.  

This software analyzes tweets related to the 2016 US Presidential Debates (currently ongoing at the time of this writing).   It gauges the sentiment (ie. level of happiness) towards each candidate, and towards the election in general.  Sentiment is analyzed with unigram (word-by-word) averaging, using the [LabMT sentiment dictionary](http://journals.plos.org/plosone/article?id=10.1371/journal.pone.0026752).  It also determines the topics being discussed, using [a parallelized adaptation](http://www.datalab.uci.edu/papers/distributed_topic_modeling.pdf) of [Latent Dirichlet Allocation](http://machinelearning.wustl.edu/mlpapers/paper_files/BleiNJ03.pdf).  
  
<u>File Tree</u>  
.  
├── site    
│   ├── __init__.py  
│   ├── baker.py  
│   ├── nocache.py  
│   ├── passenger_wsgi.py  
│   ├── run.py  
│   ├── static  
│   │   ├── css  
│   │   │   ├── admini.css  
│   │   │   └── main.css  
│   │   ├── images  
│   │   │   └── twitter-flag.png  
│   │   └── js  
│   │       ├── admini.js  
│   │       ├── instance-types.txt  
│   │       ├── main.js  
│   │       └── oboe-browser.min.js  
│   ├── templates  
│   │   ├── admini.html  
│   │   ├── experimental.html  
│   │   └── index.html  
│   └── utils.py  
└── streaming  
    ├── bootstrap_actions  
    │   ├── install-basics.sh  
    │   ├── install-kafka.sh  
    │   ├── install-zookeeper.sh  
    │   ├── server.properties.aws  
    │   ├── start-kafka-server.sh  
    │   └── start-kafka-topic.sh  
    └── jobs  
        ├── __init__.py  
        ├── creds.py  
        ├── jars  
        │   └── spark-streaming-kafka-assembly_2.10-1.5.2.jar  
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