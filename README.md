## Gauging Debate: Tracking Sentiment for the 2016 US Elections
### Authors: Daniel Rajchwald, Andrew Reece

See <a href="https://docs.google.com/document/d/1ncgcKObu8FmFr2-T6JLUhg-GArKaeCCcC7qfIMB1dbc/edit?usp=sharing">process book</a> for updates.
  
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