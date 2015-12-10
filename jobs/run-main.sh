#!/bin/bash

# start spark consumer of kafka tweet feed
(/usr/bin/spark-submit --jars /home/hadoop/spark-streaming-kafka-assembly_2.10-1.5.2.jar /home/hadoop/scripts/spark-output.py &)

# start kafka ingestion of twitter streaming API
(python /home/hadoop/scripts/twitter-in.py &)

