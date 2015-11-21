#!/bin/bash

(/usr/bin/spark-submit --jars /home/hadoop/spark-streaming-kafka-assembly_2.10-1.5.2.jar /home/hadoop/scripts/spark-output.py &)
(python /home/hadoop/scripts/twitter-in.py &)

