''' 
	File:  		bake-emr.py

	Author: 	Andrew Reece
	Github:		andrewreece
	Email:		reece@g.harvard.edu 

	Context: 	Debate Tracker Project for CS205 (Harvard University)
	Created:	Nov 2015

	This script uses the boto3 module to create an EMR cluster on AWS, with Spark 1.5.0 installed.
	
	It is designed to be run on a local machine with Python 2.7+ and Boto3 installed.

	It includes functionality for finding the AWS zone (eg. us-east-1a) with the lowest average spot price
	for a given instance type, which it then uses as a basis to submit a spot price bid for each node.


	Apps installed:

		Hadoop
		Spark 
		* Note: With AWS Release Versions (replacing AMIs), you no longer specify the version of software
				you wish to install. You just get whatever version comes with the specified release.
				In this case, we are getting Hadoop 2.6 and Spark 1.5.0.


	Workflow initiated on cluster startup (includes bootstrap and steps):

		** Bootstrap Actions **
		
			1. install-basics.sh
				- Switches AWS Python default to 2.7.9 (out-of-the-box is 2.6)
				- Upgrades and installs necessary Python modules
				- Makes /scripts directory in home folder
				- Uploads spark-kafka .jar file and job scripts to all nodes
				- Sets 744 permissions on all files in /scripts 
			
			2. install-zookeeper.sh 
				- Downloads and installs Zookeeper to master node only
				- Sets Zookeeper config file 
				- Starts Zookeeper on master node 

			3. install-kafka.sh 
				- Downloads and installs Kafka to master node only (we may change to install on all nodes)
				- Replaces default Kafka server config file with custom file from s3 (mainly to set log folder)

			4. start-kafka-server.sh
				- Starts up Kafka server on master node only

		** Step Actions **

			1. start-kafka-topic.sh 
				- Initializes Kafka topic (abstraction for handling a given input stream)

			2. run-twitter-in.sh 
				- Runs twitter-in.py, feeds streaming Twitter API to Kafka producer
				- See documentation for twitter-in.py for more 

	Note: In its current form, it is NOT designed to be modular, ie., to accept custom input for 
	bootstrapping and step actions. It only runs to serve its original purpose, for this project.
'''

import boto3
import numpy as np 
from datetime import datetime, timedelta
from collections import OrderedDict

ec2client = boto3.client('ec2')
emrclient = boto3.client('emr')

EC2_KEY_NAME   = 'cs205' 		# SSH key name
RELEASE_LABEL  = 'emr-4.1.0'		# AWS Release (replaces AMI version)
HADOOP_VERSION = '2.6.0'	
#SPARK_VERSION  = '1.5.0'		# Spark version is defined by Release version
INSTANCE_TYPES  = OrderedDict() 	# Use ordered dict to get master zone first (see below)
INSTANCE_TYPES['MASTER'] = "m1.large"	# Master node instance type
INSTANCE_TYPES['CORE'] = "m1.medium"	# Core node instance type
#INSTANCE_TYPES['TASK'] = "m1.small"	# Task node instance type

''' IMPORTANT NOTES ABOUT YOUR CONFIGURATION: 

	- As of 18 NOV you have master and core nodes running on spot pricing. 
	  Change this so that at least master is on-demand when you go live.
'''


def find_best_spot_price(ec2,itypes,lowest_bid=0.02,hours_back=1,max_results=20):
	''' Determines best spot price for given instance types in cluster.

		How it works:
			- Determine how far back in time to look to collect average price. Set by hours_back, default 1hr.
			- Get N=max_results spot prices with describe_spot_price_history
			- Initialize best dict, with keys for each instance type
			- Loop over possible zones in us-east-1 and find the best average price in each one 
			- We do this with the master node first, because the entire cluster has to be in one zone.
				(Since the master node will likely be the most expensive, we choose the zone based on its best price.
				 Note that we could also argue that the greatest price could be incurred by a large number of workers,
				 so if you end up with a lot of CORE or TASK nodes, it might be best to change this logic.)
			- Determine best average price for each node type in the best zone 
			- Compute our spot price bid per instance at 1.2x of the best average price
	'''
	start_time  = datetime.now() - timedelta(hours=hours_back)

	spots = ec2.describe_spot_price_history(InstanceTypes=itypes.values(), StartTime=start_time, MaxResults=max_results)
	zones = ['us-east-'+z for z in ['1a','1b','1c','1d','1e']]
	best  = {
			  'MASTER':
						{	'zone':'',
							'price':np.inf
						},
			  'CORE':
						{	'zone':'',
							'price':np.inf
						}
			}

	for ilevel,itype in itypes.items():
		if ilevel == "MASTER":
			for zone in zones:
				prices = [float(x['SpotPrice']) for x in spots['SpotPriceHistory'] if x['AvailabilityZone']==zone and x['InstanceType'] == itype]
				avgp = np.mean(prices) if len(prices) else np.inf
				if avgp < best[ilevel]['price']:
					best['MASTER']['zone'] = zone
					best['CORE']['zone'] = zone
					best[ilevel]['price'] = round(avgp,3)
					best[ilevel]['bid'] = round(best[ilevel]['price']*1.2,3) if best[ilevel]['price']*1.2 >= lowest_bid else lowest_bid
			print "Best bid for {} ({}) = {}: {}".format(ilevel,itype,best[ilevel]['zone'],best[ilevel]['bid'])
		else:
			prices = [float(x['SpotPrice']) for x in spots['SpotPriceHistory'] if x['AvailabilityZone']==best['MASTER']['zone'] and x['InstanceType'] == itype]
			avgp = np.mean(prices)
			best[ilevel]['price'] = round(avgp,3)
			best[ilevel]['bid'] = round(best[ilevel]['price']*1.2,3) if best[ilevel]['price']*1.2 >= lowest_bid else lowest_bid
			print "Best bid for {} ({}) = {}: {}".format(ilevel,itype,best[ilevel]['zone'],best[ilevel]['bid'])
	return best 

best = find_best_spot_price(ec2client,INSTANCE_TYPES)

# software to load as part of AWS Release package
app_names = ['spark','hadoop']
apps = [dict(Name=appname) for appname in app_names]

# define instance groups
instance_groups = 	[
						{	# master
					    	'InstanceCount':1,
					    	'InstanceRole':"MASTER",
					    	'InstanceType':INSTANCE_TYPES['MASTER'],
					    	'Market':"SPOT",
					    	'BidPrice':str(best['MASTER']['bid']),
					    	'Name':"Spot Main node"
					    },
						{	# core
					    	'InstanceCount':2,
					    	'InstanceRole':"CORE",
					    	'InstanceType':INSTANCE_TYPES['CORE'],
					    	'Market':"SPOT",
					    	'BidPrice':str(best['CORE']['bid']),
					    	'Name':"Spot Worker node"
					    },
					]

instance_count = sum([x['InstanceCount'] for x in instance_groups])

bootstraps = [
				{
				  'Name':'Upgrade yum, python, pip, and install/upgrade necessary modules',
				  'ScriptBootstrapAction': {
				  		'Path':'s3://cs205-final-project/scripts/install-basics.sh'
				  }
				},
				{
				  'Name':'Start up Zookeeper',
				  'ScriptBootstrapAction': {
				  		'Path':'s3://cs205-final-project/scripts/install-zookeeper.sh'
				  }
				},
				{
				  'Name':'Install Kafka',
				  'ScriptBootstrapAction': {
				  		'Path':'s3://cs205-final-project/scripts/install-kafka.sh'
				  }
				},
				{
				  'Name':'Start Kafka server',
				  'ScriptBootstrapAction': {
				  		'Path':'s3://cs205-final-project/scripts/start-kafka-server.sh'
				  }
				},
				#{
				#  'Name':'Start Kafka topic "tweets"',
				#  'ScriptBootstrapAction': {
				#  		'Path':'s3://cs205-final-project/setup/startup/kafka-topic.sh'
				#  }
				#}
			 ]

steps = [
	        {
	            'Name': 'Start Kafka topic',
	            'ActionOnFailure': 'TERMINATE_CLUSTER',
	            'HadoopJarStep': {
	                'Jar': 'command-runner.jar',
	                'Args':['/home/hadoop/scripts/start-kafka-topic.sh']
	            }
	        },
	        {
	            'Name': 'Run pipeline (from twitter to spark output)',
	            'ActionOnFailure': 'TERMINATE_CLUSTER',
	            'HadoopJarStep': {
	                'Jar': 'command-runner.jar',
	                'Args':['/home/hadoop/scripts/run-main.sh']
	            }
	        },
	        #{
	        #    'Name': 'Run spark script',
	        #    'ActionOnFailure': 'TERMINATE_CLUSTER',
	        #    'HadoopJarStep': {
	        #        'Jar': 'command-runner.jar',
	        #        'Args':['spark-submit', '--jars', '/home/hadoop/spark-streaming-kafka-assembly_2.10-1.5.2.jar', '/home/hadoop/scripts/spark-output.py']
	        #    }
	        #}
		]


JOB_NAME = 'run twitter-in, all kafka setup in bootstrap'
response = emrclient.run_job_flow(
									Name=JOB_NAME,
									LogUri='s3://cs205-final-project/logs/emr/',
									ReleaseLabel=RELEASE_LABEL,
									Instances={
												'InstanceGroups':instance_groups,
												'Ec2KeyName':EC2_KEY_NAME,
												'Placement': { 'AvailabilityZone': best['MASTER']['zone'] },
												'KeepJobFlowAliveWhenNoSteps':True,
												'TerminationProtected':False,
												'HadoopVersion':HADOOP_VERSION,
												'EmrManagedMasterSecurityGroup':'sg-d33b7cb8', #GroupName=ElasticMapReduce-master
												'EmrManagedSlaveSecurityGroup':'sg-d13b7cba', #GroupName=ElasticMapReduce-slave
											   },
									Applications=apps,
									BootstrapActions=bootstraps,
									VisibleToAllUsers=True,
									JobFlowRole="EMR_EC2_DefaultRole",
									ServiceRole="EMR_DefaultRole",
									Steps=steps
								 )

cluster_id = response['JobFlowId']

print "Starting cluster", cluster_id

status = emrclient.describe_cluster(ClusterId=cluster_id)
print "Cluster status", status

#conn.terminate_jobflow(cluster_id)
#status = conn.describe_jobflow(cluster_id)
#print "Cluster status", status
#print "Cluster terminated"
