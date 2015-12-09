''' 
	File:  		baker.py

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

''' Notes on bake_emr default kwargs:
	
	As of 08 DEC 2015:
		c3.xlarge EMR $0.053/hr and EC2 $0.21/hr. 
		m1.xlarge EMR $0.088/hr and EC2 $0.35/hr! c3.xlarge is cheaper! (Also cheaper than m3.xlarge).
		That's why we have it set as the master type default.  Core type m1.medium is $0.02/$0.087.)
		https://aws.amazon.com/elasticmapreduce/pricing/

	You generated the master and slave security groups, somehow.  They seem to work indefinitely.
'''
def bake_emr(MASTER_TYPE='c3.xlarge', 
			 CORE_TYPE='m1.medium',
			 STREAM_DURATION=30,
			 EC2_KEY_NAME='cs205',
			 RELEASE_LABEL='emr-4.1.0',
			 HADOOP_VERSION='2.6.0',
			 MASTER_SECURITY_GROUP='sg-d33b7cb8',
			 SLAVE_SECURITY_GROUP='sg-d13b7cba',
			 JOB_NAME='gauging_debate'):

	ec2client = boto3.client('ec2')
	emrclient = boto3.client('emr')
	s3res     = boto3.resource('s3')

	# how long should the stream stay open and read tweet data? we store this value on s3 for twitter-in.py
	s3res.Object('cs205-final-project','setup/stream_duration.txt').put(Body=str(STREAM_DURATION))

	INSTANCE_TYPES  		 = OrderedDict() 	# Use ordered dict to get master zone first (see below)
	INSTANCE_TYPES['MASTER'] = MASTER_TYPE	# Master node instance type
	INSTANCE_TYPES['CORE'] 	 = CORE_TYPE	# Core node instance type

	''' IMPORTANT NOTES ABOUT YOUR CONFIGURATION: 

		- As of 18 NOV you have master and core nodes running on spot pricing. 
		  Change this so that at least master is on-demand when you go live.
		  Or setup auto-scaling to handle all this.
	'''


	def get_bid_details(best,level,spots,itypes,zone,bid_multiplier=1.2,lowest_bid=0.015,digits=3):
		prices = [float(x['SpotPrice']) for x in spots['SpotPriceHistory'] if x['AvailabilityZone']==zone and x['InstanceType'] == itypes[level]]
		avg_price = round(np.mean(prices),digits) if len(prices) else np.inf 
		proposed_bid = round(avg_price*bid_multiplier,digits)
		if proposed_bid < lowest_bid:
			proposed_bid = lowest_bid
		return avg_price, proposed_bid

	def find_best_spot_price(ec2,itypes,lowest_bid=0.02,hours_back=3,max_results=20):
		''' Determines best spot price for given instance types in cluster.

			How it works:
				1. Determine how far back in time to look to collect average price. Set by hours_back, default 1hr.
				2. Get N=max_results spot prices with describe_spot_price_history
				3. Initialize best dict, with keys for each instance type
				4. Loop over possible zones in us-east-1 and find the best average price in each one 
					- We do this with the master node first, because the entire cluster has to be in one zone.
						(Since the master node will likely be the most expensive, we choose the zone based on its best price.
						 Note that we could also argue that the greatest price could be incurred by a large number of workers,
						 so if you end up with a lot of CORE or TASK nodes, it might be best to change this logic.)
					- Determine best average price for each node type in the best zone 
					- Most of this work is done by the helper function get_big_details() 
				5. Compute our spot price bid per instance at 1.2x of the best average price

		'''
		start_time  = datetime.now() - timedelta(hours=hours_back)

		spots = ec2.describe_spot_price_history(InstanceTypes=itypes.values(), StartTime=start_time, MaxResults=max_results)
		zones = ['us-east-'+z for z in ['1a','1b','1c','1d','1e']]
		best  = {
			  'MASTER':
					{ 'zone':'',
					  'price':np.inf,
					  'bid':lowest_bid
					},
			  'CORE':
					{ 'zone':'',
					  'price':np.inf,
					  'bid':lowest_bid
					}
			}
		for zone in zones:
			avgp, new_bid = get_bid_details(best, 'MASTER', spots, INSTANCE_TYPES, zone)

			if avgp < best['MASTER']['price']:
				best['MASTER']['zone'] = zone
				best['MASTER']['price'] = avgp
				best['MASTER']['bid'] = new_bid

		print "Best bid for MASTER ({}) = {}: {}".format(INSTANCE_TYPES['MASTER'],best['MASTER']['zone'],best['MASTER']['bid'])
		
		best['CORE']['zone'] = best['MASTER']['zone']
		avgp, new_bid = get_bid_details(best, 'CORE', spots, INSTANCE_TYPES, best['CORE']['zone'])
		best['CORE']['price'] = avgp
		best['CORE']['bid'] = new_bid if (new_bid < np.inf) else lowest_bid

		print "Best bid for CORE ({}) = {}: {}".format(INSTANCE_TYPES['CORE'],best['CORE']['zone'],best['CORE']['bid'])
		
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

	# how many nodes in total? (for now we use 3)
	instance_count = sum([x['InstanceCount'] for x in instance_groups])

	''' Bootstrap actions are carried out on all nodes in a cluster.  They're usually for software installs.
		We have 4 bootstrap scripts, all load from s3: 
			- install-basics
				* this makes upgrades & installations to core python version and certain modules
			- install-zookeeper
				* installs zookeeper
				* starts a zookeeper service on the master node 
			- install-kafka
				* installs kafka
			- start-kafka-server
				* starts a karfka server (no producers or consumer or topic yet)
				* uses (&) notation (see process book for more) in order to make it run in the background
				* if we don't run it in the background, it hangs the bootstrap process
	'''
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
			}
		]

	''' Two jobs run after bootstrapping.
		1. Start Kafka topic (currently named "tweets")
			- See process book for more on topics, they're kind of like tables in a database.
		2. Run actual data pipeline
			- Opens Twitter->Kafka stream, then Spark ingests Kafka, does analysis, and writes to db.
			- Two scripts inside run-main.sh: 
				* twitter-in.py
				* spark-output.py 
	'''
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
		        }
			]


	''' Boto3 has an EMR client, the run_job_flow() method instantiates an EMR cluster.
		Lots of configurations, see Boto3 docs for more.
		NOTE: Both JobFlowRole and ServiceRole are necessary, even if they are only default roles.
			  These roles need to be created with AWSCLI beforehand (I think they're actually files?)
	'''
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
							'EmrManagedMasterSecurityGroup':MASTER_SECURITY_GROUP, #GroupName=ElasticMapReduce-master
							'EmrManagedSlaveSecurityGroup':SLAVE_SECURITY_GROUP, #GroupName=ElasticMapReduce-slave
						   },
						Applications=apps,
						BootstrapActions=bootstraps,
						VisibleToAllUsers=True,
						JobFlowRole="EMR_EC2_DefaultRole",
						ServiceRole="EMR_DefaultRole",
						Steps=steps
					 )

	cluster_id = response['JobFlowId']

	status = emrclient.describe_cluster(ClusterId=cluster_id)
	return status


def terminate_emr(job_id):
	''' Terminates EMR cluster with ID job_id 
		* Cluster does not terminate immediately, but should go into termination process immediately.
		* Check web console to be sure that termination has been initiated. (This code should work fine.)
	'''
	client = boto3.client('emr')
	try:
		client.terminate_job_flows(JobFlowIds=[job_id])
		return "Cluster terminating now."
	except Exception, e:
		return str(e)
		