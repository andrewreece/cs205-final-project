import boto3
import numpy as np 
from datetime import datetime, timedelta
from collections import OrderedDict

ec2client = boto3.client('ec2')
emrclient = boto3.client('emr')

EC2_KEY_NAME   = 'cs205'
RELEASE_LABEL  = 'emr-4.1.0'
HADOOP_VERSION = '2.6.0'
#SPARK_VERSION  = '1.5.0'
INSTANCE_TYPES  = OrderedDict() # ordered so we can get master zone first
INSTANCE_TYPES['MASTER'] = "m1.xlarge"
INSTANCE_TYPES['CORE'] = "m1.medium"
LOWEST_BID	   = 0.02 # minimum spot price bid

max_results = 20
start_time  = datetime.now() - timedelta(hours=1)

''' IMPORTANT NOTES ABOUT YOUR CONFIGURATION: 

	- As of 18 NOV you have master and core nodes running on spot pricing. 
	  Change this so that at least master is on-demand when you go live.

	- All nodes are of type "m1.medium" as of 18 NOV.

	- If you want to have different instance types running at different levels of your cluster,
 		you'll need to build in calculation of multiple spot averages.  
		(Just add to the list of instance types and divvy up the math accordingly)
'''

spots = ec2client.describe_spot_price_history(InstanceTypes=INSTANCE_TYPES.values(), StartTime=start_time, MaxResults=max_results)
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

for ilevel,itype in INSTANCE_TYPES.items():
	if ilevel == "MASTER":
		for zone in zones:
			prices = [float(x['SpotPrice']) for x in spots['SpotPriceHistory'] if x['AvailabilityZone']==zone and x['InstanceType'] == itype]
			avgp = np.mean(prices) if len(prices) else np.inf
			if avgp < best[ilevel]['price']:
				best['MASTER']['zone'] = zone
				best['CORE']['zone'] = zone
				best[ilevel]['price'] = round(avgp,3)
				best[ilevel]['bid'] = round(best[ilevel]['price']*1.2,3) if best[ilevel]['price']*1.2 >= LOWEST_BID else LOWEST_BID
		print "Best bid for {} ({}) = {}: {}".format(ilevel,itype,best[ilevel]['zone'],best[ilevel]['bid'])
	else:
		prices = [float(x['SpotPrice']) for x in spots['SpotPriceHistory'] if x['AvailabilityZone']==best['MASTER']['zone'] and x['InstanceType'] == itype]
		avgp = np.mean(prices)
		best[ilevel]['price'] = round(avgp,3)
		best[ilevel]['bid'] = round(best[ilevel]['price']*1.2,3) if best[ilevel]['price']*1.2 >= LOWEST_BID else LOWEST_BID
		print "Best bid for {} ({}) = {}: {}".format(ilevel,itype,best[ilevel]['zone'],best[ilevel]['bid'])

apps = [
			{
	            'Name': 'spark',
	            #'Version': SPARK_VERSION
        	},
        	{
	            'Name': 'hadoop',
	            #'Version': HADOOP_VERSION
        	}
        ]

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
				  'Name':'Upgrade yum, python, pip, and install boto3, awscli',
				  'ScriptBootstrapAction': {
				  		'Path':'s3://cs205-final-project/setup/startup/upgrades.sh'
				  }
				},
				{
				  'Name':'Start up Zookeeper',
				  'ScriptBootstrapAction': {
				  		'Path':'s3://cs205-final-project/setup/startup/zookeeper.sh'
				  }
				},
				{
				  'Name':'Install Kafka',
				  'ScriptBootstrapAction': {
				  		'Path':'s3://cs205-final-project/setup/startup/kafka.sh'
				  }
				},
				#{
				#  'Name':'Start Kafka server',
				#  'ScriptBootstrapAction': {
				#  		'Path':'s3://cs205-final-project/setup/startup/kafka-start.sh'
				#  }
				#},
				#{
				#  'Name':'Start Kafka topic "tweets"',
				#  'ScriptBootstrapAction': {
				#  		'Path':'s3://cs205-final-project/setup/startup/kafka-topic.sh'
				#  }
				#}
			 ]

steps = [
	        {
	            'Name': 'Start Kafka server',
	            'ActionOnFailure': 'TERMINATE_CLUSTER',
	            'HadoopJarStep': {
	                'Jar': 'command-runner.jar',
	                'Args':['/home/hadoop/startup/kafka-start.sh']
	            }
	        },
	        {
	            'Name': 'Start Kafka topic "tweets"',
	            'ActionOnFailure': 'TERMINATE_CLUSTER',
	            'HadoopJarStep': {
	                'Jar': 'command-runner.jar',
	                'Args':['/home/hadoop/startup/kafka-topic.sh']
	            }
	        }
		]


response = emrclient.run_job_flow(
									Name='no sudo',
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
