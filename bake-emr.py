import boto3
import numpy as np 
from datetime import datetime, timedelta

ec2client = boto3.client('ec2')
emrclient = boto3.client('emr')

EC2_KEY_NAME   = 'cs205'
RELEASE_LABEL  = 'emr-4.1.0'
HADOOP_VERSION = '2.6.0'
#SPARK_VERSION  = '1.5.0'
INSTANCE_TYPE  = "m1.medium"
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

spots = ec2client.describe_spot_price_history(InstanceTypes=[INSTANCE_TYPE], StartTime=start_time, MaxResults=max_results)
zones = ['us-east-'+z for z in ['1a','1b','1c','1d','1e']]
best  = {'zone':'','price':np.inf}

for zone in zones:
	prices = [float(x['SpotPrice']) for x in spots['SpotPriceHistory'] if x['AvailabilityZone']==zone]
	avgp = np.mean(prices) if len(prices) else np.inf
	if avgp < best['price']:
		best['zone'] = zone
		best['price'] = round(avgp,3)
		best['bid'] = round(best['price']*1.2,3) if best['price']*1.2 >= LOWEST_BID else LOWEST_BID

print "Best bid for {}: {}".format(best['zone'],best['bid'])
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
					    	'InstanceType':INSTANCE_TYPE,
					    	'Market':"SPOT",
					    	'BidPrice':str(best['bid']),
					    	'Name':"Spot Main node"
					    },
						{	# core
					    	'InstanceCount':2,
					    	'InstanceRole':"CORE",
					    	'InstanceType':INSTANCE_TYPE,
					    	'Market':"SPOT",
					    	'BidPrice':str(best['bid']),
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
				  'Name':'Start up Kafka',
				  'ScriptBootstrapAction': {
				  		'Path':'s3://cs205-final-project/setup/startup/kafka.sh'
				  }
				},
				{
				  'Name':'Start Kafka topic "tweets"',
				  'ScriptBootstrapAction': {
				  		'Path':'s3://cs205-final-project/setup/startup/kafka-topic.sh'
				  }
				}
			 ]

'''steps = [
	        {
	            'Name': 'Start Kafka topic "tweets"',
	            'ActionOnFailure': 'TERMINATE_CLUSTER',
	            'HadoopJarStep': {
	                'Jar': 's3://cs205-final-project/setup/startup/kafka-topic.sh'
	            }
	        }
		]
'''

response = emrclient.run_job_flow(
									Name='agr-test-cluster',
									LogUri='s3://cs205-final-project/logs/emr/',
									ReleaseLabel=RELEASE_LABEL,
									Instances={
												'InstanceGroups':instance_groups,
												'Ec2KeyName':EC2_KEY_NAME,
												'Placement': { 'AvailabilityZone': best['zone'] },
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
									#Steps=steps
								 )

cluster_id = response['JobFlowId']

print "Starting cluster", cluster_id

status = emrclient.describe_cluster(ClusterId=cluster_id)
print "Cluster status", status

#conn.terminate_jobflow(cluster_id)
#status = conn.describe_jobflow(cluster_id)
#print "Cluster status", status
#print "Cluster terminated"
