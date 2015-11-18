import boto3
import numpy as np 
from datetime import datetime, timedelta

instance_type = "m1.small"
max_results = 10
starttime = datetime.now() - timedelta(hours=1)

ec2client = boto3.client('ec2')
spots = ec2client.describe_spot_price_history(InstanceTypes=[instance_type], StartTime=starttime, MaxResults=max_results)
prices = [float(x['SpotPrice']) for x in spots['SpotPriceHistory']]
avg_spot = np.mean(prices)

# NOTE: If you want to have different instance types running at different levels of your cluster,
# 		you'll need to build in calculation of multiple spot averages.  
#		(Just add to the list of instance types and divvy up the math accordingly)

emrclient = boto3.client('emr')

# 

conn = boto.emr.connect_to_region('us-east-1')

# get average spot price for the past hour

instance_groups = []
instance_groups.append(
						{
					    	InstanceCount=1,
					    	InstanceRole="MASTER",
					    	InstanceType=instance_type,
					    	Market="SPOT",
					    	BidPrice=avg_spot,
					    	Name="Spot Main node"
					    },
						{
					    	InstanceCount=2,
					    	InstanceRole="CORE",
					    	InstanceType=instance_type,
					    	Market="SPOT",
					    	BidPrice="0.002",
					    	Name="Spot Worker node"
					    },
					   )
instance_groups.append(InstanceGroup(
    num_instances=2,
    InstanceRole="CORE",
    InstanceType="m1.small",
    Market="SPOT",
    name="Spot Worker nodes",
    BidPrice="0.002"))
'''instance_groups.append(InstanceGroup(
    num_instances=2,
    role="TASK",
    type="m1.small",
    market="SPOT",
    name="My cheap spot nodes",
    bidprice="0.002"))
'''
cluster_id = conn.run_jobflow(
    "Test cluster",
    instance_groups=instance_groups,
    action_on_failure='TERMINATE_JOB_FLOW',
    keep_alive=True,
    enable_debugging=True,
    log_uri="s3://cs205-final-project/logs/",
    hadoop_version=None,
    release_label="2.4.9",
    steps=[],
    bootstrap_actions=[],
    ec2_keyname="my-ec2-key",
    visible_to_all_users=True,
    job_flow_role="EMR_EC2_DefaultRole",
    service_role="EMR_DefaultRole")


print "Starting cluster", cluster_id

status = conn.describe_jobflow(cluster_id)
print "Cluster status", status

conn.terminate_jobflow(cluster_id)

status = conn.describe_jobflow(cluster_id)
print "Cluster status", status

print "Cluster terminated"
