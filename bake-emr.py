import boto
import boto.emr
from boto.emr.instance_group import InstanceGroup

conn = boto.emr.connect_to_region('us-east-1')