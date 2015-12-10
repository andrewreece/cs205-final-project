from flask import Flask, jsonify, request, current_app, render_template, Response
from functools import wraps
from random import randint
from datetime import datetime, timedelta
from collections import OrderedDict
import numpy as np
import time, json, boto3
import baker # this is a library we made with EMR baking functions
import utils # misc helper functions, includes get/set debate schedule
from nocache import nocache # this is a library someone else made that keeps Flask from caching results


## the core flask app
app = Flask(__name__)

bucket_name  = 'cs205-final-project'
settings_key = 'setup/bake-defaults.json'

''' Load web interface index.html '''
@app.route("/")
def template_index():
	try:
		return render_template('index.html')
	except Exception, e:
		return "Error:"+str(e)


''' Load admin interface admini.html '''
@app.route("/admini205")
def template_admini():
	try:
		return render_template('admini.html')
	except Exception, e:
		return "Error:"+str(e)


''' Sets new default cluster-bake settings from /admini205 '''
@app.route("/set_default_bake_settings/<x1>/<x2>/<x3>/<x4>/<x5>/<x6>")
def set_default_bake_settings(x1,x2,x3,x4,x5,x6):
	try:
		s3 = boto3.resource('s3')
		result = s3.Object(bucket_name,settings_key).get()['Body'].read()		
		settings = json.loads(result)
		newsets = [x1,x2,x3,x4,x5,x6]
		for s in newsets:
			k,v = s.split("___")
			settings[k]["val"] = v
		s3.Object(bucket_name,settings_key).put(Body=json.dumps(settings))
		return jsonify(settings)
	except Exception,e:
		return "There was an error: "+str(e)


''' Get default cluster bake settings '''
@app.route("/get_default_bake_settings")
def get_default_bake_settings():
	try:
		s3 = boto3.resource('s3')
		result = s3.Object(bucket_name,settings_key).get()['Body'].read()		
 		return jsonify(json.loads(result))
	except Exception,e:
		return str(e)


''' Spin up a new cluster '''
@app.route("/bake")
def bake():
	try:
		return jsonify(baker.bake_emr(bucket_name,settings_key))
	except Exception, e:
		return 'error'+str(e)


''' Terminate an existing cluster (needs cluster ID) '''
@app.route("/terminate/<jobid>")
def terminate(jobid):
	try:
		return baker.terminate_emr(jobid)
	except Exception, e:
		return str(e)


''' Pulls down analysis output, stored in AWS SimpleDB table '''
@app.route('/pull/<table_name>')
@nocache # we need nocache, otherwise these results may cache in some browsers and ignore new data
def pull(table_name):
	try:
		client = boto3.client('sdb')
		paginator = client.get_paginator('select')
		output = {}
		query = "select * from {}".format(table_name)
		response = paginator.paginate( SelectExpression=query, ConsistentRead=True )
		ct = 0
		for r in response:
			for i,row in enumerate(r['Items']):
				ct+=1
				output[str(row['Name'])] = row['Attributes']
		#output["count"] = ct 
		return jsonify(output)
	except Exception, e:
		return str(e)+'error'


''' Check current status of cluster (need cluster ID) '''
@app.route('/checkcluster/<cid>')
def check_cluster_status(cid):
	try:
		client  = boto3.client('emr')
		cluster = client.describe_cluster(ClusterId=str(cid))['Cluster']

		if cluster is not None:
			cstatus = cluster['Status']['State']
			cname   = cluster['Id']
			return jsonify({"id":cname,"status":cstatus})
		else:
			return "No active clusters found"
	except Exception, e:
		return str(e)


''' Check if GD cluster is currently running '''
@app.route('/gd_cluster_running')
def gd_cluster_running(cluster_name="gaugingdebate"):
	try:
		client  = boto3.client('emr')

		response = client.list_clusters(
		    ClusterStates=['STARTING','BOOTSTRAPPING','RUNNING','WAITING']
		)

		clusters = response['Clusters']
		states = [c['Status']['State'] for c in clusters if c['Name'] == cluster_name]

		if len(clusters) == 0:
			return "0_There are no sentiment trackers currently running.<br /> \
					(If you have administrator privileges and want to start a tracker, <br />\
					go to the administrator dashboard to get one started.)<br /> \
					<input type='button' id='override-live-tracking' value='Try Tracking Anyway'>"
		
		elif ( ('RUNNING' not in states) and ('WAITING' not in states) ):
			return "0_A sentiment tracker is currently starting up. It takes about 15 minutes to start up \
					a tracker, please try back soon."
		
		else:
			return "1_Sentiment tracker found! <input type='button' id='start-live-tracking' value='Start Tracking'>..."

	except Exception, e:
		return str(e)


@app.route('/get_debate_options')
@nocache
def get_debate_options():
	try:
		client  = boto3.client('sdb')
		query = 'select * from debates'
		results = client.select(SelectExpression=query)['Items']
		output = []
			
		for r in results:
			a = r['Attributes']
			for el in a:
				if el['Name'] == 'party':
					party = el['Value']
				if el['Name'] == 'ddate':
					ddate = el['Value']
				if el['Name'] == 'start_time':
					start = el['Value']
				if el['Name'] == 'end_time':
					end = el['Value']
			output.append( {"party":party,"ddate":ddate,"start_time":start,"end_time":end} )
		
		return jsonify({"data":output})
	except Exception,e:
		return str(e)


@app.route('/get_debate_data/<start>/<end>')
def get_debate_data(start,end):
	try:
		client  = boto3.client('sdb')
		paginator = client.get_paginator('select')
		if int(end) > 0: 

			query = 'SELECT * FROM sentiment WHERE timestamp BETWEEN "{}" AND "{}"'.format(start,end)

		else:
			query = 'SELECT * FROM sentiment WHERE timestamp="{}"'.format(start)

		output = {}
			
		response = paginator.paginate( SelectExpression=query, ConsistentRead=True )
		ct = 0
		for r in response:
			for i,row in enumerate(r['Items']):
				ct+=1
				output[str(row['Name'])] = row['Attributes']

		return jsonify({"data":output})
	except Exception,e:
		return str(e)+" "+query


''' Scrape debate schedule and write to file '''
@app.route('/setschedule')
@nocache # we need nocache, otherwise these results may cache in some browsers and ignore new data
def set_schedule():
	try:
		utils.set_debate_schedule()
		return "ok"
	except Exception,e:
		return str(e)

''' Get current debate schedule on file '''
@app.route('/getschedule')
@nocache # we need nocache, otherwise these results may cache in some browsers and ignore new data
def get_schedule():
	try:
		csv = utils.get_debate_schedule()
		return Response(csv,mimetype="text/csv", headers={"Content-disposition":"attachment; filename=events.csv"})
	except Exception,e:
		print str(e)

''' Run main Flask app '''
if __name__ == '__main__':
	app.run(host='127.0.0.1',port=12340, debug = True)

## setting default values for optional paths:
## http://stackoverflow.com/questions/14032066/flask-optional-url-parameters
