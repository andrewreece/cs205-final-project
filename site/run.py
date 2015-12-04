from flask import Flask, jsonify, request, current_app, render_template, Response
from functools import wraps
from random import randint
from datetime import datetime, timedelta
from collections import OrderedDict
import numpy as np
import time
import boto3
import baker # this is a library we made with EMR baking functions
import utils # misc helper functions, includes get/set debate schedule
from nocache import nocache # this is a library someone else made that keeps Flask from caching results


## the core flask app
app = Flask(__name__)


''' Load web interface index.html '''
@app.route("/")
def template_index():
	try:
		return render_template('index.html')
	except Exception, e:
		return "Error:"+str(e)

''' Load admin interface admini.html '''
@app.route("/admini")
def template_admini():
	try:
		return render_template('admini.html')
	except Exception, e:
		return "Error:"+str(e)

''' Spin up a new cluster '''
@app.route("/bake")
def bake():
	try:
		return jsonify(baker.bake_emr())
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
