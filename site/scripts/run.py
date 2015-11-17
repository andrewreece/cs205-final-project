from flask import Flask, jsonify, request, current_app, render_template
import numpy as np
import pandas as pd
import re
import time
from functools import wraps
from random import randint
import boto.sdb

## the core flask app
app = Flask(__name__)

## GLOBALS ##
#data_directory  = "/home/dharmahound/research.andrewgarrettreece.com/data/"
path_head = "/home/dharmahound/analytics.andrewgarrettreece.com/"
conn = boto.sdb.connect_to_region(
					'us-east-1',
					aws_access_key_id='AKIAJIDIX4MKTPI4Y27A',
					aws_secret_access_key='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')


@app.route("/")
def template_index():
	try:
		return render_template('index.html')
	except Exception, e:
		return "Error:"+str(e)
		
@app.route('/pull/<table_name>')
def pull(table_name):
	try:
		output = {}
		table = conn.get_domain(table_name)
		query = "select * from {}".format(table_name)
		rows = table.select(query)
		for i,row in enumerate(rows):
			output[str(i)] = row
		return jsonify(output)
	except Exception, e:
		return str(e)
## verify: verify that a user has followed us on instagram/twitter; if so, register them in 'usernames' table
## we need the support_jsonp decorator to return JSONP format to Qualtrics - otherwise it throws a CORS error
#@app.route('/verify/<medium>/<username>/<followed>/<unique_id>', methods = ['GET'])
#@support_jsonp
#def verify(medium, username, followed, unique_id):


## run flask
if __name__ == '__main__':
	app.run(host='127.0.0.1',port=12340, debug = True)

## setting default values for optional paths:
## http://stackoverflow.com/questions/14032066/flask-optional-url-parameters
