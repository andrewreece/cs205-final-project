def get_twitter_creds(bucket='cs205-final-project',key='setup/twitter/credentials'):
	''' get twitter api credentials from s3 '''
	import json
	import boto3
	s3 = boto3.resource('s3')
	obj = s3.Object(bucket,key)
	creds = json.loads(obj.get()['Body'].read())

	return creds['APP_KEY'], creds['APP_SECRET'],creds['OAUTH_TOKEN'],creds['OAUTH_TOKEN_SECRET']