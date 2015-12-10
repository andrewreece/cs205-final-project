
def filter_tweets(_):
	''' Filters out the tweets we do not want.  Filters include:
			* No retweets 
			* No geolocation or location field (do we really care about this?)
			* English language only
			* No tweets with links
				- We need to check both entities and media fields for this (is that true?)
	'''
	return lambda t: (('retweeted_status' not in t.keys()) 						  and 
					  ((t['geo'] is not None) or (t['user']['location'] is not None)) and
					  (t['user']['lang']=='en') 					  and
					  (len(t['entities']['urls'])==0) 				  and
					  ('media' not in t['entities'].keys())
					  )

def get_relevant_fields(_):
	''' Reduce the full set of metadata down to only those we care about, including:
			* timestamp
			* username
			* text of tweet 
			* hashtags
			* geotag coordinates (if any)
			* location (user-defined in profile, not necessarily current location)
	'''
	return lambda t:(t['id'], 
					   {"timestamp":  t['created_at'],
						"username":	t['user']['screen_name'],
						"text":		t['text'],
						"hashtags":	[el['text'] for el in t['entities']['hashtags']],
						"geotag":	t['geo'],
						"user_loc":	t['user']['location']
						}
					)