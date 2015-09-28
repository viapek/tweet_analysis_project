import config
import oauth2 as oauth
import urllib2 as urllib
import json
import traceback
from pymongo import MongoClient
from time import gmtime, strftime
"""
 Make a new collection name for each restart
 This means we can start analysis and also moving of data sets before getting too big
"""
def initNewCollection(colSuffix):
    #build a new collection name using the suffix and initiate it
    return db[strftime('%m%d_%H%M%S', gmtime()) + "_" + colSuffix]

def writeToDbase(jsonString):
  """write the json string to the database and then check that the
  database has grown above the i_MaxCollectionSize
  
  If it has, create a new one. and carry on
  """
  global collection
  collection.insert(jsonString)
  i_CollectionSize = db.command("collstats", collection.name)["size"]
  if i_CollectionSize >= config.i_MaxCollectionSize:
    collection = initNewCollection(config.s_colSuffix)
      
#build an oauth token
oauth_token    = oauth.Token(key=config.access_token_key, secret=config.access_token_secret)
oauth_consumer = oauth.Consumer(key=config.api_key, secret=config.api_secret)

signature_method_hmac_sha1 = oauth.SignatureMethod_HMAC_SHA1()

http_method = "GET"

http_handler  = urllib.HTTPHandler(debuglevel=config._debug)
https_handler = urllib.HTTPSHandler(debuglevel=config._debug)

"""
connect to Mongo to prepare for writing
"""
client = MongoClient(config.s_MongoDBHost, config.i_MongoDBPort)
db = client[config.s_Dbase]
collection = initNewCollection(config.s_colSuffix)
'''
Construct, sign, and open a twitter request
using the hard-coded credentials above.
'''
def twitterreq(url, method, parameters):
  req = oauth.Request.from_consumer_and_token(oauth_consumer,
                                             token=oauth_token,
                                             http_method=http_method,
                                             http_url=url, 
                                             parameters=parameters)

  req.sign_request(signature_method_hmac_sha1, oauth_consumer, oauth_token)

  headers = req.to_header()

  if http_method == "POST":
    encoded_post_data = req.to_postdata()
  else:
    encoded_post_data = None
    url = req.to_url()

  opener = urllib.OpenerDirector()
  opener.add_handler(http_handler)
  opener.add_handler(https_handler)

  response = opener.open(url, encoded_post_data)

  return response

def fetchsamples():
  url = "https://stream.twitter.com/1.1/statuses/filter.json?track=" + config.s_TwitQueryString
  parameters = []
  response = twitterreq(url, "GET", parameters)
  
  # print type(pyresponse)
  for line in response:
      if line:
          try:
              lineJson = json.loads(line)
          except (ValueError, KeyError, TypeError) as e:
              if len(line) != 2:
                  print "Problem with json.loads"
                  print "-"*50
                  print "line length: {0}".format(len(line))
                  print "line data: {0}".format(line)
                  print "-"*50
                  pass
          else:
              writeToDbase(lineJson)

if __name__ == '__main__':
  fetchsamples()
  print traceback.print_stack()
