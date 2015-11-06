import config
import oauth2 as oauth
import urllib2 as urllib
import json
import traceback
import threading
from pymongo import MongoClient
from time import gmtime, strftime
import time
import subjserver

class jTweetCollector(object):
    
  def __init__(self):
    print "Got a class: {0}".format(self)
  
  def reconnect(self):
    print "Interclass notification... Reconnect done"
    
  """
    Make a new collection name for each restart
    This means we can start analysis and also moving of data sets before getting too big
  """
  def __initNewCollection(self, colSuffix):
    #build a new collection name using the suffix and initiate it
    return self.__db[strftime('%m%d_%H%M%S', gmtime()) + "_" + colSuffix]

  def __writeToDbase(self, jsonString):
    """write the json string to the database and then check that the
    database has grown above the i_MaxCollectionSize
  
    If it has, create a new one. and carry on
    """
    self.__collection.insert(jsonString)
    i_CollectionSize = self.__db.command("collstats", self.__collection.name)["size"]
    if i_CollectionSize >= config.i_MaxCollectionSize:
      self.__collection = self.__initNewCollection(config.s_colSuffix)
      

  '''
  Construct, sign, and open a twitter request
  using the hard-coded credentials above.
  '''
    
  def __twitterreq(self, url, method, parameters):
    req = oauth.Request.from_consumer_and_token(self.__oauth_consumer,
                                             token=self.__oauth_token,
                                             http_method=self.__http_method,
                                             http_url=url, 
                                             parameters=parameters)

    req.sign_request(self.__signature_method_hmac_sha1, self.__oauth_consumer, self.__oauth_token)

    headers = req.to_header()

    if self.__http_method == "POST":
      encoded_post_data = req.to_postdata()
    else:
      encoded_post_data = None
      url = req.to_url()

    opener = urllib.OpenerDirector()
    opener.add_handler(self.__http_handler)
    opener.add_handler(self.__https_handler)

    response = opener.open(url, encoded_post_data)

    return response

  def fetchsamples(self):
    url = "https://stream.twitter.com/1.1/statuses/filter.json?track=" + config.s_TwitQueryString
    parameters = []
    response = self.__twitterreq(url, "GET", parameters)
  
    # print type(pyresponse)
    for line in response:
      if line:
          try:
              print line #lineJson = json.loads(line)
          except (ValueError, KeyError, TypeError) as e:
              if len(line) != 2:
                  print "Problem with json.loads"
                  print "-"*50
                  print "line length: {0}".format(len(line))
                  print "line data: {0}".format(line)
                  print "-"*50
                  pass
          else:
              self.__writeToDbase(lineJson)


  def httpRequestPrep(self):

    #build an oauth token
    self.__oauth_token    = oauth.Token(key=config.access_token_key, secret=config.access_token_secret)
    self.__oauth_consumer = oauth.Consumer(key=config.api_key, secret=config.api_secret)

    self.__signature_method_hmac_sha1 = oauth.SignatureMethod_HMAC_SHA1()

    self.__http_method = "GET"

    self.__http_handler  = urllib.HTTPHandler(debuglevel=config._debug)
    self.__https_handler = urllib.HTTPSHandler(debuglevel=config._debug)

  def mongoConnect(self):
    """
    connect to Mongo to prepare for writing
    """
    self.__client = MongoClient(config.s_MongoDBHost, config.i_MongoDBPort)
    self.__db = self.__client[config.s_Dbase]
    self.__collection = self.__initNewCollection(config.s_colSuffix)


if __name__ == '__main__':

  x = 0

  me = jTweetCollector()
  me.mongoConnect()
  me.httpRequestPrep()
  
  netmon = subjserver.jNetworkMonitor()
  netmon.watchOutForMe(me)
  
  t = threading.Timer(10.0, netmon.grabPage())
  t.start()
  
  me.fetchsamples()  
         
  netmon.endMyWatch(me)

  
  
    