import config
import sys
import json
import os
import tweepy
import pymongo
from pymongo import MongoClient


def stitchexecute(m_Dbase, ary_ExecuteData):
    s_CollectionName = ary_ExecuteData[0] + "_" + ary_ExecuteData[2] + "_stitch"
    if s_CollectionName in m_Dbase.collection_names():
        return "{0} already exists with {1} records".format(s_CollectionName, m_Dbase[s_CollectionName].count())

    collection = m_Dbase[s_CollectionName]
    if (not collection):
        return "Mongo problem"
        sys.exit(-1)
    else:

# Replace the API_KEY and API_SECRET with your application's key and secret.
        auth = tweepy.AppAuthHandler(config.api_key, config.api_secret)
  
        api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
  
        if (not api):
            return "Can't Authenticate"
            sys.exit(-1)
        
        searchQuery = config.s_TwitQueryString  # this is what we're searching for
        maxTweets = 10000000 # Some arbitrary large number
        tweetsPerQry = 100  # this is the max the API permits
  
        # If results from a specific ID onwards are reqd, set since_id to that ID.
        # else default to no lower limit, go as far back as API allows
        sinceId = ary_ExecuteData[1]
 
        # If results only below a specific ID are, set max_id to that ID.
        # else default to no upper limit, start from the most recent tweet matching the search query.
        max_id = ary_ExecuteData[3]
 
        tweetCount = 0

        while tweetCount < maxTweets:
            try:
                if (max_id <= 0):  # if we don't care about when to stop
                    if (not sinceId): # if we don't care about where to start
                        new_tweets = api.search(q=searchQuery, count=tweetsPerQry)
                    else: #if we care about where to start
                        new_tweets = api.search(q=searchQuery, count=tweetsPerQry, since_id=sinceId)
                else: #if we care about the range
                    if (not sinceId): #but not start
                        new_tweets = api.search(q=searchQuery, count=tweetsPerQry, max_id=str(max_id - 1))
                    else: #care about start and end
                        new_tweets = api.search(q=searchQuery, count=tweetsPerQry, max_id=str(max_id - 1), since_id=sinceId)
            
                if not new_tweets:
                    print "No new tweets for {0}".format(collection.name)
        
                for tweet in new_tweets:
                    try:
                        collection.insert(tweet._json)
                    except (ValueError, KeyError, TypeError) as e:
                        return str(e)
                            
                    tweetCount += len(new_tweets)

                    max_id = new_tweets[-1].id
            except tweepy.TweepError as e:
                # Just exit if any error
                return str(e)
            



def stitchCollections(m_Dbase, c_suffix):
    """This function will take a mongodatabase and cycle through the collections.
    Each collection will be parsed for its first and last record id.
    Then twitter search api will be queried to find any tweets missed between the last
    and the first.
    """
    #create an array to put the data groups for development feedback
    ary_PlannedActions = []
    
    #set initial colleciton to zero string so we know it's the first iteration
    from_Collection = ""
    to_Collection = ""
    
    #get the collections
    tw_Collections = m_Dbase.collection_names()
    
    for collection in tw_Collections:
        if not collection.endswith(c_suffix): #if it ain't matching our collection suffix, skip it
            #ary_PlannedActions.append("Don't care about {0} because it doesn't match suffix {1}".format(collection,c_suffix))
            continue
        if from_Collection != "": #we already have from_Collection and since_id
            #get the first record and set max_id
            first = m_Dbase[collection].find({}, { "id":1,"text":1,"created_at":1,"user.name":1 }).sort("_id",pymongo.ASCENDING).limit(1)
            for item in first:
                #set max_id
                max_id = item["id"]-1
            to_Collection = collection
            #ary_PlannedActions.append("Got first record id: {0}, from {1}".format(max_id, collection))

              
            ary_PlannedActions.append(stitchexecute(m_Dbase, [from_Collection, since_id, to_Collection, max_id]))
            
            from_Collection = to_Collection #set the from to the current collection
            
        #get the last id to become the since_id
        last = m_Dbase[collection].find({}, { "id":1,"text":1,"created_at":1,"user.name":1 }).sort("_id",pymongo.DESCENDING).limit(1)
        for item in last:
            #set max_id
            since_id = item["id"]
        from_Collection = collection
        
    return ary_PlannedActions


if __name__ == '__main__':
    client = MongoClient(config.s_MongoDBHost, config.i_MongoDBPort)
    db = client[config.s_Dbase]

    print stitchCollections(db,config.s_colSuffix)
