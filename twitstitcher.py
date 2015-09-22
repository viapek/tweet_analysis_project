import config
import sys
import json
import os
import tweepy
import pymongo
from pymongo import MongoClient

debug = False

def stitchexecute(m_Dbase, ary_ExecuteData):
    if debug:
        print "   |" + "-"*50
        print "   | In stitchexecute with database: {0}, arraydata: {1}".format(m_Dbase.name, ary_ExecuteData)
        print "   |" + "-"*50
    s_CollectionName = ary_ExecuteData[0] + "_" + ary_ExecuteData[2] + "_stitch"
    if debug:
        print "   | Checking if {0} is in the list of Collection names".format(s_CollectionName)
    if s_CollectionName in m_Dbase.collection_names():
        if debug:
            print "   | It is in the list so skip this one and return"
            print "   | {0} already exists with {1} records".format(s_CollectionName, m_Dbase[s_CollectionName].count())
            print "   |" + "-"*50
        return "{0} already exists with {1} records".format(s_CollectionName, m_Dbase[s_CollectionName].count())
    
    collection = m_Dbase[s_CollectionName]
    
    if (not collection):
        return "Mongo problem"
        sys.exit(-1)
    else:
        if debug:
            print "   | We have a handle on {0}".format(collection.name)
    
# Replace the API_KEY and API_SECRET with your application's key and secret.
              
        searchQuery = config.s_TwitQueryString  # this is what we're searching for
        if debug:
            print "   | Go looking for {0} at twitter".format(searchQuery)
        maxTweets = 10000000 # Some arbitrary large number
        tweetsPerQry = 100  # this is the max the API permits
  
        # If results from a specific ID onwards are reqd, set since_id to that ID.
        # else default to no lower limit, go as far back as API allows
        since_id = ary_ExecuteData[1]
        if debug:
            print "   | Query since_id: {0} ".format(since_id)

        # If results only below a specific ID are, set max_id to that ID.
        # else default to no upper limit, start from the most recent tweet matching the search query.
        max_id = ary_ExecuteData[3]
        if debug:
            print "   | Query max_id:   {0}".format(max_id)
        tweetCount = 0

        while tweetCount < maxTweets:
            try:
                if (max_id <= 0):  # if we don't care about when to stop
                    if (not since_id): # if we don't care about where to start
                        if debug:
                            print "   | Running query without end"
                        new_tweets = api.search(q=searchQuery, count=tweetsPerQry)
                    else: #if we care about where to start
                        if debug:
                            print "   | Running query without since_id only"
                        new_tweets = api.search(q=searchQuery, count=tweetsPerQry, since_id=since_id)
                else: #if we care about the range
                    if (not since_id): #but not start
                        if debug:
                            print "   | Running query max_id only"
                        new_tweets = api.search(q=searchQuery, count=tweetsPerQry, max_id=str(max_id - 1))
                    else: #care about start and end
                        if debug:
                            print "   | Running query with since_id and max_id"
                        new_tweets = api.search(q=searchQuery, count=tweetsPerQry, max_id=str(max_id - 1), since_id=since_id)
            
                if not new_tweets:
                    if debug:
                        print "   | No new tweets for {0}".format(collection.name)
                    return "No new tweets for {0}".format(collection.name)
                
                for tweet in new_tweets:
                    try:
                        if debug:
                            print "   | Write to mongo"
                            print tweet._json
                        else:
                            collection.insert(tweet._json)
                    except (ValueError, KeyError, TypeError) as e:
                        return str(e)
                            
                    tweetCount += len(new_tweets)

                    max_id = new_tweets[-1].id
            except tweepy.TweepError as e:
                # Just exit if any error
                return str(e)
        if debug:
            print "   |" + "-"*50
            print "   | Finished stitchexecute"
            print "   |" + "-"*50
            

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
    if debug:
        print "Start iterating through tw_Collections with {0} items".format(len(tw_Collections))
    for collection in tw_Collections:
        if debug:
            print "Working with {0}".format(collection)
            print "-"*50
        if not collection.endswith(c_suffix): #if it ain't matching our collection suffix, skip it
            #ary_PlannedActions.append("Don't care about {0} because it doesn't match suffix {1}".format(collection,c_suffix))
            if debug:
                print "- suffix not {0}".format(c_suffix)
            continue
        if from_Collection != "": #we already have from_Collection and since_id
            #get the first record and set max_id
            if debug:
                print "We do have a from_Collection so this will be the to_Collection"
            first = m_Dbase[collection].find({}, { "id":1,"text":1,"created_at":1,"user.name":1 }).sort("_id",pymongo.ASCENDING).limit(1)
            for item in first:
                #set max_id
                max_id = item["id"] #stitchexecute performs -1 operation
                if debug:
                    print "The max_id for {0} is {1}".format(collection,max_id)
            to_Collection = collection
            #ary_PlannedActions.append("Got first record id: {0}, from {1}".format(max_id, collection))

            if debug:
                print "Run stitchexecute"  
            ary_PlannedActions.append(stitchexecute(m_Dbase, [from_Collection, since_id, to_Collection, max_id]))
            
            #from_Collection = to_Collection #set the from to the current collection
        #get the last id to become the since_id
        last = m_Dbase[collection].find({}, { "id":1,"text":1,"created_at":1,"user.name":1 }).sort("_id",pymongo.DESCENDING).limit(1)
        for item in last:
            #set max_id
            since_id = item["id"]
            if debug:
                print "The since_id for {0} is {1}".format(collection,since_id)
        from_Collection = collection
        if debug:
            print "Reassigning {0} to be the next from_Collection".format(collection)
    return ary_PlannedActions


if __name__ == '__main__':
    client = MongoClient(config.s_MongoDBHost, config.i_MongoDBPort)
    db = client[config.s_Dbase]

    auth = tweepy.AppAuthHandler(config.api_key, config.api_secret)
  
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
  
    if (not api):
        print "Can't Authenticate"
        sys.exit(-1)

    print stitchCollections(db,config.s_colSuffix)
