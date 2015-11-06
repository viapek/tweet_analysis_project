# set global some variables

s_Dbase = "yourDBasename"
# a suffix will be added to each collection so different projects can be isolated
s_colSuffix = "suffix"
# if there are network issues twitstitcher will use the search api to try to fill the gap
# these collections will have this suffix for id purposes
s_StitchSuffix = "suffix"
# this is the max size of the mongo collections. When this is exceeded a new collection is started
i_MaxCollectionSize = 50000000

# mongo user credentials
s_MongoUser = ""
s_MongoPassword = ""
s_MongoDBHost = ""
i_MongoDBPort = 27017

# this is sent with the api request
s_TwitQueryString = ""

# twitter api and access tokens
api_key = ""
api_secret = ""
access_token_key = ""
access_token_secret = ""

# set destination for offline collection storage
s_OfflineCollectionPath = ""

# set working database for imports and analysis
s_WorkingDatabase = ""
s_WorkingCollection = ""
s_WorkingResults = ""

LOGFILE_NAME = ''
LOGFILE_FORMAT = '%(asctime)s TweetCollector (%(filename)s):%(funcName)s:%(threadName)s: %(message)s'
WORKING_DIR = ''
SCRIPT_NAME = ''

_debug = 0
# if this is true then scripts will print progressto help identify issues
debug = False