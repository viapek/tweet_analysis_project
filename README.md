# tweet_analysis_project
A python running collection from twitter stream into mongodb using pymongo along with some tools for management and analysis.

As part of a learning project into mongodb, mapreduce, python, and web apis I set out to collect tweets relating to the Rugby World Cup.

This was taking place on my laptop so I wrote some scripts to run a rollingcollection.py of data from the stream 
while maintaining small collections sizes to enable management of disk usage.

I also added a script twitsticher.py to fill any gaps left due to network or other failure. This script is run once the
rollingcollection has been started again and then cycles through all of the collections and checks to see if there are any
tweets that were missed and puts them in a stitch collection.

export_dropCollections.py gets the list of collections and asks a date. Then it exports, drops, or both to matching collections. 
The export also gzips the mongodump to minimise disk usage. If the both option is selected, the export and gzip must be
successful before the drop is performed.
***** This works with a very simple month/day hash and will NOT work when the year changes so this is something that needs to be 
addressed before you run over a new years ****

The importCollections.py asks for a date range to import and then unzips, imports to a working database, and rezips the archives.
***** Again the date funciton is rudimentary so you should address this to include a year if you want this functionality *****

mapreduction.py is a simple mapreduce script to bundle up tweets for five minute intervals and aggregate a word count.
This uses the working database and collection defined in the config.py and is the same as the importCollections.py.

TODO

Update so that the rolling collections and other scripts address the date range limitations.
