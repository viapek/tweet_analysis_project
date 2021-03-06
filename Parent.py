import config
import logging
import signal
import subprocess
import sys
import threading
import time
import socket
import urllib2 as urllib
from dev_collector import fetchsamples as fetch

def restartNetwork(er):
    logging.debug("error - %s", er)
    proc = subprocess.call(['sudo', '/home/jeremy/jUtils/netrestart.sh'])
    logging.debug('restarted network with return code %d', proc)
    

def signal_handler(signal, frame):
    logging.debug('Signal %d received. Terminating...', signal)
    sys.exit(0)

def watcher(e, exitFetcher):
    site = "google.com"
    while not e.isSet():
        event_is_set = e.wait(300)
        if not event_is_set:
            logging.debug('timed out waiting for write confirmation. trying http test')
            try:
                # resolve the site to IP to confirm network connectivity
                resolved = socket.gethostbyname(site)
            except socket.error as er:
                # had an error so restart the network interface
                restartNetwork(er)
                logging.debug("Restarted network")
                exitFetcher.set()
                logging.debug("Fetch process called to exit")
                return
            else:
                logging.debug("Successfully resolved %s to %s", site , resolved)
                
        else:
            e.clear()
            
        
# set up the logging for the app
logging.basicConfig(filename=config.LOGFILE_NAME, level=logging.DEBUG, format=config.LOGFILE_FORMAT)
signal.signal(signal.SIGINT, signal_handler)

#create the event used to watch over rollingcollections
e = threading.Event()
#create an event to exit fetcher
exitFetcher = threading.Event()

while True:
  print 'Launching watcher'
  #initiate the rollingcollections thread and leave it do do it's thing
  watcherThread = threading.Thread(name='watcher', target=watcher, args=(e,exitFetcher,))
  watcherThread.setDaemon(True)
  watcherThread.start()
  print 'Starting tweet collector'
  fetchThread = threading.Thread(name='fetch', target=fetch, args=(e,exitFetcher,))
  fetchThread.setDaemon(True)
  fetchThread.start()

  while fetchThread.isAlive(): # wait until the fetchThread is ended
    fetchThread.join(60)
    
    
  print 'fetch exited...'