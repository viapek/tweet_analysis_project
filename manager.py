import random
import socket
import subprocess
import threading
import time
import fetch

def checkNetwork(restartEvent):
  # trying to resolve a network name using DNS
  while True:
    try:
      # resolve the site to IP to confirm network connectivity
      resolved = socket.gethostbyname("google.co.nz")
    except socket.error as er:
      # checkNetwork resolution failed. restarting network interface
      proc = 1
      while proc == 1:
        proc = subprocess.call(['sudo', '/home/jeremy/jUtils/netrestart.sh'])
        time.sleep(1)
      #notify restartEvent listeners
      restartEvent.set()
      time.sleep(1)
    else:
      # network fine
      break
      
def watcher(writeEvent, restartEvent):
  # watcher starting in an infinite loop
  while True:
    # waiting 30 seconds for a writeEvent
    event_is_set = writeEvent.wait(30)
    if not event_is_set:
      # watcher did not receive a writeEvent so check the network
      checkNetwork(restartEvent)
      time.sleep(2)
      if restartEvent.isSet():
        # watcher saw restartEvent was set
        while fetchThread.isAlive():
          #watcher waiting for fetchThread to get the message and end
          time.sleep(1)
        # watcher saw fetchThread has ended
        restartEvent.clear()
        # watcher resetting restartEvent
        startFetchThread(writeEvent,restartEvent)
        # watcher restarted fetchThread
        time.sleep(1)
        
    else:
      #watcher received a writeEvent to carry on
      # reset the writeEvent
      writeEvent.clear()
      #watcher reset writeEvent
      time.sleep(1)
"""
def fetch(writeEvent, restartEvent):
  print "fetch starting"
  rndRange = random.sample(range(6),6)
  print rndRange
  for x in rndRange:
    if restartEvent.isSet():
        print "fetch received a restartEvent... bye"
        return
    print "fetch setting writeEvent"
    writeEvent.set()
    print "fetch pausing for ", x
    time.sleep(x)
"""

def startWatcherThread(writeEvent, restartEvent):
  watcherThread = threading.Thread(name='watcher', target=watcher, args=(writeEvent,restartEvent,))
  watcherThread.setDaemon(True)
  watcherThread.start()
  return watcherThread

def startFetchThread(writeEvent, restartEvent):
  fetchThread = threading.Thread(name='fetch', target=fetch.fetch, args=(writeEvent,restartEvent,))
  fetchThread.setDaemon(False)
  fetchThread.start()
  return fetchThread

#create the event used to watch over rollingcollections
writeEvent = threading.Event()
#create an event to exit fetcher
restartEvent = threading.Event()

#print "main starting watch" 
watcherThread = startWatcherThread(writeEvent, restartEvent)   
#print 'main starting fetch'
fetchThread = startFetchThread(writeEvent, restartEvent)
