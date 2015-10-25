import config
import logging
import signal
import subprocess
import sys
import threading
import time
import socket

from dev_collector import fetchsamples as fetch

def signal_handler(signal, frame):
    logging.debug('Signal %d received. Terminating...', signal)
    sys.exit(0)

def watcher(e):
    site = "google.com"
    while not e.isSet():
        event_is_set = e.wait(300)
        if not event_is_set:
            logging.debug('timed out waiting for write confirmation. trying http test')
            try:
                resolved = socket.gethostbyname(site)
            except socket.error as er:
                logging.debug("error - %s", er)
                proc = subprocess.call(['sudo', '/home/jeremy/jUtils/netrestart.sh'])
                logging.debug('restarted network with return code %d', proc)
                pass
        else:
            e.clear()
            
        
# set up the logging for the app
logging.basicConfig(filename=config.LOGFILE_NAME, level=logging.DEBUG, format=config.LOGFILE_FORMAT)
signal.signal(signal.SIGINT, signal_handler)

#create the event used to watch over rollingcollections
e = threading.Event()
print 'Launching watcher'
#initiate the rollingcollections thread and leave it do do it's thing
watcherThread = threading.Thread(name='watcher', target=watcher, args=(e,))
watcherThread.setDaemon(True)
watcherThread.start()
print 'Starting tweet collector'
fetchThread = threading.Thread(name='fetch', target=fetch, args=(e,))
fetchThread.setDaemon(True)
fetchThread.start()

while True:
    time.sleep(5)
    sExit = raw_input('Enter e[X]it to quit tweet collector... ')
    if sExit == 'X':
        break