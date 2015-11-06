import socket
import time
while True:
    try:
        resolved = socket.gethostbyname('google.com')
    except socket.error as e:
        print e
    else:
        print resolved
        time.sleep(300)