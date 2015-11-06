import threading
import time


def hello():

  y = 0

  while True:
    print "Counting y: {0}".format(y)
    time.sleep(4)
    if y == 10:
      break
    else:
      y += 1
    
    
t = threading.Timer(5.0, hello)
t.start()

x = 0

while True:
    print "Counting x: {0}".format(x)
    time.sleep(1)
    if x== 20:
      break
    else:
      x += 1
      
t.join(10.0)
print t.ident