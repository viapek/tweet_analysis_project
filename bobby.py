import time
import random

x = random.randint(1,10)

while True:
    time.sleep(2)
    if x == random.randint(1,10):
        print "die"
    else:
        print "all good"
        