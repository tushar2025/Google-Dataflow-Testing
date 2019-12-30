import socket
import sys
from google.cloud import pubsub_v1
import time
import re
import datetime

publisher = pubsub_v1.PublisherClient()
topic_name = 'projects/{project_id}/topics/{topic}'.format(
    project_id='practice-00001',
    topic='securecyber')
UDP_IP = "127.0.0.1"
UDP_PORT = 1555
sock = socket.socket(socket.AF_INET, # Internet
                      socket.SOCK_DGRAM) # UDP
sock.bind(("", UDP_PORT))
i = 0

while True:
    data, addr = sock.recvfrom(1024) # buffer size is 1024 bytes
    dtarr = [re.search('(?<=date=)(.*)(?= time=)', data).group(0),
                 re.search('(?<=time=)(.*)(?= timezone=)', data).group(0)]
    dt = time.mktime((datetime.datetime.strptime(dtarr[0]+' '+dtarr[1], '%Y-%m-%d %H:%M:%S')).timetuple())
    publisher.publish(topic=topic_name,data=data,timestamp=str(time.time()))    
#if(i==20):
      #  publisher.publish(topic=topic_name,data=data,timestamp=str(time.time()))
     #   i=0
    #else:
      #  i+=1