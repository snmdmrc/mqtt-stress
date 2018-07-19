#!/usr/bin/env python

import random
import os
import binascii
import Queue as Queue
import threading
import time
import ConfigParser as ConfigParser
import paho.mqtt.client as mqtt


class ThreadPub(threading.Thread):
    """Threaded Publish"""
    def __init__(self, url, sender_number, receiver_number, min_time, max_time, msg_root,msg_number):
        threading.Thread.__init__(self)
        self.url = url
        self.sender_number = sender_number
        self.receiver_number = receiver_number
        self.min_time = min_time
        self.max_time = max_time
        self.msg_root = msg_root
        self.msg_number=msg_number
        self.counter=0


    def run(self):
        mqttc = mqtt.Client("Publisher"+str(self.sender_number), clean_session=True)
        mqttc.username_pw_set("admin", "admin")
        mqttc.disconnect()
        mqttc.connect(self.url.hostname, self.url.port, 60)

        for y in range(self.receiver_number):
            for i in range(self.msg_number):
                time.sleep(random.uniform(self.min_time, self.max_time))
                msg = self.msg_root
                topic = "publisher/"+str(y)
                mqttc.publish(topic, msg)
            time.sleep(0.1)
        time.sleep(2)



