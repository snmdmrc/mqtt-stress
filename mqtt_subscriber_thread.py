#!/usr/bin/env python

import random
import os
import binascii
import queue as Queue
import threading
import time
import urllib.parse as urlparse
import configparser as ConfigParser
import paho.mqtt.client as mqtt

global_count={}
lock = threading.Lock()

def on_message(client, userdata, msg):
    global global_count
    lock.acquire()
    receiver=str(msg.topic)[str(msg.topic).rfind('/')+1:]
    global_count[receiver]=global_count.get(receiver,0) +1
    lock.release()

class ThreadSub(threading.Thread):
    """Threaded Subs"""
    def __init__(self, url, sender_number, receiver_number, min_time, max_time, msg_root,msg_number):
        threading.Thread.__init__(self)
        self.url = url
        self.sender_number = sender_number
        self.receiver_number = receiver_number
        self.min_time = min_time
        self.max_time = max_time
        self.msg_root = msg_root
        self.msg_number = msg_number

    def run(self):
        global global_count
        mqttc = mqtt.Client("Receiver"+str(self.receiver_number), clean_session=True)
        mqttc.on_message = on_message
        mqttc.username_pw_set("admin", "admin")
        mqttc.disconnect()
        mqttc.connect(self.url.hostname, self.url.port, 60)
        mqttc.loop_start()

        mqttc.subscribe("publisher/"+str(self.receiver_number))
        #print(str(self.receiver_number)+"has subs")

        if self.sender_number > 1:
            while True:
                time.sleep(1)
                print("Total read message is: "+str(global_count.get(str(self.receiver_number))))
                if global_count.get(str(self.receiver_number)) == self.msg_number * self.sender_number:
                    break
        else:
            while True:
                time.sleep(1)
                #print("Total read message is: "+str(global_count.get(str(self.receiver_number))) +"for "+str(self.receiver_number))
                if global_count.get(str(self.receiver_number)) == self.msg_number:
                    break
        print("All messages are received for receiver "+str(self.receiver_number))