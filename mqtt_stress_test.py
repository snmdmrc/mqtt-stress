#!/usr/bin/env python

import os
import time
import urllib.parse as urlparse
import configparser as ConfigParser
import mqtt_publisher_thread as pub
import mqtt_subscriber_thread as sub

senders = []
receivers = []

def get_url(cp):
    url_str = cp.get('MQTT','url')
    if url_str.find('mqtt://') < 0:
        url_str = 'mqtt://' + url_str
    print("Trying url: " + url_str)
    url = urlparse.urlparse(url_str)
    return url

def create_senders(url, sender_number, receiver_number, min_time, max_time, msg_root,msg_number):
    for i in range(sender_number):
        t = pub.ThreadPub(url, i, receiver_number, min_time, max_time, msg_root,msg_number)
        t.setDaemon(True)
        senders.append(t)
        t.start()


def create_receivers(url, sender_number, receiver_number, min_time, max_time, msg_root,msg_number):
    for i in range(receiver_number):
        t = sub.ThreadSub(url, sender_number, i, min_time, max_time, msg_root,msg_number)
        t.setDaemon(True)
        receivers.append(t)
        t.start()


def main():
    default_cfg = {
        'url'        : 'localhost:1883',
        'clientid'   : 'stress',
        'msg_num'    : '100',
        'sleep_min'  : '0.01',
        'sleep_max'  : '0.50',
        'threads'    : '50',
        'topic'      : '/stress',
        'msg'        : 'abcd'
        }

    print('Config read from: ' + os.path.splitext(__file__)[0] + '.ini')
    cp = ConfigParser.SafeConfigParser(default_cfg)
    cp.read(os.path.splitext(__file__)[0] + '.ini')

    publisher_count = cp.getint('MESSAGES','publisher_count')
    receiver_count = cp.getint('MESSAGES', 'receiver_count')
    url =get_url(cp)
    sleep_min=cp.getfloat('MESSAGES', 'sleep_min')
    sleep_max=cp.getfloat('MESSAGES', 'sleep_max')
    msg=cp.get('MESSAGES', 'msg')
    msg_num= cp.getint('MESSAGES', 'msg_num')

    print("Each of"+str(publisher_count)+" publisher will send" + str(msg_num) +" messages ");
    print ("Totally "+str(receiver_count) +" receiver will be there")

    if publisher_count > 1 and receiver_count > 1:
        raise Exception("Both publisher and receiver can not be greater than 1")

    create_receivers(url,publisher_count,receiver_count,sleep_min,sleep_max,msg,msg_num)
    time.sleep(10)
    print("sending start")
    create_senders(url,publisher_count,receiver_count,sleep_min,sleep_max,msg,msg_num)

    for sender in senders:
        sender.join()

    for receiver in receivers:
        receiver.join()


    print(str(sub.global_count) +" items are received")
    print("COMPLETED")

main()


