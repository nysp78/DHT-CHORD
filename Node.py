from hashlib import sha1
from threading import Thread, Lock
import requests
import sys
import time
from flask.json import JSONEncoder, JSONDecoder
from flask import Flask, redirect, url_for, request, logging, abort, render_template
import json
    
def hashing(value):
    return sha1(value.encode()).hexdigest()

class Node(object):
    def __init__(self, ip_addr, port):
        self.ip_addr = ip_addr
        self.port = port
        self.host = ip_addr + ":" + str(port)
        self.node_id = sha1(self.host.encode()).hexdigest()
        self.successor = self.host
        self.predecessor = self.host
        self.succ_lock = Lock() #loSck to write succ
        self.pred_lock = Lock() #lock to write pred
        self.node_storage = {}
        self.stabilizer_thread = Thread(target = self.stabilize, args=(2,)) #periodic operation
        self.stability = True
        self.stabilizer_thread.start()
       #self.check_predecessor_thread = Thread(target=self.check_predecessor, args=(10,)) #periodic operation


    def shutdown(self):
        func = request.environ.get('werkzeug.server.shutdown')
        if func is None:
            raise RuntimeError('Not running with the Werkzeug Server')
        func()
        return "ok", 200


    #Node leaves the DHT
    def leave(self):
        self.doing_stabilize = False
        succ = self.get_succ()
        keys = list(self.node_storage.keys())
        for key in keys:
            transferred = self.transfer_keys(key, succ) 
            if not transferred:
                print("KEYS NOT TRANSFERRED!!!!!!!!!!!!!!!!!!!")
                return False
        pred = self.get_pred()
        url = "http://{0}/node/set_successor/{1}".format(pred, succ)
        reply = requests.post(url)
        if reply.status_code != 200:
            print("2o IF!!!!!!!!!!!!!!")
            return False
        url = "http://{0}/node/set_predecessor/{1}".format(succ, pred)
        reply = requests.post(url)
        if reply.status_code != 200:
            print("3o IF!!!!!!!!!!!!!!")
            return False
        return True



    def get_succ(self):
        return self.successor
    
    def get_pred(self):
        return self.predecessor

    def get_storage(self):
        return json.dumps(self.node_storage, indent = 4) 

    def set_successor(self, res):
        self.succ_lock.acquire()
        self.successor = res
        self.succ_lock.release()

    #update the successor of the this node
    def update_successor(self, res):
        self.succ_lock.acquire()
        self.successor = res
        self.succ_lock.release()
        
        if res == self.host:
            return

        else:
            url = "http://{0}/node/transfer_keys/{1}".format(res, self.host)
            reply = requests.post(url)
            if reply.status_code != 200:
                return("error to transfer keys from successor")


    def update_predecessor(self, res):
        self.pred_lock.acquire()
        self.predecessor = res
        self.pred_lock.release()

    def transfer_keys(self, key, target_addr):
        if target_addr == self.host:
            return 1
        key_tostr = str(key)
        value = self.node_storage[key_tostr]
        url = "http://{0}/node/send_item/{1}/{2}".format(target_addr, key_tostr, value)
        reply = requests.post(url)
        if reply.status_code==200:
            del self.node_storage[key_tostr]
            return 1
        
        else:
            return 0

    @staticmethod
    def between(x, a, b):
        if a > b:
            return a < x or x < b
        elif a < b:
            return a < x and x < b
        else:
            return x !=a and x!=b

    @staticmethod
    def between_right(x, a, b):
        if a > b:
            return a < x or b >= x
        elif a < b:
            return a < x and b >= x
        else:
            return a !=x

    #successor sets node as its pred
    def notify(self, addr):
        addr_id = hashing(addr)
        pred = self.get_pred()
        pred_id = hashing(pred)
        if pred == self.host or Node.between(addr_id, pred_id, self.node_id):
            self.update_predecessor(addr)
            return True
        return False

    def find_successor(self, help_node, target_node):
        try:
            url = "http://{0}/node/find_successor/{1}".format(help_node, target_node)
            request = requests.get(url)
            if request.status_code == 200:
                return request.text #5000
        except:
            raise ConnectionError


    @staticmethod
    def stabilize_node(node):
        succ = node.get_succ()
        pred = node.get_pred()

        if succ != node.host: #node asks its successor for its predecessor
            url = 'http://{0}/node/get_pred'.format(succ)
            r = requests.get(url)
            if r.status_code == 200:
                    x = r.text
            else:
                print('Error getting predecessor for successor: {0}'.format(url))

        elif pred != node.host:
            x = pred
        else:
            return

        print("PRED HERE:", x)
        x_id = hashing(x)
        succ_id = hashing(succ)

        if Node.between(x_id, node.node_id, succ_id):
            node.update_successor(x)

        url = 'http://{0}/node/notify?addr={1}'.format(node.get_succ(), node.host) #node notify his successor for being his pred
        r = requests.post(url)
        if r.status_code != 200:
            print('Unable to notify successor: {0}'.format(url))

    
    def stabilize(self, interval):
        while self.stability:
            Node.stabilize_node(self)
            time.sleep(interval) 