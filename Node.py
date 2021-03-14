from hashlib import sha1
from threading import Thread, Lock
import requests
import sys
import time
from flask.json import JSONEncoder, JSONDecoder
from flask import Flask, redirect, url_for, request, logging, abort, render_template
import json
from config import *

    
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
        self.check_predecessor_thread = Thread(target=self.check_predecessor, args=(10,)) #periodic operation
        self.stabilizer_thread.start()
        self.check_predecessor_thread.start()


    #shutdown the Werkzeug Server
    def shutdown(self):
        func = request.environ.get('werkzeug.server.shutdown')
        if func is None:
            raise RuntimeError('Not running with the Werkzeug Server')
        func()
        return "ok", 200


    #Node leaves the DHT
    def leave(self):
        self.stability = False
        succ = self.get_succ()
        keys = list(self.node_storage.keys())        
        for key in keys:
            transferred = self.transfer_keys(key, succ) 
            if not transferred:
                print("KEYS NOT TRANSFERRED!!!!!!!!!!!!!!!!!!!")
                return False
        pred = self.get_pred()
        try:
            url = "http://{0}/node/set_successor/{1}".format(pred, succ) 
            reply = requests.post(url)
            if reply.status_code != 200:
                return False
            
            url = "http://{0}/node/set_predecessor/{1}".format(succ, pred)
            reply = requests.post(url)
            if reply.status_code != 200:
                return False

        except Exception:
            return "PIPIS"
        
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


    #function that transfer keys between nodes: departing and join nodes in the ring
    # chain replication -> not preserve the number of replicas 
    # eventual consistency -> preserve the number of replicas 
    def transfer_keys(self, key, target_addr):
        if CONSISTENCY == "chain":
            if target_addr == self.host:
                return 1
            value = self.node_storage[str(key)]
            replicas = value.split(":")[1]
            if replicas == "1":
                url = "http://{0}/node/send_item/{1}/{2}".format(target_addr, key, value)
                reply = requests.post(url)
                if reply.status_code==200:
                    del self.node_storage[key]
                    return 1
                else:
                    return 0
            else: 
                url = "http://{0}/node/send_repl_item/{1}/{2}".format(target_addr, key, value)
                reply = requests.post(url)
                if reply.status_code==200:
                    del self.node_storage[key]
                    return 1
                else:
                    return 0
                    
        elif CONSISTENCY == "eventual":
            if target_addr == self.host:
                return 1
            value = self.node_storage[str(key)]
            url = "http://{0}/node/eventually_transfer/{1}/{2}".format(target_addr, key, value)
            reply = requests.post(url)
            if reply.status_code==200:
                del self.node_storage[key]
                return 1
            
            else:
                return 0

#determine the position of node/key in the ring
    @staticmethod
    def between(x, a, b):
        if a > b: return a < x or x < b
        
        elif a < b: return a < x and x < b
        
        else: return x !=a and x!=b

    @staticmethod
    def between_right(x, a, b):
        if a > b: return a < x or b >= x
        
        elif a < b: return a < x and b >= x
        
        else: return a !=x


    #successor sets node as its pred: notifies the successor for its predecessor
    def notify(self, addr):
        addr_id = hashing(addr)
        pred = self.get_pred()
        pred_id = hashing(pred)
        if pred == self.host or Node.between(addr_id, pred_id, self.node_id):
            self.update_predecessor(addr)
            return True
        return False

    #given a known node in the ring as help node, we find the successor of the target node
    def find_successor(self, help_node, target_node):
        try:
            url = "http://{0}/node/find_successor/{1}".format(help_node, target_node)
            request = requests.get(url)
            if request.status_code == 200:
                return request.text #5000
        except:
            return "SKATA"

    #this method is running a thread periodically and fixes the predecessor
    @staticmethod
    def stabilize_node(node):
        succ = node.get_succ()
        pred = node.get_pred()

        if succ != node.host: #node asks its successor for its predecessor 
            try:
                url = 'http://{0}/node/get_pred'.format(succ)
                r = requests.get(url)
                if r.status_code == 200:
                        x = r.text
                else:
                    print('Error getting predecessor for successor: {0}'.format(url))
                    return 
            except:
                return "Pred not updating!"
                
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


    #threads that runs periodically and checks if the predecessor is alive 
    def check_predecessor(self, interval):
        while self.stability:
            pred = self.get_pred()
            if pred != self.host:
                url = "http://{0}/".format(pred)
                try:
                    r = requests.get(url)
                    if r.status_code != 200:
                        self.update_predecessor(self.host)
                except Exception:
                    self.update_predecessor(self.host)  
            time.sleep(interval)
