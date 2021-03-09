from hashlib import sha1
from threading import Thread, Lock
import requests
import sys
import time
from flask.json import JSONEncoder, JSONDecoder

class Node(object):
    def __init__(self, ip_addr, port):
        self.ip_addr = ip_addr
        self.port = port
        self.host = ip_addr + ":" + str(port)
        self.node_id = sha1(self.host.encode()).hexdigest()
        self.successor = self.host
        self.predecessor = self.host
        self.succ_lock = Lock() #lock to write succ
        self.pred_lock = Lock() #lock to write pred
        self.node_storage = {}
        self.stabilizer_thread = Thread(target = self.stabilizer, args=(3,)) #periodic operation
        self.stabilize = True
       #self.check_predecessor_thread = Thread(target=self.check_predecessor, args=(10,)) #periodic operation


    def get_succ(self):
        return self.successor
    
    def get_pred(self):
        return self.predecessor

    def join(self, address):
        node_successor = self.find_successor(self.node_id, address)
        self.update_successor(node_successor)

    def find_successor(self, target_node, help_node):
        try:
            url = "http://{0}/node/find_successor/{1}".format(help_node, target_node)
            request = requests.get(url)
            if request.status_code == 200:
                return request.text
        except:
            raise ConnectionError


    #update the successor of the this node
    def update_successor(self, res):
        self.succ_lock.acquire()
        self.successor = res
        self.succ_lock.release()
        
        if res == self.host:
            return

        else:
            pass


    @staticmethod
    def between(x, a, b):
        if a > b:
            return a < x or x < b
        elif a < b:
            return a < x and x < b
        else:
            return x !=a and x!=b

    @staticmethod
    def between_right_inclusive(x, a, b):
        if a > b:
            return a < x or b >= x
        elif a < b:
            return a < x and b >= x
        else:
            return a !=x


    def stabilizer():
        print("pipis")


    

    

    




        