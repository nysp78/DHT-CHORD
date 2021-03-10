from Node import Node
from hashlib import sha1
from threading import Thread, Lock
import requests
import sys
import time
from flask.json import JSONEncoder, JSONDecoder

class Bootstrap(Node):
    def __init__(self, ip_addr, port):
       super(Bootstrap, self).__init__(ip_addr, port)
       self.number_of_nodes = 0
       self.nodes_dict = {}


    #request from current node to join a node with address 
    def join(self, address):
        print("JOIN", self.successor, self.host)
        self.number_of_nodes+=1
        self.nodes_dict[address] = address.split(":")[1]
        node_successor = self.find_successor(self.host, address)
        url = "http://{0}/node/send_succ/{1}".format(address, node_successor) 
        request = requests.post(url) #send the successor to node with addr


        if self.successor == self.host or Bootstrap.between(sha1(address.encode()).hexdigest(), self.node_id,sha1(self.successor.encode()).hexdigest()):
           print("MPHKA")
           print("ADDRESSS", address)
           self.update_successor(address)
         
        Bootstrap.stabilize_node(self)

    def find_successor(self, help_node, target_node):
        try:
            url = "http://{0}/node/find_successor/{1}".format(help_node, target_node)
            request = requests.get(url)
            if request.status_code == 200:
                return request.text #5000
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

        return 

