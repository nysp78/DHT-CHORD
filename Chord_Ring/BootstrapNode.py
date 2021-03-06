from Node import Node, hashing
from hashlib import sha1
from threading import Thread, Lock
import requests
import sys
import time
import json
from flask.json import JSONEncoder, JSONDecoder
from config import *

class Bootstrap(Node):
    def __init__(self, ip_addr, port):
       super(Bootstrap, self).__init__(ip_addr, port)
       self.number_of_nodes = 0
       self.nodes_dict = {BOOTSTRAP_ADDR: BOOTSTRAP_ADDR.split(":")[1]}
       self.total_storage = {}


    def update_dictionaries(self, addr_key):
        print("NODE DICT, ADDR", self.nodes_dict[addr_key], addr_key)
        try:
            del self.nodes_dict[addr_key]
            del self.total_storage[addr_key]
            self.number_of_nodes -= 1
        
        except:
            return "Error of deleting from bootstrap"

    def collect_total_data(self):
        for address in self.nodes_dict.keys():
            url = "http://{0}/node/get_storage/".format(address)
            reply = requests.get(url)
            self.total_storage[address] = reply.text
        return json.dumps(self.total_storage)



    #request from current node to join a node with address 
    def join(self, address):
        self.number_of_nodes+=1
        self.nodes_dict[address] = address.split(":")[1]
        node_successor = self.find_successor(self.host, address)
        url = "http://{0}/node/send_succ/{1}".format(address, node_successor) 
        request = requests.post(url) #send the successor to node with addr
        if request.status_code!=200:
            return "Error with sending successesor"


        if self.successor == self.host or Bootstrap.between(sha1(address.encode()).hexdigest(), 
                                                                self.node_id,sha1(self.successor.encode()).hexdigest()):
           print("MPHKA")
           print("ADDRESSS", address)
           self.set_successor(address)
         
        Bootstrap.stabilize_node(self)

    
    def get_total_nodes(self):
        return "total nodes in system:" + str(self.number_of_nodes + 1)