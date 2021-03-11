from flask import Flask, redirect, url_for, request, logging, abort
from hashlib import sha1
import os
import requests
import json
from Node import Node, hashing
from BootstrapNode import Bootstrap
from config import *

app = Flask(__name__)
current_node = None

@app.before_first_request
def startServer():
  global current_node
  ip_addr = app.config["IP"] 
  port = app.config["PORT"]
  isboot = app.config["ISBOOT"]
  print(ip_addr, port)
  try:
    if isboot == 1:
        current_node = Bootstrap(ip_addr, port)
        print("Bootstrap created!")
    else:
        current_node = Node(ip_addr, port)
        print("A node created!")
    
    print("The server is up with id:{0}".format(current_node.node_id))
    return "server is ok!", 200
  
  except:
     return "error", abort(500)


@app.route("/", methods=["GET"])
def check_server():
    return "Server is up!!"


#add a node in the chord ring
@app.route('/node/join/', methods=['POST', 'PUT'])
def join():
    addrs = ["127.0.0.1:5001", "127.0.0.1:5002", "127.0.0.1:5003", "127.0.0.1:5004", "127.0.0.1:5005", "127.0.0.1:5006",
    "127.0.0.1:5007", "127.0.0.1:5008", "127.0.0.1:5009"]

    try:
        for addr in addrs:
            current_node.join(addr)
        return "ok", 200

    except:
        raise ConnectionAbortedError

# Gracefully departing node
@app.route("/node/depart/", methods = ["POST", "PUT"])
def depart():
    dep = current_node.leave()
    if not dep :
        return "Error while departing", 500
    url = "http://{0}/node/update_bootstrap/{1}".format(BOOTSTRAP_ADDR, current_node.host)
    reply = requests.post(url)
    if reply.status_code != 200 :
        return "Error updating boot's dict", 500
    current_node.shutdown()
    return "Node departed", 200

# Updates Bootstrap's dictionary when a node gracefully departs from DHT Chord
@app.route("/node/update_bootstrap/<addr>", methods = ["POST", "PUT"])
def update_bootstrap(addr):
    current_node.update_dictionaries(addr)
    return "Bootstrap updated", 200

#bootstrap tells us how many nodes we have in the system
@app.route("/node/get_total_nodes/", methods=["GET"])
def return_nodes():
    return current_node.get_total_nodes()


#bootstrap sends the succ of current node
@app.route("/node/send_succ/<succ_id>", methods=["POST"])
def send_succ(succ_id):
    current_node.update_successor(succ_id)
    return "ok", 200

#Update successor pointer when a nodes departs gracefully from DHT Chord
@app.route("/node/set_successor/<succ_id>", methods = ["POST", "PUT"])
def set_succ(succ_id):
    current_node.set_successor(succ_id)
    return "Successor set", 200

#Update predecessor pointer when a nodes departs gracefully from DHT Chord
@app.route("/node/set_predecessor/<pred_id>", methods = ["POST", "PUT"])
def set_pred(pred_id):
    current_node.update_predecessor(pred_id)
    return "Predecessor set", 200

#Returns successor
@app.route("/node/get_succ/", methods=["GET"])
def return_successor():
    return current_node.get_succ()

#Returns predecessor
@app.route("/node/get_pred/", methods=["GET"])
def return_predecessor():
    return current_node.get_pred()

#Notifies the successor about its predecessor (call in Stabilize())
@app.route('/node/notify', methods=['POST', 'PUT'])
def notify():
  addr = request.args.get('addr')
  updated = current_node.notify(addr)
  if updated:
    return "predecessor updated", 200
  return "predecessor not updated", 200

#Transfer keys from the successor to the node
@app.route("/node/transfer_keys/<target_node>", methods=["POST, PUT"])
def transfer_keys(target_node):
    keys = current_node.node_storage.keys() #obtain keys
    for key in keys:
        key_unh = int(key.decode())
        if not Node.between_right(key_unh, hashing(target_node), current_node.node_id):
            trans_key = current_node.transfer_keys(key_unh, target_node)
            if trans_key:
                return "Key transferred", 200
            else:
                return abort(500)

#Update node's storage with key:value
@app.route("/node/send_item/<key>/<value>", methods=["POST", "PUT"])
def send_item(key,value):
    current_node.node_storage[key] = value
    return "key set", 200

#Returns successor of the node given a helper node inside the DHT Chord
@app.route("/node/find_successor/<target_node>", methods=["GET"])
def find_successor(target_node):
    successor = current_node.get_succ()
    hash_succ = sha1(successor.encode()).hexdigest() #hash succ
    target_id = sha1(target_node.encode()).hexdigest()
    if Node.between_right(target_id, current_node.node_id, hash_succ):
        return successor

    else:
       url = "http://{0}/node/find_successor/{1}".format(successor,target_node) 
       r = requests.get(url)
       return r.text

#Inserts pair (key:value) into the DHT Chord
@app.route("/node/insert_pair/<key>/<value>", methods=["POST", "PUT"])
def insert_pair(key, value):
    #perform a lookup
    successor = current_node.find_successor(current_node.host, key)

    #if current node is equal to key succ
    if successor == current_node.host:
        current_node.node_storage[key] = value
        print("SUCCESS!!!!!!!:", key, value)
        url = "http://{0}/node/send_node_storage/{1}/{2}".format(BOOTSTRAP_ADDR, current_node.host, current_node.node_storage)
        reply = requests.post(url)
        if reply.status_code != 200 :
            return "Error in sending the mf dictionary", 500
        return "pair inserted to Chord", 200

    #else call insert for the successor
    else:
        url = "http://{0}/node/insert_pair/{1}/{2}".format(successor, key, value)
        res = requests.post(url)
        if res.status_code == 200:
            return "pair inserted to Chord", 200
        else:
            return "ERROR with inserting"

#Updates Bootstrap's total storage attribute called in Insert function
@app.route("/node/send_node_storage/<addr>/<diction>", methods = ["POST", "PUT"])
def send_node_storage(addr, diction):
    current_node.total_storage[addr] = diction
    return "Bootstrap updated [node's] storage", 200

#Returns value based on given key
@app.route("/node/query_key/<key>", methods=["GET"])
def query_key(key):
    if key == "*":
        return query_all()
    
    else:
        if key in current_node.node_storage.keys():
            value = current_node.node_storage[key]
            return value, 200

        else:
            key_succ = current_node.find_successor(current_node.host, key)
            if key_succ == current_node.host:
                return "key Not Found", 500
        
            else:
            #      print("SUCCC:",key_succ, current_node.host)
                url = "http://{0}/node/query_key/{1}".format(key_succ, key)
                reply = requests.get(url)

                if reply.status_code == 200:
                    return reply.text, 200
            
                else:
                    return "Key not found" , 500
    

#return the node storage 
@app.route("/node/get_storage/", methods=["GET"])
def get_storage():
    return current_node.get_storage()


#Helper function for query_all()
@app.route("/node/collect_total/", methods = ["GET"])
def collect_total():
   return current_node.collect_total_data()

#Delete key:value from DHT Chord
@app.route("/node/delete/<key>", methods = ["DELETE"])
def delete(key):
    if key in current_node.node_storage.keys():
        current_node.node_storage.pop(key)
        return "Key deleted", 200
    else:
        key_succ = current_node.find_successor(current_node.host, key)
        if key_succ == current_node.host:
            return "key Not Found", 500
        else:
            url = "http://{0}/node/delete/{1}".format(key_succ, key) 
            reply = requests.delete(url)
            if reply.status_code == 200:
                return "Key deleted", 200
            else:
                return "Key does not exist", 500


#return the topology of the chord ring -> a list node
@app.route("/node/net_overlay/", methods=["GET"])
def net_overlay():
    nodes = []
    nodes.append(current_node.host) #insert the first node
    succ = current_node.get_succ()
    nodes.append(succ) #insert succ of current node
    while succ!=nodes[0]:
        url = "http://{0}/node/get_succ/".format(succ)
        reply = requests.get(url)
        if reply.status_code==200:
            succ = reply.text
            nodes.append(reply.text)  
        else:
            return "error" , 500
    
    return json.dumps(nodes)


#Collects all nodes' storages
def query_all():
    url = "http://{0}/node/collect_total/".format(BOOTSTRAP_ADDR)
    reply = requests.get(url)
    return reply.text