from flask import Flask, redirect, url_for, request, logging, abort
from hashlib import sha1
import os
import requests
import json
from Node import Node, hashing
from BootstrapNode import Bootstrap

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

#bootstrap tells us how many nodes we have in the system
@app.route("/node/get_total_nodes/", methods=["GET"])
def return_nodes():
    return current_node.get_total_nodes()


#bootstrap sends the succ of current node
@app.route("/node/send_succ/<succ_id>", methods=["POST"])
def send_succ(succ_id):
    current_node.update_successor(succ_id)
    return "ok", 200


@app.route("/node/get_succ/", methods=["GET"])
def return_successor():
    return current_node.get_succ()


@app.route("/node/get_pred/", methods=["GET"])
def return_predecessor():
    return current_node.get_pred()


@app.route('/node/notify', methods=['POST', 'PUT'])
def notify():
  addr = request.args.get('addr')
  updated = current_node.notify(addr)
  if updated:
    return "predecessor updated", 200
  return "predecessor not updated", 200


@app.route("/node/transfer_keys/<target_node>", methods=["POST, PUT"])
def transfer_keys(target_node):
    keys = current_node.node_storage.keys() #obtain keys
    for key in keys:
        key_unh = int(key.decode())
        if not Node.between_right(key_unh, hashing(target_node), current_node.node_id):
            trans_key = current_node.transfer_keys(key_unh, target_node)
            if trans_key:
                return "key transfered", 200
            else:
                return abort(500)


@app.route("/node/send_item/<item>", methods=["POST, PUT"])
def send_item(item):
    current_node.node_storage[item[0]] = item[1]
    return "key set", 200


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


@app.route("/node/insert_pair/<key>/<value>", methods=["POST", "PUT"])
def insert_pair(key, value):
    #perform a lookup
    successor = current_node.find_successor(current_node.host, key)

    #if current node is equal to key succ
    if successor == current_node.host:
        current_node.node_storage[key] = value
        print("SUCCESS!!!!!!!:", key, value)
        return "pair inserted to Chord", 200

    #else call insert for the successor
    else:
        url = "http://{0}/node/insert_pair/{1}/{2}".format(successor, key, value)
        res = requests.post(url)
        if res.status_code == 200:
            return "pair inserted to Chord", 200
        else:
            return "ERROR with inserting"


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



@app.route("/node/collect_total/", methods = ["GET"])
def collect_total():
   return current_node.collect_total_data()



def query_all():
    url = "http://{0}/node/collect_total/".format("127.0.0.1:5000")
    reply = requests.get(url)
    return reply.text





    




