from flask import Flask, redirect, url_for, request, logging, abort
from hashlib import sha1
import os
import requests
from Node import Node

app = Flask(__name__)
current_node = None

@app.route("/")
def startup():
  global current_node
  ip_addr = app.config["IP"]
  port = app.config["PORT"]
  print(ip_addr, port)
  try:
    current_node = Node(ip_addr, port)
    print("The server is up with id:{0}".format(current_node.node_id))
    return "server is ok", 200
  except:
     return "error", abort(500)



#add a node in the chord ring
@app.route('/node/join/', methods=['POST', 'PUT'])
def join():
    addrs = ["127.0.0.1:5001"]
    print(current_node)
    try:
        for addr in addrs:
            current_node.join(addr)
        return "ok", 200

    except:
        raise ConnectionAbortedError

     


@app.route("/node/get_succ/", methods=["GET"])
def return_successor():
    return current_node.get_succ()

@app.route("/node/find_successor/<target_id>", methods=["GET"])
def find_successor(target_id):
    successor = current_node.get_succ()
    hash_succ = sha1(successor.encode()).hexdigest() #hash succ
    if Node.between_right_inclusive(target_id, current_node.node_id, hash_succ):
        return successor

    else:
       url = "http://{0}/node/find_successor/{1}".format(successor,target_id) 
       r = requests.get(url)
       return r.text


