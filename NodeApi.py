from flask import Flask, redirect, url_for, request, logging, abort
from hashlib import sha1
import os
import requests
from Node import Node
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
    "127.0.0.1:5007", "127.0.0.1:5008", "127.0.0.1:5009", "127.0.0.1:5010"]

    try:
        for addr in addrs:
            current_node.join(addr)
        return "ok", 200

    except:
        raise ConnectionAbortedError

     
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


