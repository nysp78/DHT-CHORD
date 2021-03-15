from flask import Flask, redirect, url_for, request, logging, abort
from hashlib import sha1
import os
import requests
import json
import time
from threading import Thread, Lock
from Node import Node, hashing
from BootstrapNode import Bootstrap
from config import *

app = Flask(__name__)
current_node = None

#Starting DHT server
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

#Checking if server is alive
@app.route("/", methods=["GET"])
def check_server():
    return "Server is up!!"


# add a node in the chord ring
@app.route('/node/join/', methods=['POST', 'PUT'])
def join():
    
    #a list of nodes that are inserted in chord
    addrs = [NODE_ADDR1, NODE_ADDR2, NODE_ADDR3, NODE_ADDR4, 
                                    NODE_ADDR5, NODE_ADDR6, NODE_ADDR7, NODE_ADDR8, NODE_ADDR9]

    try:
        for addr in addrs:
            current_node.join(addr)
        return "ok\n", 200

    except:
        raise ConnectionAbortedError



# Gracefully departing node
@app.route("/node/depart/", methods=["POST", "PUT"])
def depart():
    dep = current_node.leave()
    if not dep:
        return "Error while departing", 500
    url = "http://{0}/node/update_bootstrap/{1}".format(
        BOOTSTRAP_ADDR, current_node.host)
    reply = requests.post(url)
    if reply.status_code != 200:
        return "Error updating boot's dict\n", 500
    current_node.shutdown()
    return "Node departed\n", 200



# Updates Bootstrap's dictionary when a node gracefully departs from DHT Chord
@app.route("/node/update_bootstrap/<addr>", methods=["POST", "PUT"])
def update_bootstrap(addr):
    current_node.update_dictionaries(addr)
    return "Bootstrap updated", 200



# bootstrap tells us how many nodes we have in the system
@app.route("/node/get_total_nodes/", methods=["GET"])
def return_nodes():
    return current_node.get_total_nodes()


# bootstrap sends the succ of current node
@app.route("/node/send_succ/<succ_id>", methods=["POST"])
def send_succ(succ_id):
    current_node.update_successor(succ_id)
    return "ok", 200



# Update successor pointer when a nodes departs gracefully from DHT Chord
@app.route("/node/set_successor/<succ_id>", methods=["POST", "PUT"])
def set_succ(succ_id):
    current_node.set_successor(succ_id)
    return "Successor set", 200



# Update predecessor pointer when a nodes departs gracefully from DHT Chord
@app.route("/node/set_predecessor/<pred_id>", methods=["POST", "PUT"])
def set_pred(pred_id):
    current_node.update_predecessor(pred_id)
    return "Predecessor set", 200



# Returns successor
@app.route("/node/get_succ/", methods=["GET"])
def return_successor():
    return current_node.get_succ()



# Returns predecessor
@app.route("/node/get_pred/", methods=["GET"])
def return_predecessor():
    return current_node.get_pred()



# Notifies the successor about its predecessor (call in Stabilize())
@app.route('/node/notify', methods=['POST', 'PUT'])
def notify():
    addr = request.args.get('addr')
    updated = current_node.notify(addr)
    if updated:
        return "predecessor updated", 200
    return "predecessor not updated", 200



# Transfer keys from the successor to the node
@app.route("/node/transfer_keys/<target_node>", methods=["POST, PUT"])
def transfer_keys(target_node):
    keys = current_node.node_storage.keys()  # obtain keys
    for key in keys:
        key_unh = int(key.decode())
        if not Node.between_right(key_unh, hashing(target_node), current_node.node_id):
            trans_key = current_node.transfer_keys(key_unh, target_node)
            if trans_key:
                return "Key transferred", 200
            else:
                return abort(500)

#Overwrite key:value:replica
@app.route("/node/send_item/<key>/<value>", methods=["POST", "PUT"])
def send_item(key, value):
    current_node.node_storage[key] = value
    return "key set", 200



#Keeps the replication factor and stores it with the key:value[0] it receives
@app.route("/node/send_repl_item/<key>/<value>", methods=["POST", "PUT"])
def send_repl_item(key, value):
    repl = current_node.node_storage[key].split(":")[1]
    current_node.node_storage[key] = value.split(":")[0] + ":" + repl
    return "key set", 200

#Preserves the number of replicas by moving forward the keys
@app.route("/node/eventually_transfer/<key>/<value>", methods = ["POST","PUT"])
def eventually_transfer(key, value):
    if key in current_node.node_storage.keys():
        before_overwritten = current_node.node_storage[key]
        if value.split(":")[1] < before_overwritten.split(":")[1]:
            return "Terminate loop", 200
        current_node.node_storage[key] = value
        succ = current_node.get_succ()
        url = "http://{0}/node/eventually_transfer/{1}/{2}".format(succ, key, before_overwritten)
        reply = requests.post(url)
        if reply.status_code == 200 :
            return "Node updated with replica", 200
        else :
            return "Error!!!!!!!!!!!!!!!!!!!", 500
    else:
        current_node.node_storage[key] = value
        return "New tail updated", 200




# Returns successor of the node given a helper node inside the DHT Chord
@app.route("/node/find_successor/<target_node>", methods=["GET"])
def find_successor(target_node):
    successor = current_node.get_succ()
    if current_node.host == successor:
        return current_node.host
    hash_succ = sha1(successor.encode()).hexdigest()  # hash succ
    target_id = sha1(target_node.encode()).hexdigest()
    if Node.between_right(target_id, current_node.node_id, hash_succ):
        return successor

    else:
        url = "http://{0}/node/find_successor/{1}".format(
            successor, target_node)
        r = requests.get(url)
        return r.text



# Inserts pair (key:value:replicas) into the DHT Chord
@app.route("/node/insert_pair/<key>/<value>/<replicas>", methods=["POST", "PUT"])
def insert_pair(key, value, replicas):

    if CONSISTENCY == "chain":
        # perform a lookup
        successor = current_node.find_successor(current_node.host, key)

        # if current node is equal to key succ
        if successor == current_node.host:
            current_node.node_storage[key] = value + ":" + replicas
            url = "http://{0}/node/send_node_storage/{1}/{2}".format(
                BOOTSTRAP_ADDR, current_node.host, current_node.node_storage)
            reply = requests.post(url)
            if reply.status_code != 200:
                return "Error in sending the dictionary\n", 500
            if int(replicas) == 1:
                return "Replicas inserted to Chord\n", 200
            elif int(replicas) > 1:
                succ = current_node.get_succ()
                url = "http://{0}/node/store_replicas/{1}/{2}/{3}".format(
                    succ, key, value, replicas)
                reply = requests.post(url)
                if reply.status_code != 200:
                    return "Error while inserting replicas\n", 500
                return reply.text, 200

        # else call insert for the successor
        else:
            url = "http://{0}/node/insert_pair/{1}/{2}/{3}".format(
                successor, key, value, replicas)
            res = requests.post(url)
            if res.status_code == 200:
                return "Replicas inserted to Chord\n", 200
            else:
                return "ERROR with inserting", 500

    elif CONSISTENCY == "eventual" :
        successor = current_node.find_successor(current_node.host, key)
        if successor == current_node.host:
            current_node.node_storage[key] = value + ":" + replicas
            url = "http://{0}/node/send_node_storage/{1}/{2}".format(
                BOOTSTRAP_ADDR, current_node.host, current_node.node_storage)
            reply = requests.post(url)
            if reply.status_code != 200:
                return "Error in sending the dictionary\n", 500
            if int(replicas) == 1:
                return "Replicas inserted to Chord\n", 200
            else :
                succ = current_node.get_succ()
                replicaThread = Thread(target = eventual_threading_replicas_storing, args=(key, value, replicas, succ))
                #time.sleep(3)
                replicaThread.start()
                return "Primary replica inserted to Chord\n", 200

        else:
            url = "http://{0}/node/insert_pair/{1}/{2}/{3}".format(
                successor, key, value, replicas)
            res = requests.post(url)
            if res.status_code == 200:
                return res.text, 200
            else:
                return "ERROR with inserting", 500   


#Stores to k-1 nodes the key:value:replicas tuples with the help of thread
def eventual_threading_replicas_storing(key, value, replicas, succ_addr):
    time.sleep(3)
    url = "http://{0}/node/store_replicas/{1}/{2}/{3}".format(succ_addr, key, value, replicas)
    reply = requests.post(url)
    if reply.status_code != 200:
        return "Error while storing replicas", 500
    else:
        return "Ok", 200

#Stores to k-1 nodes the key:value:replicas tuples used from chain replication and eventual consistency
@app.route("/node/store_replicas/<key>/<value>/<replicas>", methods=["POST", "PUT"])
def store_replicas(key, value, replicas):
    replicas = int(replicas)
    replicas -= 1
    if int(replicas) > 0:
        current_node.node_storage[key] = value + ":" + str(replicas)
        url = "http://{0}/node/send_node_storage/{1}/{2}".format(
            BOOTSTRAP_ADDR, current_node.host, current_node.node_storage)
        reply = requests.post(url)
        if reply.status_code != 200:
            return "Error in sending the dictionary\n", 500
        succ = current_node.get_succ()
        if int(replicas) > 1:
            url = "http://{0}/node/store_replicas/{1}/{2}/{3}".format(
                succ, key, value, replicas)
            reply = requests.post(url)
    return "Replicas inserted to chord\n", 200


# Updates Bootstrap's total storage attribute called in Insert function
@app.route("/node/send_node_storage/<addr>/<diction>", methods=["POST", "PUT"])
def send_node_storage(addr, diction):
    current_node.total_storage[addr] = diction
    return "Bootstrap updated [node's] storage", 200



# Returns value based on given key
@app.route("/node/query_key/<key>", methods=["GET"])
def query_key(key):
    if key == "*":
        return query_all()

    else:
        if CONSISTENCY == "chain":
            if key in current_node.node_storage.keys():
                value = current_node.node_storage[key]

                if int(value.split(":")[1]) == 1:
                    return json.dumps(value), 200

                else:
                    succ = current_node.get_succ()
                    url = "http://{0}/node/find_tail/{1}".format(succ, key)
                    reply = requests.get(url)
                    if reply.status_code == 200:
                        return reply.text, 200
                    else:
                        return "ERROR in query"

            else:
                key_succ = current_node.find_successor(current_node.host, key)
                if key_succ == current_node.host:
                    return "key Not Found\n", 200

                else:
                    #      print("SUCCC:",key_succ, current_node.host)
                    url = "http://{0}/node/query_key/{1}".format(key_succ, key)
                    reply = requests.get(url)

                    if reply.status_code == 200:
                        return reply.text, 200

                    else:
                        return "Key not found\n", 200

        elif CONSISTENCY == "eventual":
            if key in current_node.node_storage.keys():
                value = current_node.node_storage[key]
                return json.dumps(value), 200
            else:
                key_succ = current_node.find_successor(current_node.host, key)
                if key_succ == current_node.host:
                    return "key Not Found\n", 200

                else:
                    #      print("SUCCC:",key_succ, current_node.host)
                    url = "http://{0}/node/query_key/{1}".format(key_succ, key)
                    reply = requests.get(url)

                    if reply.status_code == 200:
                        return reply.text, 200

                    else:
                        return "Key not found\n", 200



@app.route("/node/find_tail/<key>", methods=["GET"])
def find_tail(key):
    value = current_node.node_storage[key]
    if int(value.split(":")[1]) == 1:
        return json.dumps(value), 200
    
    else:
        succ = current_node.get_succ()
        url = "http://{0}/node/find_tail/{1}".format(succ, key)
        reply = requests.get(url)
        if reply.status_code == 200:
            return "find tail", 200
        else:
            return "Not Find the tail", 500


# return the node storage
@app.route("/node/get_storage/", methods=["GET"])
def get_storage():
    return current_node.get_storage()

# Returns Bootsrap's dictionary of alive nodes
@app.route("/node/get_boot_dict/", methods=["GET"])
def get_boot_dict():
    return json.dumps(current_node.nodes_dict)


# Helper function for query_all()
@app.route("/node/collect_total/", methods=["GET"])
def collect_total():
    return current_node.collect_total_data()



# Delete key:value from DHT Chord
@app.route("/node/delete/<key>", methods=["DELETE"])
def delete(key):

    succ = current_node.find_successor(current_node.host, key)
    url = "http://{0}/node/delete_replicas/{1}".format(succ, key)
    reply = requests.delete(url)
    if reply.status_code == 200:
        return "Key deleted succesfully\n"
    else:
        return "ERROR with key deletion", 500

#Helper function to delete replicas
@app.route("/node/delete_replicas/<key>", methods=["DELETE"])
def delete_replicas(key):
    replica = current_node.node_storage[key].split(":")[1]
    replicas = int(replica)
    if replicas > 0:
        current_node.node_storage.pop(key)
        url = "http://{0}/node/send_node_storage/{1}/{2}".format(
            BOOTSTRAP_ADDR, current_node.host, current_node.node_storage)
        reply = requests.post(url)
        #print("BOOTSTRAPS REPLY2", reply.text)
        if reply.status_code != 200:
            return "Error in sending the dictionary\n", 500
        succ = current_node.get_succ()
        if int(replicas) > 1:
            url = "http://{0}/node/delete_replicas/{1}".format(succ, key)
            reply = requests.delete(url)
            #print("REPLICATION > 1!!!!", reply.text)
    return "Replicas deleted from chord\n", 200


# return the topology of the chord ring -> a list node
@app.route("/node/net_overlay/", methods=["GET"])
def net_overlay():
    nodes = []
    nodes.append(current_node.host)  # insert the first node
    succ = current_node.get_succ()
    nodes.append(succ)  # insert succ of current node
    while succ != nodes[0]:
        url = "http://{0}/node/get_succ/".format(succ)
        reply = requests.get(url)
        if reply.status_code == 200:
            succ = reply.text
            nodes.append(reply.text)
        else:
            return "error", 500

    return json.dumps(nodes)


# Collects all nodes' storages
def query_all():
    url = "http://{0}/node/collect_total/".format(BOOTSTRAP_ADDR)
    reply = requests.get(url)
    return reply.text