import numpy as np
from config import *
import requests
import time

node_list = [BOOTSTRAP_ADDR, NODE_ADDR1, NODE_ADDR2, NODE_ADDR3, NODE_ADDR4, 
                                        NODE_ADDR5, NODE_ADDR6, NODE_ADDR7, NODE_ADDR8, NODE_ADDR9]




"""Reads requests from filename and forwards
each of them to a random server"""

start = time.time()
query_count = 0
with open("query.txt", 'r') as f:
    for file_line in f:
        line = file_line.rstrip()
        line = line.replace("?", "")
        line = line.replace(" ", "%20")
        chosen_node = np.random.choice(node_list)
        url = "http://{0}/node/query_key/{1}".format(chosen_node, line)
        try:
            reply = requests.get(url)
            if reply.status_code != 200:
                break
            query_count += 1
        except:
            print("An error occured")
            break
print ((time.time() - start) / query_count)
