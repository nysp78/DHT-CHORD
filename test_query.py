import numpy as np
from config import *
import requests
import time

node_list = ["127.0.0.1:5000", "127.0.0.1:5001", "127.0.0.1:5002", "127.0.0.1:5003", "127.0.0.1:5004",
             "127.0.0.1:5005", "127.0.0.1:5006", "127.0.0.1:5007", "127.0.0.1:5008", "127.0.0.1:5009"]




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
