import numpy as np
from config import *
import requests
import time

node_list = ["127.0.0.1:5000", "127.0.0.1:5001", "127.0.0.1:5002", "127.0.0.1:5003", "127.0.0.1:5004",
             "127.0.0.1:5005", "127.0.0.1:5006", "127.0.0.1:5007", "127.0.0.1:5008", "127.0.0.1:5009"]




"""Reads requests from filename and forwards
each of them to a random server"""

start = time.time()
insert_count = 0
with open("insert.txt", 'r') as f:
    for file_line in f:
        line = file_line.rstrip()
        commasplit = file_line.split(", ")
        no_questionmark_commasplit = commasplit[0].replace("?", "")
        no_whitespace_commasplit = no_questionmark_commasplit.replace(" ", "%20")
        chosen_node = np.random.choice(node_list)
        url = "http://{0}/node/insert_pair/{1}/{2}/{3}".format(chosen_node, no_whitespace_commasplit, commasplit[1], REPLICATION)
        try:
            reply = requests.post(url)
            if reply.status_code != 200:
                break
            insert_count += 1
            print("COUNT = ", insert_count)
        except:
            break
print ((time.time() - start) / insert_count)
