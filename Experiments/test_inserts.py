import numpy as np
import sys 
sys.path.append("/home/panos/Documents/Distributed Systems 2020/CHORDv2") #INSERT THE PROJECT PATH
from config import *
import requests
import time

node_list = [BOOTSTRAP_ADDR, NODE_ADDR1, NODE_ADDR2, NODE_ADDR3, NODE_ADDR4, 
                                        NODE_ADDR5, NODE_ADDR6, NODE_ADDR7, NODE_ADDR8, NODE_ADDR9]


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
        except:
            break
print ((time.time() - start) / insert_count)
