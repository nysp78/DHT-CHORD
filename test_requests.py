import numpy as np
from config import *
import requests
import time

node_list = ["127.0.0.1:5000", "127.0.0.1:5001", "127.0.0.1:5002", "127.0.0.1:5003", "127.0.0.1:5004",
             "127.0.0.1:5005", "127.0.0.1:5006", "127.0.0.1:5007", "127.0.0.1:5008", "127.0.0.1:5009"]

start = time.time()
requests_count = 1
with open("query_reply.txt","w+") as query_reply:
    with open("requests.txt", 'r')as f:
            for file_line in f:
                line = file_line.rstrip()
                if line.startswith('query'):
                    args = line.split(", ")
                    args[1]= args[1].replace("?", "")
                    args[1] = args[1].replace(" ", "%20")
                    chosen_node = np.random.choice(node_list)
                    url = "http://{0}/node/query_key/{1}".format(chosen_node, args[1])
              # print("request:", requests_count, url)
                    reply = requests.get(url)

                    if reply.status_code != 200:
                        print("Error occured in query!")
                        break
                    query_reply.write("Request:" + str(requests_count) + " returned:" + str(reply.text) + "\r\n")

                elif line.startswith('insert'):
                    args = line.split(", ")
                    args[1]= args[1].replace("?", "")
                    args[1] = args[1].replace(" ", "%20")
                    chosen_node = np.random.choice(node_list)
                    url = "http://{0}/node/insert_pair/{1}/{2}/{3}".format(chosen_node, args[1], args[2], REPLICATION)
             #  print("request:", requests_count, url)
                    reply = requests.post(url)
                    if reply.status_code != 200:
                        print("Error occured in insert!")
                        break
            
                requests_count += 1

query_reply.close()
print((time.time() - start) / (requests_count-1))
