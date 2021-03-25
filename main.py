import run_node
import os
import sys

#init bootstrap
os.system("gnome-terminal -e 'bash -c \"python3 run_node.py -ip=127.0.0.1 -p=5000 -b=1\" '")

#init 9 others nodes
for i in range(5001,5003):
    command = "python3 run_node.py -ip=127.0.0.1 -p" + str(i) + " -b=0"
    os.system("gnome-terminal -e 'bash -c \""+ command +";bash\"'")

#9 nodes join the ring
#command_join = "curl --location --request PUT '127.0.0.1:5000/node/join'"
#os.system("gnome-terminal -e 'bash -c \""+ command_join +";bash\"'")
