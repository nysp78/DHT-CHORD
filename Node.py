from hashlib import sha1
from threading import Thread, Lock
import requests
import sys
import time
from flask.json import JSONEncoder, JSONDecoder

class Node(object):
    def __init__(self, ip_addr, port):
        self.ip_addr = ip_addr
        self.port = port
        self.host = ip_addr + ":" + str(port)
        self.node_id = sha1(self.host.encode()).hexdigest()
        self.successor = self.host
        self.predecessor = self.host
        self.succ_lock = Lock() #lock to write succ
        self.pred_lock = Lock() #lock to write pred
        self.node_storage = {}
        self.stabilizer_thread = Thread(target = self.stabilize, args=(2,)) #periodic operation
        self.stabilizer_thread.start()
       #self.check_predecessor_thread = Thread(target=self.check_predecessor, args=(10,)) #periodic operation


    def get_succ(self):
        return self.successor
    
    def get_pred(self):
        return self.predecessor

    #update the successor of the this node
    def update_successor(self, res):
        self.succ_lock.acquire()
        self.successor = res
        self.succ_lock.release()
        
        if res == self.host:
            return

        else:
            pass

        return 

    def update_predecessor(self, res):
        self.pred_lock.acquire()
        self.predecessor = res
        self.pred_lock.release()

    @staticmethod
    def between(x, a, b):
        if a > b:
            return a < x or x < b
        elif a < b:
            return a < x and x < b
        else:
            return x !=a and x!=b

    @staticmethod
    def between_right_inclusive(x, a, b):
        if a > b:
            return a < x or b >= x
        elif a < b:
            return a < x and b >= x
        else:
            return a !=x

    
    def notify(self, addr):
        n_prime = sha1(addr.encode()).hexdigest()
        pred = self.get_pred()
        pred_id = sha1(pred.encode()).hexdigest()
        if pred == self.host or Node.between(n_prime, pred_id, self.node_id):
            self.update_predecessor(addr)
            return True
        return False


    @staticmethod
    def stabilize_node(node):
        succ = node.get_succ()
        pred = node.get_pred()
        print("succ:",succ,  "????", "pred:", pred)
        if succ != node.host:
            url = 'http://{0}/node/get_pred'.format(succ)
            try:
                r = requests.get(url)
                if r.status_code == 200:
                    x = r.text
                else:
                    print('Error getting predecessor for successor: {0}'.format(url))
                    return
            except Exception:
                print('Error getting predecessor for successor: {0}'.format(url))
                return
        elif pred != node.host:
            x = pred
        else:
            return
        print("PRED HERE:", x)
        x_id = sha1(x.encode()).hexdigest()
        succ_id = sha1(succ.encode()).hexdigest()
        if Node.between(x_id, node.node_id, succ_id):
            node.update_successor(x)
        url = 'http://{0}/node/notify?addr={1}'.format(node.get_succ(), node.host)
        r = requests.post(url)
        if r.status_code != 200:
            print('Error notifying successor: {0}'.format(url))


    
    def stabilize(self, interval):
        while 1:
            print("PERIODICALLY")
            Node.stabilize_node(self)
            time.sleep(interval)

    

    

    




        