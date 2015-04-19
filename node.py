import socket
from termcolor import colored
import subprocess
import json
import time
import signal
import sys
import threading
import signal
from sys import platform

def add_keys(keys_to_add):
    """
    Add keys to node's store
    """
    keys.append(keys_to_add)

def get_id(port):
    """
    Get id of node from port
    EX: Start port is 5001, port of a certain node is 5002, then the id for that node is 2
    """
    return port - start_port + 1

def get_port(id):
    """
    Given a node id, determines the port to communicate with
    EX: Start port is 5001, id of a certain node is 2, then the port for that node is 5002
    """
    return start_port + id -1

"""
Following functions used for message passing
"""

def send_message(node_id, data):
    """
    Create socket and send data
    """
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    port = node_id
    if node_id != coordinator_port:
       port = get_port(node_id)
    s2.connect(('', port))
    s2.send(data)
    s2.close()


def send_ack():
    """
    Send acknowledgement to coordinator thread
    """
    encoded_string = json.dumps({'action':'ACK'})
    send_message(coordinator_port, encoded_string)


def send_found_response(key_to_find, successor_id):
    """
    Send node containing given key to coordinator
    """
    encoded_string = json.loads({'action':'ACK','found':successor_id})
    send_message(coordinator_port, encoded_string)


def retrieve_successor(node_id):
    """
    Retrieve the successor of a given node
    """
    encoded_string = json.dumps({'action':'retrieve_successor', 'query_node_id':self_id})
    send_message(node_id)


def respond_with_successor(query_node_id):
    """
    Send successor information back to query node
    """
    encoded_string = json.dumps({'action':'successor retrieved','successor_id':self_successor_id})
    send_message(query_node_id, encoded_string)

def retrieve_predecessor(node_id):
    """
    Retrieve the predecessor of a given node
    """
    encoded_string = json.dumps({'action':'retrieve_predecessor', 'query_node_id':self_id})
    send_message(node_id, encoded_string)

def respond_with_predecessor(query_node_id):
    """
    Send successor information back to query node
    """
    encoded_string = json.dumps({'action':'predecessor_retrieved','predecessor_id':self_predecessor_id})
    send_message(query_node_id, encoded_string)


def send_back_predecessor(key_to_find, query_node_id):
    """
    Send back the predecessor that was found searching the circle
    """
    encoded_string = json.dumps({'action':'predecessor_found', 'key':key_to_find, 'predecessor_id':self_id})
    send_message(query_node_id, encoded_string)


def send_back_successor(successor, query_node):
    """
    Send back the successor that was found searching the circle
    """
    encoded_string = json.dumps({'action':'successor_found','successor_id':successor})
    send_message(query_node, encoded_string)


"""
Following functions used to operate on other nodes
"""
def ask_arbitrary_node_for_successor(arbitrary_node_id, key_to_find):
    """
    Ask an arbitrary node to find the successor for the given key
    :return: successor node
    """
    encoded_message = json.dumps({'action':'find_successor', 'key':key_to_find, 'query_port':self_id})
    send_message(arbitrary_node_id, encoded_message)


def ask_next_node_for_predecessor(next_node_id, key_to_find, query_node_id):
    """
    Ask the next node to run find_predecessor
    """
    encoded_string = json.dumps({'action':'find_predecessor', 'key':key_to_find, 'query_node':query_node_id})
    send_message(next_node_id, encoded_string)


def ask_next_node_to_move_keys():
    """
    Ask successor to move keys in the range (predecessor, current node] to the current node
    """
    encoded_string = json.loads({'action':'move_keys', 'begin_range':self_predecessor_id, 'end_range':self_id})
    send_message(self_successor_id, encoded_string)


def notify_node_to_update_finger_table(out_of_date_node_id, new_node_id, i):
    """
    Notify a node to update it's finger table with a new node that joined in
    """
    encoded_string = json.loads({'action':'update_finger_table','new_node_id':new_node_id, 'i':i})
    send_message(out_of_date_node_id, encoded_string)


"""
Following functions used to listen for messages
"""
#TODO: Condense next 4 functions into one general listening function
def listen_for_successor_query():
    """
    Waits until the successor information of a specific node is returned
    """
    conn, addr = s.accept()
    data = conn.recv(buffer_size)
    try:
        message = json.loads(data)
        if message["action"] != "successor_retrieved":
            print "Bad Successor Message..."
        return message['successor_id']
    except Exception:
            print "Bad Successor Message..."

def listen_for_predecessor_query():
    """
    Waits until the predecessor information of a specific node is returned
    """
    conn, addr = s.accept()
    data = conn.recv(buffer_size)
    try:
        message = json.loads(data)
        if message["action"] != "predecessor_retrieved":
            print "Bad Successor Message..."
        return message['predecessor_id']
    except Exception:
            print "Bad Successor Message..."

def listen_for_successor():
    """
    Waits until the successor for a given key is found
    """
    conn, addr = s.accept()
    data = conn.recv(buffer_size)
    try:
        message = json.loads(data)
        if message["action"] != "successor_found":
            print "Bad Successor Message..."
        return message['successor_id']
    except Exception:
            print "Bad Successor Message..."


def listen_for_predecessor():
    """
    Waits until the predecessor for a given key is found
    """
    conn, addr = s.accept()
    data = conn.recv(buffer_size)
    try:
        message = json.loads(data)
        if message["action"] != "predecessor_found":
            print "Bad Predecessor Message..."
        return message['predecessor_id']
    except Exception:
            print "Bad Predecessor Message..."


def handle_message(data):
    """
    Decode message and determine task
    """
    message = json.loads(data)

    if 'action' in message:
        action = message['action']

        if action == 'force_key':
            add_keys(message['data'])
            send_ack()

        elif action == 'find':
            find(message['key'])

        elif action == 'find_successor':
            find_successor(message['key'], message['query_node_id'])

        elif action == 'find_predecessor':
            find_predecessor(message['key'], message['query_node_id'])

        elif action == 'retrieve_successor':
            respond_with_successor(message['query_node_id'])

        elif action == 'retrieve_predecessor':
            respond_with_successor(message['query_node_id'])

        elif action == 'update_finger_table':
            update_finger_table(message['new_node_id'], message['i'])

        elif action == 'update_predecessor':
            self_predecessor_id = message['predecessor_id']

        elif action == 'move_keys':
            move_keys(message['begin_range'], message['end_range'])

    start_listening()


def start_listening():
    """
    Receive incoming messages
    """
    global buffer_size
    buffer_size = 4096
    global s
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('', self_id))
    s.listen(1)

    try:
        conn, addr = s.accept()
        data = conn.recv(buffer_size)
        handle_message(data)
        #conn.close()

    except:
        pass


def term_handler(signal, frame):
    """
    Gracefully exit when encountering SIGTERM
    """
    #TODO: Still not exiting gracefully
    s.close()
    exit(0)


"""
Following functions used to find location of key in network
"""
def find(key_to_find):
    """
    Finds node that contains given key
    """
    find_successor(key_to_find, self_id)
    successor_id = listen_for_successor()
    send_found_response(key_to_find, successor_id)


def find_successor(key_to_find, query_node_id):
    """
    Find successor of given key
    :param query_node: node that initiated the query
    """
    find_predecessor(key_to_find, query_node_id)
    predecessor = listen_for_predecessor()
    retrieve_successor(predecessor)
    successor = listen_for_successor_query()
    send_back_successor(successor, query_node_id)


def find_predecessor(key_to_find, query_node_id):
    """
    Finds the predecessor by moving forward in the chord circle till it finds a node whose value is less than the key \
    predecessor is greater than the given key
    """
    current_node = self_id
    current_successor = self_successor_id

    if current_node < key_to_find <= current_successor:
        #Predecessor found!
        send_back_predecessor(key_to_find, query_node_id)
    else:
        next_node_id = closest_preceding_finger(key_to_find)
        ask_next_node_for_predecessor(next_node_id, key_to_find, query_node_id)


def closest_preceding_finger(key_to_find):
    """
    Scans the finger table from bottom to top to determine the closest known predecessor of the given key
    :return:closest known predecessor of given key
    """
    for i in range(m,1,-1):
        finger_node = successors[i]
        if self_id < finger_node < key_to_find:
            return finger_node
        return self_id#Don't understand how it could ever reach here? Won't the key be between one of the fingertable entries?


"""
Following functions used to join nodes
"""
def calculate_finger_start(k):
    """
    Calculate start of finger - (n + 2^(k-1)) mod 2^m
    """
    return (self_id + pow(2,k-1) ) % pow(2,m)

def set_predecessor(out_of_date_node_id, new_predecessor):
    """
    Update the predecessor of given node with a new node that joined
    """
    encoded_string = json.dumps({'action':'set_predecessor', 'predecessor_id':new_predecessor})
    send_message(out_of_date_node_id, encoded_string)

def init_finger_table(arbitrary_node_id):
    """
    Use an arbitrary node to initialize the new node's finger table
    """
    finger_starts[1] = calculate_finger_start(1)
    intervals[1] = (finger_starts[1], calculate_finger_start(2))
    ask_arbitrary_node_for_successor(arbitrary_node_id, finger_starts[1])
    successors[1] = listen_for_successor()
    self_successor_id = successors[1]

    retrieve_predecessor(self_successor_id)
    self_predecessor_id = listen_for_predecessor_query()
    set_predecessor(self_successor_id, self_id)

    for i in range(1,m-1):
        finger_starts[i+1] = calculate_finger_start(i+1)
        intervals[i+1] = (finger_starts[i+1], calculate_finger_start(i+2))
        if self_id <= finger_starts[i+1] < successors[i]:
            successors[i+1] = successors[i]
        else:
            ask_arbitrary_node_for_successor(arbitrary_node_id, finger_starts[i+1])
            successors[i+1] = listen_for_successor()


def update_finger_table(new_node_id, i):
    """
    Some logic to update an individual finger table, need to figure how this works
    """
    if self_id <= new_node_id < successors[i]:
        successors[i] = new_node_id
        notify_node_to_update_finger_table(self_predecessor_id, new_node_id, i)


def update_others():
    """
    Update the finger tables of other nodes to reflect the new node joining
    """
    for i in range(1,m):
        find_predecessor(self_id - pow(2,i-1))
        predecessor_id = listen_for_predecessor()
        notify_node_to_update_finger_table(predecessor_id, self_id, i)


def transfer_keys_to_predecessor(keys_to_remove):
    """
    Add these keys removed from current node to the predecessor node
    """
    encoded_string = json.dumps({'action':'force_key', 'key':keys_to_remove})
    send_message(self_predecessor_id, encoded_string)


def move_keys(begin_range, end_range):
    """
    Remove keys in range from keystore and send them to predecessor
    """
    keys_to_remove = []
    for key in keys:
        if begin_range < key < end_range:
            keys_to_remove.append(key)

    keys.remove(keys_to_remove)
    transfer_keys_to_predecessor(keys_to_remove)

def main():
    """
    Initialize node variables, send ack to coordinator, start listening
    """
    global coordinator_port #TODO: Need to remember start port for node checking logic FIX ALL ACCOMPANYING LOGIC global node_port global pred_port global succ_port global m  global keys  global finger_starts
    global start_port #Port where identifier space starts

    #Lists to hold finger table information, should probably create object for entries and store in one list
    global keys
    global finger_starts
    global intervals
    global successors

    global self_predecessor_id
    global self_successor_id
    global self_id
    global m #number of bits in ID

    signal.signal(signal.SIGTERM, term_handler)

    coordinator_port = int(sys.argv[1])
    start_port = coordinator_port + 1
    self_port = int(sys.argv[2])
    self_id = get_id(self_port)

    json_data = sys.argv[3]
    arbitrary_node_port = int(sys.argv[4]) #If initial node, expecting 0

    m = 5 #TODO: Currently hardcoding identifier size
    keys = []
    finger_starts = []
    intervals = []
    successors = []

    if arbitrary_node_port != 0:
        arbitrary_node_id = get_id(arbitrary_node_port)
        init_finger_table(arbitrary_node_id)
        update_others()
        ask_next_node_to_move_keys()
    else:
        for i in range(1,m):
            finger_starts[i] = calculate_finger_start(i)
            intervals[i] = (finger_starts[i], calculate_finger_start(i+1))
            successors[i] = self_id
        self_predecessor_id = self_id

    send_ack()
    start_listening()

if __name__ == "__main__":
    main()
