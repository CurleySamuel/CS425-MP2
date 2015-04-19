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

"""
Following functions used for message passing
"""

def send_message(port, data):
    """
    Create socket and send data
    """
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2.connect(('', port))
    s2.send(data)
    s2.close()


def send_ack():
    """
    Send acknowledgement to coordinator thread
    """
    encoded_string = json.dumps({'action':'ACK'})
    send_message(coordinator_port, encoded_string)


def send_found_response(key_to_find, successor):
    """
    Send node containing given key to coordinator
    """
    encoded_string = json.loads({'action':'ACK','found':successor})
    send_message(coordinator_port, encoded_string)


def retrieve_successor(node):
    """
    Retrieve the successor of a given node
    """
    encoded_string = json.dumps({'action':'retrieve_successor', 'query_node':own_port})
    send_message(node)


def respond_with_successor(query_node):
    """
    Send successor information back to query node
    """
    encoded_string = json.dumps({'action':'successor retrieved','successor':successor_port})
    send_message(query_node, encoded_string)


def send_back_predecessor(key_to_find, query_node):
    """
    Send back the predecessor that was found searching the circle
    """
    encoded_string = json.dumps({'action':'predecessor_found', 'key':key_to_find, 'found':own_port})
    send_message(query_node, encoded_string)


def send_back_successor(successor, query_node):
    """
    Send back the successor that was found searching the circle
    """
    encoded_string = json.dumps({'action':'successor found','successor':successor})
    send_message(query_node, encoded_string)

"""
Following functions used to operate on other nodes
"""
def ask_arbitrary_node_for_successor(arbitrary_node, key_to_find):
    """
    Ask an arbitrary node to find the successor for the given key
    :return: successor node
    """
    encoded_message = json.dumps({'action':'find_successor', 'key':key_to_find, 'query_port':own_port})
    send_message(arbitrary_node, encoded_message)


def ask_next_node_for_predecessor(next_node, key_to_find, query_node):
    """
    Ask the next node to run find_predecessor
    """
    encoded_string = json.dumps({'action':'find_predecessor', 'key':key_to_find, 'query_node':query_node})
    send_message(next_node, encoded_string)


"""
Following functions used to listen
"""
#TODO: Condense next 3 functions into one general listening function
def listen_for_successor_query():
    """
    Waits until the successor information from a specific node is returned
    """
    conn, addr = s.accept()
    data = conn.recv(buffer_size)
    try:
        message = json.loads(data)
        if message["action"] != "successor_retrieved":
            print "Bad Successor Message..."
        return message['successor']
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
        return message['successor']
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
        return message['found']
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
            find_successor(message['key'], message['query_node'])

        elif action == 'find_predecessor':
            find_predecessor(message['key'], message['query_node'])

        elif action == 'retrieve_successor':
            respond_with_successor(message['query_node'])

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
    s.bind(('', own_port))
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
    find_successor(key_to_find, own_port)
    successor = listen_for_successor()
    send_found_response(key_to_find, successor)


def find_successor(key_to_find, query_node):
    """
    Find successor of given key
    :param key_to_find:
    :param query_node: node that initiated the query
    :return:
    """
    find_predecessor(key_to_find, query_node)
    predecessor = listen_for_predecessor()
    retrieve_successor(predecessor)
    successor = listen_for_successor_query()
    send_back_successor(successor, query_node)


def find_predecessor(key_to_find, query_node):
    """
    Finds the predecessor by moving forward in the chord circle till it finds a node whose value is less than the key \
    predecessor is greater than the given key
    :param key_to_find:
    :return:predecessor of given key
    """
    current_node = coordinator_port
    current_successor = successor_port

    if current_node <= key_to_find < current_successor: #TODO: Convert port to id numbers
        #Predecessor found!
        send_back_predecessor(key_to_find, query_node)
    else:
        next_node = closest_preceding_finger(key_to_find)
        ask_next_node_for_predecessor(next_node, key_to_find, query_node)


def closest_preceding_finger(key_to_find):
    """
    Scans the finger table from bottom to top to determine the closest known predecessor of the given key
    :param key_to_find:
    :return:closest known predecessor of given key
    """
    for i in range(m,1,-1):
        finger_node = successors[i]
        if finger_node > coordinator_port and finger_node < key_to_find:
            return finger_node
        return coordinator_port #Don't understand how it could ever reach here? Won't the key be between one of the fingertable entries?


"""
Following functions used to join nodes
"""
def calculate_finger_start(k):
    """
    Calculate start of finger - (n + 2^(k-1)) mod 2^m
    """
    return (own_port + pow(2,k-1) ) % pow(2,m)


def init_finger_table(arbitrary_node):
    """
    Use an arbitrary node to initialize the new node's finger table
    """
    finger_starts[1] = calculate_finger_start(1)
    intervals[1] = (finger_starts[1], calculate_finger_start(2))
    ask_arbitrary_node_for_successor(arbitrary_node, finger_starts[1])
    successors[1] = listen_for_successor()

    for i in range(1,m-1):
        finger_starts[i+1] = calculate_finger_start(i+1)
        intervals[i+1] = (finger_starts[i+1], calculate_finger_start(i+2))
        if own_port <= finger_starts[i+1] < successors[i]:
            successors[i+1] = successors[i]
        else:
            ask_arbitrary_node_for_successor(arbitrary_node, finger_starts[i+1])
            successors[i+1] = listen_for_successor()


def update_others():
    """
    Update the finger tables of other nodes to reflect the new node joining
    """
    for i in range(1,m):
        find_predecessor(own_port - pow(2,i-1))
        predecessor = listen_for_predecessor()
        #TODO:STILL NEED TO FINISH


def main():
    """
    Initialize node variables, send ack to coordinator, start listening
    """
    global coordinator_port #TODO: Need to remember start port for node checking logic FIX ALL ACCOMPANYING LOGIC global node_port global pred_port global succ_port global m  global keys  global finger_starts

    #Lists to hold finger table information, should probably create object for entries and store in one list
    global keys
    global finger_starts
    global intervals
    global successors

    global predecessor_port
    global successor_port
    global own_port
    global m #number of bits in ID

    signal.signal(signal.SIGTERM, term_handler)

    coordinator_port = int(sys.argv[1])
    own_port = int(sys.argv[2])
    json_data = sys.argv[3]
    arbitrary_node = sys.argv[4]

    m = 5 #TODO: Currently hardcoding identifier size
    keys = []
    finger_starts = []
    intervals = []
    successors = []

    if arbitrary_node != 0:
        init_finger_table(arbitrary_node)
        update_others()
    else:
        for i in range(1,m):
            finger_starts[i] = calculate_finger_start(i)
            intervals[i] = (finger_starts[i], calculate_finger_start(i+1))
            successors[i] = own_port

    send_ack()
    start_listening()

if __name__ == "__main__":
    main()
