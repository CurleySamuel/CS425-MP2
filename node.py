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

def send_found_message():
    """
    Send successor of current node because that will be where the key is located
    """
    encoded_string = json.dumps({'action':'ACK', 'found':str(succ_port)})
    send_message(coordinator_port, encoded_string)

def ask_next_node_for_successor(next_node, key_to_find):
    """
    Ask the next node to run find_successor
    """
    encoded_string = json.dumps({'action':'FIND', 'key':key_to_find})
    send_message(next_node, encoded_string)


def find_successor(key_to_find):
    """
    Finds the predecessor by moving forward in the chord circle till it finds a node whose value is less than the key \
    successor is greater than the given key
    :param key_to_find:
    :return:predecessor of given key
    """
    current_node = coordinator_port
    current_successor = succ_port

    if key_to_find > current_node and key_to_find < current_successor: #TODO: Convert port to id numbers
        #Predecessor found!
        send_found_message()#TODO:Return successor
    else:
        next_node = closest_preceding_finger(key_to_find)
        ask_next_node_for_successor(next_node, key_to_find)

def closest_preceding_finger(key_to_find):
    """
    Scans the finger table from bottom to top to determine the closest known predecessor of the given key
    :param key_to_find:
    :return:closest known predecessor of given key
    """
    for i in range(m,1,-1):
        finger_node = succesors[i]
        if finger_node > coordinator_port and finger_node < key_to_find:
            return finger_node
        return coordinator_port #Don't understand how it could ever reach here? Won't the key be between one of the fingertable entries?


def add_keys(keys_to_add):
    """
    Add key to node's store
    """
    keys.append(keys_to_add)

def handle_message(data):
    """
    Decode message and determine task
    :param data: Message string
    """
    message = json.loads(data)

    if 'action' in message:
        action = message['action']

        if action == 'force_key':
            add_keys(message['data'])
            send_ack()

def start_listening():
    """
    Receive incoming messages
    """
    buffer_size = 4096
    global s
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('', node_port))
    s.listen(1)
    while 1:
        conn, addr = s.accept()
        data = conn.recv(buffer_size)
        handle_message(data)
        conn.close()

def term_handler(signal, frame):
    """
    Gracefully exit when encountering SIGTERM
    """
    s.close()
    exit(0)

def send_message(port, data):
    """
    Create socket and send data
    """
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2.connect(('', coordinator_port))
    s2.send(data)
    s2.close()

def send_ack():
    """
    Send acknowledgement to coordinator thread
    """
    encoded_string = json.dumps({'action':'ACK'})
    send_message(coordinator_port, encoded_string)

def main():
    """
    Initialize node variables, start listener thread, send ack to coordinator
    """
    global coordinator_port #TODO: Need to remember start port for node checking logic
    global node_port
    global pred_port
    global succ_port
    global m

    global keys

    global finger_starts
    global intervals
    global succesors

    signal.signal(signal.SIGTERM, term_handler)

    coordinator_port = int(sys.argv[1])
    node_port = int(sys.argv[2])
    json_data = sys.argv[3]
    m = 5 #TODO: Currently hardcoding identifier size
    keys = []
    finger_starts = []
    intervals = []
    successors = []

    send_ack()
    start_listening()


if __name__ == "__main__":
    main()
