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

def add_key(key):
    """
    Add key to node's store
    """
    keys.append(key)

def handle_message(data):
    """
    Decode message and determine task
    :param data: Message string
    """
    message = json.load(data)

    if 'action' in message:
        action = message['action']

        if action == 'add':
            add_key(message['key'])

def listening_thread(buffer_size):
    """
    Receive incoming messages
    """
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

def term_handler(signal):
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

def create_action(action):
    """
    Create an 'action' dictionary entry
    :param action: Type of action
    :return: Dictionary entry
    """
    return {'action':action}

def send_ack():
    """
    Send acknowledgement to coordinator thread
    """
    encoded_string = json.dumps(create_action('ack'))
    send_message(coordinator_port, encoded_string)

def main():
    """
    Initialize node variables, start listener thread, send ack to coordinator
    """
    global coordinator_port
    global node_port
    global keys
    global pred_port
    global succ_port

    signal.signal(signal.SIGTERM, term_handler)

    coordinator_port = int(sys.argv[1])
    node_port = int(sys.argv[2])
    json_data = sys.argv[3]
    keys = []

    listener = threading.Thread(target = listening_thread, args=[1024])
    listener.daemon = True
    listener.start()

    send_ack()

if __name__ == "__main__":
    main()
