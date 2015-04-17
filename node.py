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
    keys.append(key)

def handle_message(data):
    message = json.load(data)

    if 'action' in message:
        action = message['action']

        if action == 'add':
            add_key(message['key'])

def listening_thread(buffer_size):
    global s
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((TCP_IP, node_port))
    s.listen(1)
    while 1:
        conn, addr = s.accept()
        data = conn.recv(buffer_size)
        handle_message(data)
        conn.close()

def term_handler(signal):
    s.close()
    exit(0)

def main():
    global TCP_IP
    global coordinator_port
    global node_port
    global keys
    global pred_port
    global succ_port

    signal.signal(signal.SIGTERM, term_handler)

    if platform == "darwin":
        TCP_IP = socket.gethostbyname(socket.gethostname())
    elif platform == "linux" or platform == "linux2":
        TCP_IP = '10.0.0.6'
    else:
        print "Operating system not supported"
        exit(1)

    coordinator_port = sys.argv[1]
    node_port = sys.argv[2]
    json_data = sys.argv[3]
    keys = []

    listener = threading.Thread(target = listening_thread, args=[1024])
    listener.daemon = True
    listener.start()


if __name__ == "__main__":
    main()
