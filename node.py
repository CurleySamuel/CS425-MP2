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
    keys.extend(keys_to_add)

def is_in_range_left_inclusive(value, beginning, end):
    """
    Check if value is in circular range
    """
    
    if beginning < end:
        return beginning <= value < end
    elif beginning > end:
        return not is_in_range_left_inclusive(value, end, beginning)
    else:
        return True

def is_in_range_right_inclusive(value, beginning, end):
    """
    Check if value is in circular range
    """
    
    if beginning < end:
        return beginning < value <= end
    elif beginning > end:
        return not is_in_range_right_inclusive(value, end, beginning)
    else:
        return True

def is_in_range_both_exclusive(value, beginning, end):
    """
    Check if value is in circular range
    """
    
    if beginning < end:
        return beginning < value < end
    elif beginning > end:
        return not is_in_range_both_inclusive(value, end, beginning)
    else:
        if value == beginning:
            return False 
        return True

def is_in_range_both_inclusive(value, beginning, end):
    """
    Check if value is in circular range
    """
    
    if beginning < end:
        return beginning <= value <= end
    elif beginning > end:
        return not is_in_range_both_exclusive(value, end, beginning)
    else:
        return True 


def get_id(port):
    """
    Get id of node from port
    EX: Start port is 5000, port of a certain node is 5001, then the id for that node is 1
    """
    return port - start_port

def get_port(id):
    """
    Given a node id, determines the port to communicate with
    EX: Start port is 5000, id of a certain node is 2, then the port for that node is 5002
    """
    return start_port + id

"""
Following functions used for message passing
"""

def send_message(node_id, data):
    """
    Create socket and send data
    """
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    port = node_id
    if node_id != coordinator_port:
       port = get_port(node_id)

    #try:
    print str(self_id) + "sending " + str(data) + " to " + str(port)
    s2.connect(('', port))
    s2.send(data)
    s2.close()
    print str(self_id) + "sent " + str(data) + " to " + str(get_id(port))


    #except:
    #    print "Error sending message from node " + self_id


def send_ack(data=None):
    """
    Send acknowledgement to coordinator thread
    """
    encoded_string = json.dumps({'action':'ACK', 'data':data})
    send_message(coordinator_port, encoded_string)


def send_found_response(key_to_find, successor_id):
    """
    Send node containing given key to coordinator
    """
    encoded_string = json.dumps({'action':'ACK','found':successor_id})
    send_message(coordinator_port, encoded_string)


def retrieve_successor(node_id):
    """
    Retrieve the successor of a given node
    """
    encoded_string = json.dumps({'action':'retrieve_successor', 'query_node_id':self_id})
    send_message(node_id, encoded_string)


def respond_with_successor(query_node_id):
    """
    Send successor information back to query node
    """
    encoded_string = json.dumps({'action':'successor_retrieved','successor_id':successors[1]})
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
    print str(self_id) + "sending send-back_predecessor message"


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
    encoded_message = json.dumps({'action':'find_successor', 'key':key_to_find, 'query_node_id':self_id})
    send_message(arbitrary_node_id, encoded_message)
    print str(self_id) + " asked arbitrary node for successor"



def ask_next_node_for_predecessor(next_node_id, key_to_find, query_node_id):
    """
    Ask the next node to run find_predecessor
    """
    encoded_string = json.dumps({'action':'find_predecessor', 'key':key_to_find, 'query_node_id':query_node_id})
    send_message(next_node_id, encoded_string)
    print str(self_id) + "sent ask-node message to " + str(next_node_id)


def ask_next_node_to_move_keys():
    """
    Ask successor to move keys in the range (predecessor, current node] to the current node
    """
    encoded_string = json.dumps({'action':'move_keys', 'begin_range':self_predecessor_id, 'end_range':self_id})
    send_message(successors[1], encoded_string)


def notify_node_to_update_finger_table(out_of_date_node_id, new_node_id, i, query_node_id):
    """
    Notify a node to update it's finger table with a new node that joined in
    """
    encoded_string = json.dumps({'action':'update_finger_table','new_node_id':new_node_id, 'i':i, 'query_node_id':query_node_id})
    send_message(out_of_date_node_id, encoded_string)


"""
Following functions used to listen for messages
"""
#TODO: Condense next 4 functions into one general listening function
def listen_for_successor_query():
    """
    Waits until the successor information of a specific node is returned
    """
    print str(self_id) + "Listening for successor query"
    conn, addr = s.accept()
    data = conn.recv(buffer_size)
    try:
        message = json.loads(data)
        if message["action"] != "successor_retrieved":
            print 'bad successor query message...'
            print data 
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
            print "Bad predecessor query Message..."
            print data
        return message['predecessor_id']
    except Exception:
            print "Bad predecessor query Message..."

def listen_for_successor():
    """
    Waits until the successor for a given key is found
    """
    print str(self_id) + "listening for successor"
    conn, addr = s.accept()
    data = conn.recv(buffer_size)
    print str(self_id) + "successor data received"
    try:
        message = json.loads(data)
        if message["action"] != "successor_found":
            print "Bad Successor Message..."
            print data
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
            print data
        return message['predecessor_id']
    except Exception:
            print "Bad Predecessor Message..."


def wait_for_key_transfer_to_complete():
    """
    Waits until all the keys have been transferred
    """
    conn, addr = s.accept()
    data = conn.recv(buffer_size)
    try:
        message = json.loads(data)
        if message["action"] != "force_key":
            print "Bad Key Transfer Message..."
        add_keys(message['data'])
        return 
    except Exception:
            print "Bad Key Transfer Message..."


def handle_message(data):
    """
    Decode message and determine task
    """
    message = json.loads(data)
    print str(self_id) + "handling message - " + data

    if 'action' in message:
        action = message['action']

        if action == 'force_key':
            add_keys(message['data'])
            send_ack()

        elif action == 'locate':
            find(message['key'])

        elif action == 'find_successor':
            find_successor(message['key'], message['query_node_id'])

        elif action == 'find_predecessor':
            find_predecessor(message['key'], message['query_node_id'])

        elif action == 'retrieve_successor':
            respond_with_successor(message['query_node_id'])

        elif action == 'retrieve_predecessor':
            respond_with_predecessor(message['query_node_id'])

        elif action == 'update_finger_table':
            update_finger_table(message['new_node_id'], message['i'], message['query_node_id'])

        elif action == 'set_predecessor':
            global self_predecessor_id
            self_predecessor_id = message['predecessor_id']
            print str(self_id) + " predecessor set to {}".format(self_predecessor_id)


        elif action == 'move_keys':
            move_keys(message['begin_range'], message['end_range'])

        elif action == 'list':
            send_ack(keys)

    start_listening()


def start_listening():
    """
    Receive incoming messages
    """
    while 1:
        conn, addr = s.accept()
        data = conn.recv(buffer_size)
        handle_message(data)

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
    successor_id = find_successor(key_to_find, self_id)
    send_found_response(key_to_find, successor_id)


def find_successor(key_to_find, query_node_id):
    """
    Find successor of given key
    :param query_node: node that initiated the query
    """
    if self_predecessor_id == self_id or successors[1] == self_id:
        send_back_successor(self_id, query_node_id)
        return

    find_predecessor(key_to_find, self_id)#query_node_id)
    predecessor = listen_for_predecessor()
    print str(self_id) + "predecessor found - " + str(predecessor)
    successor = None
    print str(self_id) + "finding successor"
    if predecessor != self_id:
        retrieve_successor(predecessor)
        successor = listen_for_successor_query()
    else:
        successor = successors[1]
    print str(self_id) + "successor found " + str(successor)
    if query_node_id == self_id:
        return successor
    send_back_successor(successor, query_node_id)


def find_predecessor(key_to_find, query_node_id):
    """
    Finds the predecessor by moving forward in the chord circle till it finds a node whose value is less than the key \
    predecessor is greater than the given key
    """
    print str(self_id) + " finding predecessor"
    """
    if self_predecessor_id == self_successor_id:
        send_back_predecessor(key_to_find, query_node_id)
        return
    """
    current_node = self_id
    current_successor = successors[1]

    print 'looking for ',key_to_find,' in range ',current_node, ' to ',current_successor
    if is_in_range_right_inclusive(key_to_find, current_node, current_successor):
        #Predecessor found!
        print str(self_id) + "predecessor found"
        send_back_predecessor(key_to_find, query_node_id)
    else:
        print str(self_id) + "finding closest predecessor"
        next_node_id = closest_preceding_finger(key_to_find)
        print str(self_id) + "closest preceding finger - " + str(next_node_id)
        if next_node_id == self_id: 
            send_back_predecessor(key_to_find, query_node_id)
        ask_next_node_for_predecessor(next_node_id, key_to_find, query_node_id)
        print str(self_id) + "asked next node"


def closest_preceding_finger(key_to_find):
    """
    Scans the finger table from bottom to top to determine the closest known predecessor of the given key
    :return:closest known predecessor of given key
    """
    for i in range(m,0,-1):
        finger_node = successors[i]
        if is_in_range_both_exclusive(finger_node, self_id, key_to_find):
            return finger_node
    return self_id#Don't understand how it could ever reach here? Won't the key be between one of the fingertable entries?


"""
Following functions used to join nodes
"""
def calculate_finger_start(k):
    """
    Calculate start of finger - (n + 2^(k-1)) mod 2^m
    """
    val = (self_id + pow(2,k-1) ) % pow(2,m)
    print str(self_id) +  " calculated finger start - " + str(val)
    return val

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
    print str(self_id) + "init_finger_table"
    finger_starts[1] = calculate_finger_start(1)
    #intervals[1] = (finger_starts[1], calculate_finger_start(2))
    ask_arbitrary_node_for_successor(arbitrary_node_id, finger_starts[1])
    successors[1] = listen_for_successor()

    print str(self_id) + " first finger table entry created"
    retrieve_predecessor(successors[1])
    global self_predecessor_id
    self_predecessor_id = listen_for_predecessor_query()
    set_predecessor(successors[1], self_id)
    
    for i in range(1,m):
        print str(self_id) + "working on entry - " + str(i) 
        finger_starts[i+1] = calculate_finger_start(i+1)
        #intervals[i+1] = (finger_starts[i+1], calculate_finger_start(i+2))
        if is_in_range_left_inclusive(finger_starts[i+1], self_id, successors[i]):
            print "true"
            successors[i+1] = successors[i]
        else:
            print "false"
            ask_arbitrary_node_for_successor(arbitrary_node_id, finger_starts[i+1])
            successors[i+1] = listen_for_successor()

    print 'init finger table done'
    show_finger_table()


def show_finger_table():
    """
    Print out finger table for debugging
    """
    for i in range(1,m+1):
        print 'start - ' + str(finger_starts[i]) + ', successor - ' + str(successors[i])


def send_back_update_complete(query_node_id):
    """
    Let original node that initiating query know the update is complete
    """
    encoded_string = json.dumps({'action':'update_complete'})
    send_message(query_node_id, encoded_string)


def wait_for_node_to_update():
    """
    Wait till out-of-date node is finished updating
    """
    conn, addr = s.accept()
    data = conn.recv(buffer_size)
    print str(self_id) + " waiting for update message..."
    try:
        message = json.loads(data)
        if message["action"] != "update_complete":
            print "Bad Update Message..."
            print data
        return 
    except Exception:
            print "Bad Update Message..."
   

def update_finger_table(new_node_id, i, query_node_id):
    """
    Some logic to update an individual finger table, need to figure how this works
    """
    print str(self_id) + " updating finger table"
    if is_in_range_left_inclusive(new_node_id, self_id, successors[i]):
        successors[i] = new_node_id
        if(self_predecessor_id != self_id and self_predecessor_id != query_node_id and self_predecessor_id != new_node_id):
            notify_node_to_update_finger_table(self_predecessor_id, new_node_id, i, self_id)
            wait_for_node_to_update()
    send_back_update_complete(query_node_id)
    print str(self_id) + " update finger table complete, succesors[{}] = {}".format(i,successors[i]) 


def update_others():
    """
    Update the finger tables of other nodes to reflect the new node joining
    """
    for i in range(1,m+1):
        print str(self_id) + ' i:'+ str(i)
        val = (self_id - pow(2,i-1) + 1) % pow(2,m)
        print str(self_id) + ' looking for predecessor of '+ str(val)
        predecessor_id = None
        if self_id == val:
            predecessor_id = self_predecessor_id
        else:
            find_predecessor(val, self_id)
            predecessor_id = listen_for_predecessor()
        print str(self_id) + ' predecessor found: '+ str(predecessor_id)
        if predecessor_id != self_id:
            notify_node_to_update_finger_table(predecessor_id, self_id, i, query_node_id=self_id)
            wait_for_node_to_update()
        print str(self_id) + " update others iteration complete"


def transfer_keys_to_predecessor(keys_to_remove):
    """
    Add these keys removed from current node to the predecessor node
    """
    encoded_string = json.dumps({'action':'force_key', 'data':keys_to_remove})
    send_message(self_predecessor_id, encoded_string)


def move_keys(begin_range, end_range):
    """
    Remove keys in range from keystore and send them to predecessor
    """
    keys_to_remove = []
    global keys 
    for key in keys:
        if is_in_range_right_inclusive(key,begin_range,end_range):
            keys_to_remove.append(key)

    keys = [key for key in keys if key not in keys_to_remove]
    transfer_keys_to_predecessor(keys_to_remove)

def main():
    """
    Initialize node variables, send ack to coordinator, start listening
    """
    global coordinator_port 
    global start_port #Port where identifier space starts

    #Lists to hold finger table information, should probably create object for entries and store in one list
    global keys
    global finger_starts
    global intervals
    global successors

    global self_predecessor_id
    global self_id
    global m #number of bits in ID

    
    signal.signal(signal.SIGTERM, term_handler)

    coordinator_port = int(sys.argv[1])
    start_port = coordinator_port + 1
    self_port = int(sys.argv[2])
    self_id = get_id(self_port)

    json_data = json.loads(sys.argv[3])
    arbitrary_node_port = None
    if 'existing_node' in json_data:
        arbitrary_node_port = json_data['existing_node']

    global s
    global buffer_size
    buffer_size = 4096
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('', get_port(self_id)))
    s.listen(5)

    m = 8 #TODO: Currently hardcoding identifier size
    keys = []
    finger_starts = {}
    intervals = {}
    successors = {}

    print str(self_id) + "about to join - " + str(self_port)
    if arbitrary_node_port is not None:
        arbitrary_node_id = get_id(arbitrary_node_port)
        init_finger_table(arbitrary_node_id)
        update_others()
        ask_next_node_to_move_keys()
        wait_for_key_transfer_to_complete() 
    else:
        for i in range(1,m+1):
            finger_starts[i] = calculate_finger_start(i)
            intervals[i] = (finger_starts[i], calculate_finger_start(i+1))
            successors[i] = self_id
        show_finger_table()
        self_predecessor_id = self_id

    send_ack()
    start_listening()

if __name__ == "__main__":
    main()
