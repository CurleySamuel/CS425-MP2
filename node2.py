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

def get_id(port):
    if port is not None:
        return port - start_port

def get_port(id):
    if id is not None:
        return start_port + id


def send_message(port, data):
    print "Sending {}".format(data)
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s2.connect(('', port))
    s2.send(data)
    s2.close()


def send_ack(data=None):
    encoded_string = json.dumps({'action':'ACK', 'data':data})
    send_message(coordinator_port, encoded_string)


def handle_message(datas):
    global keys
    global data
    message = json.loads(datas)
    if 'action' in message:
        action = message['action']
        if action == 'force_key':
            add_keys(message['data'])
            send_ack()
        elif action == 'find':
            find(message['key'])
        elif action == 'list':
            send_ack(keys)
        elif action == 'locate':
            key = message['key']
            result = find_successor(key)
            send_ack(result)
        elif action == 'find_successor':
            src = message['src']
            key = message['key']
            result = find_successor(key)
            reply_to_node(src, {'action': "ACK", 'data': result})
        elif action == 'your_successor':
            src = message['src']
            result = data['fing'][1]['node']
            reply_to_node(src, {'action': "ACK", 'data': result})
        elif action == 'your_predecessor':
            src = message['src']
            result = data['pred']
            reply_to_node(src, {'action': "ACK", 'data': result})
        elif action == 'set_your_predecessor':
            src = message['src']
            data['pred'] = message['key']
            reply_to_node(src, {'action': "ACK"})
        elif action == 'closest_preceding_finger':
            key = message['key']
            src = message['src']
            result = closest_preceding_finger(key)
            reply_to_node(src, {'action': "ACK", 'data': result})
        elif action == 'update_finger_table':
            key = message['key']
            src = message['src']
            i = message['i']
            update_finger_table(key,i)
            reply_to_node(src, {'action': "ACK"})
        elif action == "key_request":
            key = message['key']
            src = message['src']
            result = [x for x in keys if range_ei(self_id,x,key)]
            for x in result:
                keys.remove(x)
            reply_to_node(src, {'action': 'ACK', 'data': result})
        else:
            raise RuntimeError


def reply_to_node(node, data):
    send_message(get_port(node), json.dumps(data))


def find_successor(key):
    n = find_predecessor(key)
    return send_your_successor(n)


def find_predecessor(key):
    prime = self_id
    while (not range_ei(prime, key, send_your_successor(prime))) and prime != send_your_successor(prime):
        prime = send_closest_preceding_finger(prime, key)
    return prime


def closest_preceding_finger(key):
    for i in range(8,0,-1):
        if range_ee(self_id, data['fing'][i]['node'], key):
            return data['fing'][i]['node']
    return self_id


def send_your_successor(node):
    global data
    if self_id == node:
        return data['fing'][1]['node']
    datas = {
        'action': "your_successor",
        'src': self_id
    }
    send_message(get_port(node), json.dumps(datas))
    return listen_once()['data']

def send_your_predecessor(node):
    global data
    if self_id == node:
        return data['fing'][1]['node']
    datas = {
        'action': "your_predecessor",
        'src': self_id
    }
    send_message(get_port(node), json.dumps(datas))
    return listen_once()['data']


def send_find_successor(node, key):
    data = {
        'action': "find_successor",
        'key': key,
        'src': self_id
    }
    send_message(get_port(node), json.dumps(data))
    return listen_once()['data']


def send_update_finger_table(node, key, i):
    data = {
        'action': "update_finger_table",
        'key': key,
        'src': self_id,
        'i': i
    }
    send_message(get_port(node), json.dumps(data))


def send_key_request(node):
    data = {
        'action': "key_request",
        'key': self_id,
        'src': self_id
    }
    send_message(get_port(node), json.dumps(data))
    return listen_once()['data']


def range_ie(a,x,b):
    if a <= b:
        return a <= x < b
    return not range_ie(b,x,a)

def range_ei(a,x,b):
    if a <= b:
        return a < x <= b
    return not range_ei(b,x,a)

def range_ee(a,x,b):
    if a <= b:
        return a < x < b
    return not range_ii(b,x,a)

def range_ii(a,x,b):
    if a <= b:
        return a <= x <= b
    return not range_ee(b,x,a)


def send_closest_preceding_finger(node, key):
    if self_id == node:
        return closest_preceding_finger(key)
    data = {
        'action': "closest_preceding_finger",
        'key': key,
        'src': self_id
    }
    send_message(get_port(node), json.dumps(data))
    return listen_once()['data']


def set_your_predecessor(node, key):
    data = {
        'action': "set_your_predecessor",
        'src': self_id,
        'key': key
    }
    send_message(get_port(node), json.dumps(data))
    return listen_once()


def start_listening_loop():
    while 1:
        conn, addr = s.accept()
        data = conn.recv(4096)
        handle_message(data)


def listen_once():
    conn, addr = s.accept()
    data = json.loads(conn.recv(4096))
    print "Receiving {}".format(data)
    return data


def term_handler(signal, frame):
    s.close()
    exit(0)


def add_keys(key):
    keys.extend(key)


def init_finger_table(teacher):
    global data
    node = send_find_successor(teacher, (self_id+1)%(2**m))
    data['fing'].append({
        'node': node,
        'strt': (self_id+1)%(2**m),
        'end': (self_id+2)%(2**m)
    })
    data['pred'] = send_your_predecessor(node)
    set_your_predecessor(node, self_id)
    for i in range(1,m):
        strt = (self_id+2**(i-1))%(2**m)
        end = (self_id+2**i)%(2**m)
        strt1 = (self_id+2**(i))%(2**m)
        end1 = (self_id+2**(i+1))%(2**m)
        if range_ie(self_id, strt1, data['fing'][i]['node']):
            a = {
                'node': data['fing'][i]['node'],
                'strt': strt,
                'end': end
            }
        else:
            a = {
                'node': send_find_successor(teacher, strt1),
                'strt': strt,
                'end': end
            }
        data['fing'].append(a)


def update_others():
    for i in range(1,9):
        p = find_predecessor((self_id-(2**(i-1)))%(2**8))
        send_update_finger_table(p, self_id, i)


def update_finger_table(s,i):
    global data
    if range_ie(self_id, s, data['fing'][i]['node']) or self_id == data['fing'][i]['node']:
        data['fing'][i]['node'] = s
        send_update_finger_table(data['pred'], s, i)


def move_keys():
    global keys
    new_keys = send_key_request(data['fing'][1]['node'])
    keys.extend(new_keys)


def main():
    global coordinator_port
    global start_port
    global self_port
    global self_id
    global m
    global s
    global data
    global keys
    keys = []
    data = {
        'fing': [],
        'pred': None
    }
    data['fing'].append(None)

    signal.signal(signal.SIGTERM, term_handler)
    coordinator_port = int(sys.argv[1])
    start_port = coordinator_port + 1
    self_port = int(sys.argv[2])
    self_id = get_id(self_port)
    m = 8

    jsondata = json.loads(sys.argv[3])
    teacher_node = None
    if 'existing_node' in jsondata.keys():
        teacher_node = jsondata['existing_node']

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('', get_port(self_id)))
    s.listen(5)
    if teacher_node is not None:
        teacher_node_id = get_id(teacher_node)
        init_finger_table(teacher_node_id)
        update_others()
        move_keys()
    else:
        for i in range(1,m+1):
            strt = (self_id+2**(i-1))%(2**m)
            end = (self_id+2**i)%(2**m)
            out = { 'node': self_id, 'strt': strt, 'end': end }
            data['fing'].append(out)
        data['pred'] = self_id
    send_ack()
    start_listening_loop()

if __name__ == "__main__":
    main()
