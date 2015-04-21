import socket
from termcolor import colored
import subprocess
import json
import time
import signal
import sys
import random

coord_port = 44443
start_port = 44444
node_list = {}
outfile = sys.stdout
benchmark_mode = False
benchmark_P = 4
benchmark_F = 64
benchmark_node_list = []

def benchmark_command():
    for x in range(0,benchmark_P):
        a = random.choice(range(0,256))
        while a in benchmark_node_list:
            a = random.choice(range(0,256))
        benchmark_node_list.append(a)
        yield "join {}".format(a)
    print "Phase 1 Complete"
    time.sleep(5)
    for x in range(0,benchmark_F):
        a = random.choice(benchmark_node_list)
        b = random.choice(range(0,256))
        yield "find {} {}".format(a,b)


gen = benchmark_command()

def main():
    global outfile
    global node_list

    if len(sys.argv) > 1:
        outfile = open(sys.argv[1], 'w')

    # Confirm that port range is currently available.
    print colored("Initializing Ports", "yellow")
    initialize_ports()
    print colored("\tCoordinator port: ", "cyan") + "\n\t\t{}".format(coord_port)
    print colored("\tNode ports: ", "cyan") + "\n\t\t{} - {}".format(start_port, start_port+254)

    # Bind to our coordinator port
    global s
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', coord_port))
    s.listen(20)

    # Create a new initial empty node.
    print colored("Creating Initial Node", "yellow")
    launch_node(0)
    print colored("\tNode is launched, waiting for response", "cyan")
    listen_for_complete()
    print colored("\tNode is responding", "cyan")

    # Fill initial node with all keys.
    print colored("Populating system with keys", "yellow")
    feed_initial_keys()
    print colored("\tKeys 0 - 255 entered", "cyan")

    # Enter input loop
    print colored("System Initialized! Entering loop.\n", "yellow")
    while 1:
        if benchmark_mode:
            try:
                command = gen.next()
            except StopIteration:
                print "Benchmark Complete"
                sys.exit(0)
        else:
            command = raw_input('\x1b[35mCommand:\x1b[0m\n\t')
        command = command.lower()
        if command == "exit":
            break
        parsed = validate(command)
        if parsed is None:
            continue
        if parsed[0] == "join":
            data = {
                "action": "create",
                "existing_node": node_list[0][0]#node_list[random.choice(node_list.keys())][0]
            }
            launch_node(int(parsed[1]), data)
            listen_for_complete()
            print colored("Node created", "green")
        elif parsed[0] == "find":
            data = {
                "action": "locate",
                "key": int(parsed[2])
            }
            rsp = send_message(node_list[int(parsed[1])][0], data)
            print colored("Key located at node {}", "green").format(rsp["found"])
        elif parsed[0] == "leave":
            data = {
                "action": "leave"
            }
            rsp = send_message(node_list[int(parsed[1])][0], data)
            # You're sending an extra ack?
            listen_for_complete()
            del node_list[int(parsed[1])]
            print colored("Node successfully removed.", "green")
        elif parsed[0] == "show":
            data = {
                "action": "list"
            }
            if parsed[1] != "all":
                rsp = send_message(node_list[int(parsed[1])][0], data)
                rsp['data'].sort()
                print >> outfile, "{} ".format(parsed[1]) + ' '.join(map(str, rsp['data']))
            else:
                for key,val in node_list.iteritems():
                    rsp = send_message(val[0], data)
                    rsp['data'].sort()
                    print >> outfile, "{} ".format(key) + ' '.join(map(str,rsp['data']))

    # Kill all procreations
    smother_children()


def validate(command):
    try:
        keys = node_list.keys()
        parsed = command.split()
        if len(parsed) < 2:
            print colored("Not enough arguments to function.", "red")
        elif len(parsed) > 3:
            print colored("Too many arguments to function.", "red")
        elif parsed[0] not in ["join", "find", "leave", "show"]:
            print colored("Not a recognized command.", "red")
        elif parsed[0] == "show" and parsed[1] == "all":
            return parsed[:2]
        elif parsed[0] == "join" and int(parsed[1]) not in range(0,256):
            print colored("Key out of range.", "red")
        elif parsed[0] == "join" and int(parsed[1]) in keys:
            print colored("Node already exists.", "red")
        elif parsed[0] == "find" and int(parsed[1]) not in keys:
            print colored("Node doesn't exist.", "red")
        elif parsed[0] == "find" and int(parsed[2]) not in range(0,256):
            print colored("Key not in allowed range.", "red")
        elif parsed[0] in ["leave", "show"] and int(parsed[1]) not in keys:
            print colored("Node doesn't exist.", "red")
        else:
            # Success!
            if parsed[0] == "find":
                return parsed[:3]
            return parsed[:2]
        return None
    except ValueError:
        print colored("Key or node value not an integer.", "red")
        return None
    except IndexError:
        print colored("Invalid command.", "red")


def smother_children(signalnum=0, handler=0):
    print colored("\nKilling all nodes", "yellow")
    for key, val in node_list.iteritems():
        val[1].terminate()
    print colored("\tSIGTERMS sent. Giving nodes time to clean up", "cyan")
    time.sleep(1)
    for key, val in node_list.iteritems():
        if val[1].poll() is None:
            print colored("\tSending SIGKILL to {}", "red").format(val[1].pid)
            val[1].kill()
    print colored("\tAll processes killed. Exiting.", "cyan")
    if outfile != sys.stdout:
        outfile.close()
    sys.exit(0)


def feed_initial_keys():
    data = {
        "action": "force_key",
        "data": range(0,256)
    }
    send_message(node_list[0][0], data)


def send_message(port, data):
    data2 = json.dumps(data)
    print colored("(DEBUG) Sending Message to Node {}\n\t{}", "cyan").format(port-start_port,data2)
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s2.connect(('',port))
        s2.sendall(data2)
        s2.shutdown(socket.SHUT_RDWR)
        s2.close()
    except socket.error as e:
        print colored("oh god one of our nodes has died - {}").format(e)
        smother_children()
    return listen_for_complete()


def listen_for_complete():
    while 1:
        conn, addr = s.accept()
        data = conn.recv(2048)
        print colored("(DEBUG) Received {}", "cyan").format(data)
        try:
            msg = json.loads(data)
            if msg["action"].lower() == "ack":
                return msg
            elif msg["action"].lower() == "debug":
                print colored("(DEBUG) {}", "cyan").format(msg["data"])
            else:
                print colored("oh god unexpected message received (else)\n\t{}", "red").format(data)
        except Exception:
            print colored("oh god unexpected message received\n\t{}", "red").format(data)
            return None



def launch_node(key, data={"action": "create"}):
    node_port = key + start_port
    json_data = json.dumps(data)
    node_list[key] = (node_port, subprocess.Popen(["python", "node.py", str(coord_port), str(node_port), json_data]))


def initialize_ports():
    global start_port
    global coord_port
    print colored("\tChecking port range {} - {}", "cyan").format(coord_port, coord_port+255)
    if not check_port_range(coord_port, 256):
        coord_port = max((coord_port + 256)%65535, 1024)
        start_port = coord_port + 1
        initialize_ports()
        return


def check_port_range(start, num_ports):
    for port in range(start, start + num_ports):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = s.connect_ex(('', port))
        s.close()
        if result != 61 and result != 111:
            print "\t\tPort {}: \t".format(port) + colored("Closed", "red")
            break
    else:
        # Loop completed successfully (without break)
        print "\t\tPorts {} - {}: \t".format(start, start+num_ports-1) + colored("Open", "green")
        return True
    return False

if __name__ == "__main__":
    signal.signal(signal.SIGINT, smother_children)
    signal.signal(signal.SIGTERM, smother_children)
    main()
