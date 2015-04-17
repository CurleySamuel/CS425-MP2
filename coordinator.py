import socket
from termcolor import colored
import subprocess
import json
import time
import signal
import sys

coord_port = 44443
start_port = 44444
node_list = {}

def main():
    global node_list

    # Confirm that port range is currently available.
    print colored("Initializing Ports", "yellow")
    initialize_ports()
    print colored("\tCoordinator port: ", "cyan") + "\n\t\t{}".format(coord_port)
    print colored("\tNode ports: ", "cyan") + "\n\t\t{} - {}".format(start_port, start_port+254)

    # Bind to our coordinator port
    global s
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', coord_port))
    s.listen(10)

    # Create a new initial empty node.
    print colored("Creating Initial Node", "yellow")
    launch_node(0)
    print colored("\tNode is launched, waiting for response", "cyan")
    listen_for_complete(0)
    print colored("\tNode is responding", "cyan")

    # Fill initial node with all keys.
    print colored("Populating system with keys", "yellow")
    for key in range(0,256):
        force_key(0, key)
    print colored("\tKeys 0 - 255 entered", "cyan")

    # Enter input loop
    print colored("System Initialized! Entering loop.\n", "yellow")
    while 1:
        command = raw_input('\x1b[35mCommand:\x1b[0m\n\t')
        if command.lower() == "exit":
            break

    # Kill all procreations
    smother_children()

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
    sys.exit(0)


def force_key(node_key, key):
    pass


def listen_for_complete(key):
    conn, addr = s.accept()
    data = conn.recv(2048)
    try:
        msg = json.loads(data)
        if msg["action"].lower() != "ack":
            print colored("oh god unexpected message received\n\t{}", "red").format(data)
    except Exception:
            print colored("oh god unexpected message received\n\t{}", "red").format(data)




def launch_node(key, data={}):
    if key in node_list.keys():
        print colored("Node with key already exists!", "red")
        return
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
        if result != 61:
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
