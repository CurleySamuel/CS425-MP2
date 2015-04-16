import socket
from termcolor import colored

coord_port = 44443
start_port = 44444
host = socket.gethostbyname(socket.gethostname())
node_list = {}

def main():
    global node_list

    # Confirm that port range is currently available.
    initialize_ports()

    # Create a new initial empty node.

    # Fill initial node with all keys.

    # Enter input loop


def initialize_ports():
    global start_port
    global coord_port
    print colored("Initializing Ports", "yellow")
    print colored("\tChecking port range {} - {}", "cyan").format(coord_port, coord_port+255)
    if not check_port_range(coord_port, 256):
        coord_port = max((coord_port + 256)%65535, 1024)
        start_port = coord_port + 1
        initialize_ports()
        return
    print colored("\tCoordinator port: ", "cyan") + "\n\t\t{}".format(coord_port)
    print colored("\tNode ports: ", "cyan") + "\n\t\t{} - {}".format(start_port, start_port+254)



def check_port_range(start, num_ports):
    for port in range(start, start + num_ports):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = s.connect_ex((host, port))
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
    main()
