import socket
import sys

print "Initializing"
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('', 60001))
s.listen(20)
print "Bound. Listening"

total = 0
while 1:
    print "\rTotal: {}".format(total),
    sys.stdout.flush()
    conn, addr = s.accept()
    data = conn.recv(4096)
    total = total+1
