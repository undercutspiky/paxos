import random
import cPickle as cp
import threading
import socket
import struct
import sys

class learner():
    def __init__(self, clientID, mcast_groups):
    
        self.clientID = clientID
        self.mcast_groups = mcast_groups
        while True:
            value = raw_input()
            t = threading.Thread(target = self.send, args=(value,))
            t.start()
            t.join()
    
    def send(self, value):
        
        mcast_grp = self.mcast_groups["proposers"]
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(2)
        # Set the time-to-live for messages to 1 
        ttl = struct.pack('b', 1)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
        try:
            # Send data to the proposers
            sent = sock.sendto(cp.dumps([value]), mcast_grp)
        finally:
            print 'closing socket'
            sock.close()


if __name__ == '__main__':
    clientID = random.randint(1,5000)
    mcast_groups = {}
    
    if len(sys.argv) == 3:
        clientID = int(sys.argv[1])
        config_file = sys.argv[2]
        f = open(config_file,"r")
        for line in f:
            role,ip,port = line.split()
            mcast_groups[role] = (ip,int(port))
    else:
        #print "DID NOT RECEIVE 2 COMMAND LINE ARGUMENTS----USING DEFAULT CONFIG FILE"
        f = open("configuration","r")
        for line in f:
            role,ip,port = line.split()
            mcast_groups[role] = (ip,int(port))
    lnr = learner(clientID,mcast_groups)
