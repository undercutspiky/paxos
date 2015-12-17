import random
import cPickle as cp
import threading
import socket
import struct
import sys

class learner():
    def __init__(self, learnerID, mcast_groups):
    
        self.learnerID = learnerID
        self.learnedValues = {}
        self.mcast_groups = mcast_groups
        while True:
            t = threading.Thread(target = self.listen)
            t.start()
            t.join()
    
    def listen(self):
        
        mcast_grp = mcast_groups["learners"]

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(mcast_grp)
        mreq = struct.pack("4sl", socket.inet_aton(mcast_grp[0]), socket.INADDR_ANY)

        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        
        data, address = sock.recvfrom(1024)
        arr = cp.loads(data)
        
        if len(arr) == 2:
            self.receiveValue(arr[0], arr[1])
        else:
            print 'Invalid data received from proposer'
    
    def receiveValue(self, sequenceNumber, acceptedValue):
        if sequenceNumber not in self.learnedValues.keys():
            self.learnedValues[sequenceNumber] = acceptedValue
        #print "LEARNED %s FOR %s" % (acceptedValue,sequenceNumber)
        print acceptedValue
        sys.stdout.flush()

if __name__ == '__main__':
    learnerID = random.randint(1,5000)
    mcast_groups = {}
    
    if len(sys.argv) == 3:
        learnerID = int(sys.argv[1])
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
    lnr = learner(learnerID,mcast_groups)
