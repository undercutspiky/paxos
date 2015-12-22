import random
import cPickle as cp
import threading
import socket
import struct
import sys

class acceptor():
    def __init__(self, acceptorID, mcast_groups):
        
        self.promisedID = None
        self.acceptedID = None
        self.acceptedValue = {}
        self.acceptorID = acceptorID
        self.mcast_groups = mcast_groups
        
        self.sock_s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ttl = struct.pack('b', 1)
        self.sock_s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
        
        
        mcast_grp = self.mcast_groups["acceptors"]
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(mcast_grp)
        mreq = struct.pack("4sl", socket.inet_aton(mcast_grp[0]), socket.INADDR_ANY)

        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        
        while True:
            t = threading.Thread(target = self.listen, args=(sock,))
            t.start()
            t.join()
    
    def listen(self, sock):
        
        mcast_grp = self.mcast_groups["acceptors"]
        #print "WAITING FOR DATA FROM %s================================" % repr(mcast_grp)
        data, address = sock.recvfrom(10240)
        #print "WAIT OVER======================"
        arr = cp.loads(data)
        
        if len(arr) == 2:
            self.receivePrepare(address, int(arr[0]), arr[1])
        elif len(arr) == 3:
            self.receiveAccept(address, arr[0], arr[1], arr[2])
        else:
            print'Invalid data received from proposer'
    
    def sendPromise(self, proposalID, acceptedID, acceptedValue, fromID, sequenceNumber):
        
        arr = [self.acceptorID, proposalID, acceptedID, acceptedValue, sequenceNumber]
        message = cp.dumps(arr)
        self.sendMessage(message, fromID, self.mcast_groups["proposers"])
        
    
    def receivePrepare(self, fromID, proposalID, sequenceNumber):
        if self.promisedID is not None and proposalID == self.promisedID:
            if sequenceNumber in self.acceptedValue.keys():
                self.sendPromise(proposalID, self.acceptedID, self.acceptedValue[sequenceNumber], fromID, sequenceNumber)
            else:
                self.sendPromise(proposalID, self.acceptedID, None, fromID, sequenceNumber)
        
        elif self.promisedID is None or int(proposalID) > int(self.promisedID):
            self.promisedID = proposalID
            if sequenceNumber in self.acceptedValue.keys():
                self.sendPromise(proposalID, self.acceptedID, self.acceptedValue[sequenceNumber], fromID, sequenceNumber)
            else:
                self.sendPromise(proposalID, self.acceptedID, None, fromID, sequenceNumber)
        else:
            print "\n===========SENDING NACK TO PROPOSER FOR PREPARE================\n"
            self.sendMessage(cp.dumps([-1]), fromID)
    
    def receiveAccept(self, fromID, proposalID, value, sequenceNumber):
        
        if self.promisedID is None or proposalID >= self.promisedID:
            self.promisedID = proposalID
            self.acceptedID = proposalID
            self.acceptedValue[sequenceNumber] = value
            #print "ACCEPTED THE VALUE "+str(value)
            self.sendAccepted(self.acceptedID, self.acceptedValue[sequenceNumber], fromID, sequenceNumber)
        else:
            print "\n===========SENDING NACK TO PROPOSER FOR ACCEPTANCE================\n"
            self.sendMessage(cp.dumps([-1]), fromID, self.mcast_groups["proposers"])
    
    def sendAccepted(self, acceptedID, acceptedValue, fromID, sequenceNumber):
        
        arr = [self.acceptorID, acceptedID, acceptedValue, sequenceNumber]
        message = cp.dumps(arr)
        self.sendMessage(message, fromID, self.mcast_groups["proposers"])
    
    def sendMessage(self, message, fromID, mcast_grp):
        
        try:
		    # Send data to the multicast group
            #print 'sending "%s"' % message
            sent = self.sock_s.sendto(message, fromID)
		    # Look for responses from all proposers
        finally:
            print 'closing socket or maybe not'
            #self.sock_s.close()
            
            
if __name__ == '__main__':
    acceptorID = random.randint(1,5000)
    mcast_groups = {}
    
    if len(sys.argv) == 3:
        acceptorID = int(sys.argv[1])
        config_file = sys.argv[2]
        f = open(config_file,"r")
        for line in f:
            role,ip,port = line.split()
            mcast_groups[role] = (ip,int(port))
    else:
        print "DID NOT RECEIVE 2 COMMAND LINE ARGUMENTS----USING DEFAULT CONFIG FILE"
        f = open("configuration","r")
        for line in f:
            role,ip,port = line.split()
            mcast_groups[role] = (ip,int(port))
    acc = acceptor(acceptorID,mcast_groups)
