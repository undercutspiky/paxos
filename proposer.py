import random
import cPickle as cp
import threading
import socket
import struct
import sys
import time

class proposer():
    def __init__(self, proposerID, mcast_groups, quorumSize=2):
    
        self.proposerID = proposerID
        self.randInt = random.randint(1,5000)
        self.proposalID = str(self.randInt) + str(self.proposerID)
        self.receivedPromises = set()
        self.proposedValue = int(str(random.randint(1,5000))+str(self.proposerID))
        self.lastAcceptedID = None
        self.quorumSize = 2
        self.sequenceNumber = 0
        self.active = None
        self.mcast_groups = mcast_groups
    
    def listenToClient(self):
        
        mcast_grp = mcast_groups["proposers"]
        sock_l = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        #sock_l.settimeout(2)
        sock_l.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_l.bind(mcast_grp)
        mreq = struct.pack("4sl", socket.inet_aton(mcast_grp[0]), socket.INADDR_ANY)
        sock_l.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        try:
            while True:
                print "Waiting to receive some value from client"
                try:
                    data, address = sock_l.recvfrom(10240)
                    data = cp.loads(data)[0]
                except socket.timeout:
                    print "Timed out for client"
                    break
                else:
                    print "RECEIVED %s FROM CLIENT %s" % (data,address)
                    self.prepare(data)
                    
        finally:
            print "selecting %s as leader" % (receivedID)
            sock.close()
            sock_l.close()
            if receivedID == str(self.proposerID):
                self.active = 1
        
    
    def listenToAcceptor(self, sock):
        
        data = None
        address = None
        try:
            data, address = sock.recvfrom(1024)
        except socket.timeout:
            print "TIMED OUT"
        if data is None:
            print "Either timed out or data not received"
            return
        arr = cp.loads(data)
        if len(arr) == 5:
            self.receivePromise(arr[0],arr[1],arr[2],arr[3], arr[4])
        elif len(arr) == 3:
            self.receiveAccepted(arr[0],arr[1], arr[2])
        elif len(arr) == 1:
            if arr[0] == -1:
                print "Nack received"
            else:
                print "MAYBE SOMEONE'S TRYING BE LEADER REALLY LATE !\n LET'S IGNORE HIM"
        else:
            print 'No data received'
    
    def electLeader(self):
        mcast_grp = self.mcast_groups["proposers"]
        print mcast_grp
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        
        # Set the time-to-live for messages to 1 
        ttl = struct.pack('b', 1)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
        
        
        
        sock_l = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock_l.settimeout(1)
        sock_l.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_l.bind(mcast_grp)
        mreq = struct.pack("4sl", socket.inet_aton(mcast_grp[0]), socket.INADDR_ANY)

        sock_l.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        
        
        try:
            sent = sock.sendto(cp.dumps([str(self.proposerID)],-1), mcast_grp)
            # Look for responses from all recipients
            receivedID = str(self.proposerID)
            while True:
                print "Waiting to receive some leadership messages"
                try:
                    data, address = sock_l.recvfrom(10240)
                    data = cp.loads(data)[0]
                except socket.timeout:
                    print "Timed out for leader election"
                    break
                else:
                    print "received %s from %s" % (data,address)
                    if data > receivedID:
                        print "%s IS GREATER THAN %s" % (data, receivedID)
                        receivedID = data
                        sock.sendto(cp.dumps([str(receivedID)]), mcast_grp)
                    elif data < receivedID:
                        sock.sendto(cp.dumps([str(receivedID)]),mcast_grp)
                    
        finally:
            print "selecting %s as leader" % (receivedID)
            sock.close()
            sock_l.close()
            if receivedID == str(self.proposerID):
                self.active = 1
    
    def sendPrepare(self, message):
        
        arr = [message, self.sequenceNumber]
        message = cp.dumps(arr)
        self.sendMessage(message, self.mcast_groups["acceptors"])

    def prepare(self, proposedValue = None):
    
        self.randInt += 1
        self.sequenceNumber += 1
        if proposedValue is not None:
            self.proposedValue = proposedValue
        else:
            self.proposedValue = int(str(random.randint(1,5000))+str(self.proposerID))
        self.proposalID = str(self.randInt) + str(self.proposerID)
        self.receivedPromises.clear()
        self.sendPrepare(str(self.proposalID))
    

    def receivePromise(self, fromID, proposalID, prevAcceptedID, prevAcceptedValue, sequenceNumber):
    
        if (str(proposalID) != self.proposalID) or (fromID in self.receivedPromises):
            print "\n\nOOPS\n\n"
            return

        self.receivedPromises.add(fromID)
        if (self.lastAcceptedID is None) or (prevAcceptedID > self.lastAcceptedID):
            self.lastAcceptedID = prevAcceptedID

            if prevAcceptedValue is not None and sequenceNumber == self.sequenceNumber:
                self.proposedValue = prevAcceptedValue

        if len(self.receivedPromises) == self.quorumSize:
            if self.proposedValue is None:
                self.proposedValue = int(str(random.randint(1,5000))+str(self.proposerID))
                
            self.sendAccept(self.proposalID, str(self.proposedValue))
    
    def receiveAccepted(self, acceptedID, acceptedValue, sequenceNumber):
        print "\nRECEIVED ACCEPTANCE MSG :\n"
        print acceptedID, acceptedValue, sequenceNumber
        print "\n"
        
        #Pass the value to learners
        message = cp.dumps([sequenceNumber, acceptedValue])
        self.sendMessage(message, self.mcast_groups["learners"], False)
    
    def sendAccept(self, proposalID, proposedValue):
        
        arr = [proposalID, proposedValue, self.sequenceNumber]
        print "============PROPOSAL VALUE IS %s================"%(proposedValue)
        message = cp.dumps(arr)
        
        if self.sequenceNumber == 1:
            self.sendMessage(message, self.mcast_groups["acceptors"], sleep=True)
        else:
            self.sendMessage(message, self.mcast_groups["acceptors"], sleep=False)
    
    def sendMessage(self, message, mcast_grp, listen=True, sleep=False):
        
        if(sleep):
            time.sleep(0.2)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        #sock.settimeout(0.15)
        # Set the time-to-live for messages to 1 
        ttl = struct.pack('b', 1)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
        try:
            # Send data to the multicast group
            #print 'SENDING SOME MESSAGE "%s"' % message
            print "SENDING TO %s" % repr(mcast_grp)
            sent = sock.sendto(message, mcast_grp)
            
            if listen:
                # Look for responses from all proposers
                t = []
                for i in xrange(self.quorumSize):
                    t.append(threading.Thread(target = self.listenToAcceptor, args=(sock,)))
                
                for i in xrange(self.quorumSize):
                    t[i].start()
                
                for i in xrange(self.quorumSize):
                    t[i].join()
        finally:
            print 'closing socket'
            sock.close()

if __name__ == '__main__':
    proposerID = random.randint(1,5000)
    mcast_groups = {}
    
    if len(sys.argv) == 3:
        proposerID = int(sys.argv[1])
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
    ppr = proposer(proposerID,mcast_groups)
    ppr.electLeader()
    if ppr.active == 1:
        for x in xrange(5):
            print "STARTING PHASE 1"
            t = threading.Thread(target = ppr.listenToClient)
            t.start()
            t.join()
        
