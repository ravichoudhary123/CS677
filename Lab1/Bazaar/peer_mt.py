from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib
import time
import threading as td
import socket,SocketServer
import random

# Multi-Threaded RPC Server.
class AsyncXMLRPCServer(SocketServer.ThreadingMixIn,SimpleXMLRPCServer): pass

# Peer
class peer:
    def __init__(self,host_addr,peer_id,neighbors,db):
        self.host_addr = host_addr
        self.peer_id = peer_id
        self.neighbors = neighbors
        self.db = db
        self.reply_list = []
        self.lookup_requests = []
        self.semaphore = td.BoundedSemaphore(1)


    # Helper Method : Prevents Duplication.
    def check_duplication(self, lookup_request):
       self.semaphore.acquire()
       sim_list = filter(lambda request:request['product_name'] == lookup_request['product_name'] and lookup_request['buyer_id']['peer_id'] == request['buyer_id']['peer_id'],self.lookup_requests)
       self.semaphore.release()
       if len(sim_list) == 0:
           return False
       else:
           time_diff = [time.time() - req['time_stamp'] for req in sim_list]
           time_diff_filter = filter(lambda diff : diff < 10.0,time_diff)
           if len(time_diff_filter) > 0:
               return True # Duplicate found
           else :
               return False
          
   # Helper Method: Returns the proxy for specified address.
    def get_rpc(self,neighbor):
        a = xmlrpclib.ServerProxy('http://' + str(neighbor) + '/')
        try:
            a.test()   # Call a fictive method.
        except xmlrpclib.Fault:
            # connected to the server and the method doesn't exist which is expected.
            pass
        except socket.error:
            # Not connected ; socket error mean that the service is unreachable.
            return False, None
            
        # Just in case the method is registered in the XmlRPC server
        return True, a
        
    # Starting Server    
    def startServer(self):
        # Start Server and register its functions.
        host_ip = socket.gethostbyname(socket.gethostname())
        server = AsyncXMLRPCServer((host_ip,int(self.host_addr.split(':')[1])),allow_none=True,logRequests=False)
        server.register_function(self.lookup,'lookup')
        server.register_function(self.reply,'reply')
        server.register_function(self.buy,'buy')
        server.serve_forever()
    
    # Helper Method: Send Lookup Request to the Neighbor.
    def lookup_mt(self,buyer_id,product_name,hop_count,hop_path,neighbor):
        # Lookup Neighbor.
        connected,proxy = self.get_rpc(neighbor)
        if connected:
            proxy.lookup(buyer_id,product_name,hop_count,hop_path)
    
    # Starting Client    
    def startClient(self):
        time.sleep(5) # Wait initially for all the peers to start.
        if self.db['shop'] is not None: # Buyer.
           for item in self.db['shop']: # Iterate for each item in list
                print "P",self.peer_id,": Initiating the Lookup Process for ", item
                for neighbor in self.neighbors:
                    # Call helper method in a seperate thread to send lookup request to neighbors.
                    thread = td.Thread(target=self.lookup_mt,args=({'peer_id':self.peer_id,'host_addr':self.host_addr},item,3,[{'peer_id':self.peer_id,'host_addr':self.host_addr}],neighbor))
                    thread.start()
                time.sleep(10) # Time-out.
                # Check the number of replies.
                if len(self.reply_list) != 0:
                    while True:
                        # Try buying from 1st seller
                        seller_id = self.reply_list[0]
                        connected,proxy = self.get_rpc(seller_id[0]['host_addr'])
                        self.reply_list.remove(self.reply_list[0])
                        if connected:
                            # Get the acknowledgement from seller, if negative, then try buying from the next seller, if exists.
                            res = proxy.buy({'peer_id':self.peer_id,'host_addr':self.host_addr},item)
                            if res:
                                self.reply_list = []
                                print "P", self.peer_id, ": Bought ",item, "from ", seller_id[0]['peer_id']
                                break
                            elif len(self.reply_list) == 0:
                                break
                time.sleep(10) #Sleep for 10 sec before starting query for next item.
    
    # Start both Server and Client  
    def startServerandClient(self):
        thread1 = td.Thread(target=self.startServer,args=()) # Start Server
        thread2 = td.Thread(target=self.startClient,args=()) # Start Client
        threads = [thread1,thread2]

        for thread in threads:
            thread.start()                
            
    # Lookup Process      
    def lookup(self,buyer_id,product_name,hop_count,hop_path):
        # Forming a request for logging
        lookup_request = {'buyer_id':buyer_id,'product_name':product_name,'time_stamp':time.time()}
        # Check for Duplication
        if self.check_duplication(lookup_request): 
            pass
        else:
        #Step1 : Check if the buyerID and peerID is same, the discard  or hopCount is -1, then discard
            if buyer_id['peer_id'] is self.peer_id:
                pass
            
            elif hop_count == -1:
                pass
            else:
                self.semaphore.acquire()
                self.lookup_requests.append(lookup_request) # logging the request
                self.semaphore.release()
        #Step2 : Check if the host is a seller or buyer, if its buyer or seller doesn't have that item just forward the message and decreases the hop-count. 
                if self.db['Role'] is "Buyer" or self.db['Inv'][product_name] == 0:
                    if  self.db['Role'] is "Buyer":
                        print "P",self.peer_id,":I'm Buyer, I will forward the request for",product_name ,"to my neighbors."
                    else: 
                        print "P",self.peer_id,": I don't have",product_name,"I will forward the request to my neighbors."
                    hop_path.append({'peer_id':self.peer_id,'host_addr':self.host_addr}) # Appending the peer info to hop_path
                    for neighbor in self.neighbors: # Forwading request to the neighbors
                        if neighbor != buyer_id['host_addr'] and neighbor != hop_path[len(hop_path)-1]['host_addr']:
                            thread = td.Thread(target=self.lookup_mt,args=(buyer_id,product_name,hop_count-1,hop_path,neighbor))
                            thread.start()
                
        #Step3: If the Seller has that item, then reply back to buyer with buyer and seller-id
                else:
                    print "P",self.peer_id,": I have the ", product_name
                    peer_last = hop_path[len(hop_path)-1]
                    hop_path.pop()
                    connected,proxy = self.get_rpc(peer_last['host_addr'])
                    if connected:
                        proxy.reply(buyer_id,{'peer_id':self.peer_id,'host_addr':self.host_addr},product_name,hop_path)
     
    # Reply Function                                
    def reply(self,buyer_id,seller_id,product_name,hop_path):
        # If the hop_path is not empty, traverse back.
        if len(hop_path) != 0:
                    peer_last = hop_path[len(hop_path)-1]
                    hop_path.pop()
                    connected,proxy = self.get_rpc(peer_last['host_addr'])
                    if connected:
                        proxy.reply(buyer_id,seller_id,product_name,hop_path)
        else:
            # Log the seller request at the buyer who initiated the request.
            self.reply_list.append([seller_id,product_name])
    
    # Buy Function.       
    def buy(self,buyer_id,product_name):
        if self.db['Inv'][product_name] != 0:
            self.db['Inv'][product_name] = self.db['Inv'][product_name]-1
            if self.db['Inv'][product_name] == 0: # Refill the stock with a random item.
                product_list = ['Fish','Salt','Boar']
                x = random.randint(0, 2)
                self.db['Inv'][product_list[x]] = 3
            return 1
        return 0