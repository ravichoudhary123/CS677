from peer_mt import peer
import threading as td
from multiprocessing import Process
import socket

# For hosting all the peers on same machine.
host_ip = socket.gethostbyname(socket.gethostname())
# Configuration Paramters
host_addrs = [host_ip + ':9007',host_ip + ':9008',host_ip + ':9009',host_ip + ':9010',host_ip + ':9011',host_ip + ':9012']
neighbors = [[host_ip + ':9008',host_ip + ':9009'],[host_ip + ':9007',host_ip + ':9010',host_ip + ':9011'],[host_ip + ':9007',host_ip + ':9011'],[host_ip + ':9008',host_ip + ':9012'],[host_ip + ':9008',host_ip + ':9009',host_ip + ':9012'],[host_ip + ':9010',host_ip + ':9011']]
peer_ids = [1,2,3,4,5,6]
# Specifying the role, inventory and shopping list
db = [
    {'Role': 'Buyer','Inv':None,'shop':['Fish','Salt','Boar']},
    {'Role': 'Seller','Inv':{'Fish':0,'Boar':3,'Salt':0},'shop':None},
    {'Role': 'Buyer','Inv':None,'shop':['Boar','Fish','Salt']},
    {'Role': 'Seller','Inv':{'Fish':0,'Boar':0,'Salt':3},'shop':None},
    {'Role': 'Buyer','Inv':None,'shop':['Salt','Boar','Fish']},
    {'Role': 'Seller','Inv':{'Fish':3,'Boar':0,'Salt':0},'shop':None}
]

# Initiate the peers.
peers = []
for index in xrange(len(host_addrs)):
    p = peer(host_addrs[index],peer_ids[index],neighbors[index],db[index])
    peers.append(p)
    
# Start Server and Client for each peer(Multi Processing)    
for x in xrange(len(host_addrs)):
   process = Process(target=(peers[x].startServerandClient()),args=())
   process.start()