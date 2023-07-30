import sys
from enum import Enum
from client import Client
from raft import RaftNode
from address import Address

class Type(Enum):
    CLIENT_NODE = 'client'
    RAFT_NODE = 'raft'

if __name__ == '__main__':
    nodeType = sys.argv[1]
    ip = sys.argv[2]
    port = sys.argv[3]

    if nodeType == Type.CLIENT_NODE.value:
        client = Client(Address(port,ip))
        client.startServing()
    elif nodeType == Type.RAFT_NODE.value:
        contactIp = sys.argv[4]
        contactPort = sys.argv[5]
        raftNode = RaftNode(Address(port,ip),Address(contactPort,contactIp))
        raftNode.startServing()