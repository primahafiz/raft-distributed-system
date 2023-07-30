from address import Address
from datetime import datetime
from enums import *
from httpwrapper import HttpWrapper
import time

API_PORT = 8090

class Client:

    RPC_TIMEOUT = 5
    MAX_CNT_TIMEOUT = 5

    def __init__(self, address:Address) -> None:
        self.address = address
        # self.server = SimpleXMLRPCServer((self.address.ip,self.address.port),allow_none=True)
        self.print_log('Client registered')

    async def requestLog(self, portRaft:int) -> dict:
        self.print_log(f'Requesting log to {portRaft}')
        res = await HttpWrapper.singleRequestGetWithMaxTimeout(f'http://localhost:{portRaft}/log')
        return res

    async def requestExecute(self, executionType:str, portRaft:int, value:str) -> dict:
        self.print_log(f'Requesting execution of {executionType} with value {value} to {portRaft}')
        timestamp = time.time()
        res = await HttpWrapper.singleRequestPostWithMaxTimeout(f'http://localhost:{portRaft}/execute',5,{'timestamp':timestamp,'ipClient':self.address.ip,'portClient':self.address.port,'executionType':executionType,'value':value})
        # while("leaderAddress" in res):
        #     self.print_log("redirect to Leader")
        #     res = await HttpWrapper.singleRequestPostWithMaxTimeout(f'{res["leaderAddress"]}/execute',5,{'timestamp':timestamp,'ipClient':self.address.ip,'portClient':self.address.port,'executionType':executionType,'value':value})
        return res

    async def requestRemoveNode(self, portRaft:int, toBeRemovedPort:int) -> dict:
        self.print_log(f'Requesting removal of {toBeRemovedPort} to {portRaft}')
        res = await HttpWrapper.singleRequestPostWithMaxTimeout(f'http://localhost:{portRaft}/membership/remove',5,{'portRaft':toBeRemovedPort})
        return res

    def startServing(self) -> None:
        # self.server.register_function(self.requestLog,'requestLog')
        # self.server.register_function(self.requestExecute,'requestExecute')
        # self.server.register_function(self.requestRemoveNode,'requestRemoveNode')
        # self.server.serve_forever()
        pass
        
    def print_log(self, message:str) -> None:
        GREEN = '\033[92m'
        ENDC = '\033[0m'
        print(f'{GREEN}[{datetime.now()}] [{self.address}]{ENDC} {message}')
    
