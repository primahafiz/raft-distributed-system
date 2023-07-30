from address import Address
from datetime import datetime
from enums import *
import asyncio
from threading import Thread, Lock
from collections import deque
from httpwrapper import HttpWrapper
import time
from typing import List
import random

API_PORT = 8090

class RaftNode:

    HEARTBEAT_SEND_TIMEOUT = 1
    HEARTBEAT_TIMEOUT = 5
    HEARTBEAT_RECEIVE_TIMEOUT = 20
    HEARTBEAT_RECEIVE_INTERVAL_TIMEOUT = 0.01
    APPLY_MEMBERSHIP_TIMEOUT = 20
    MAX_RANDOM_ELECTION_TIMEOUT = 20
    MAX_CNT_TIMEOUT = 3

    class RaftNodeType(Enum):
        LEADER = 'leader'
        FOLLOWER = 'follower'
        CANDIDATE = 'candidate'

    def __init__(self, address:Address, contactAddress:Address) -> None:
        self.address = address
        self.contactAddress = contactAddress
        self.listPortFollower = []
        self.currentTerm = 0
        self.q = deque()
        self.lastLogIndex = -1
        self.lastCommitIndex = -1
        self.lastLeaderSeen = 0
        self.randomizedElectionTimeout = random.random() * RaftNode.MAX_RANDOM_ELECTION_TIMEOUT
        self.currentVote = None
        self.termAndLastLogIndexLock = Lock()
        self.threadHeartbeat = None
        # each element contains a dictionary with keys: term(int), idx(int) executionType(string), value(string), leaderId(int)
        self.log = []
        self.print_log('Raft Node registered')
        if(address == contactAddress):
            self.raftNodeType = RaftNode.RaftNodeType.LEADER
            self.initAsLeader()
        else:
            self.raftNodeType = RaftNode.RaftNodeType.FOLLOWER
            self.initAsFollower()

    # MEMBERSHIP

    # Apply membership to leader node and start heartbeat
    async def applyMembership(self, address:Address) -> None:
        self.print_log(f'Applying membership of {address}')
        res = await HttpWrapper.singleRequestPostWithMaxTimeout(f'http://localhost:{address.port}/membership/add',RaftNode.MAX_CNT_TIMEOUT, {'ipRaft':self.address.ip,'portRaft':self.address.port},RaftNode.APPLY_MEMBERSHIP_TIMEOUT)
        
        if('err' in res):
            self.print_log(f'Failed to apply membership of {address} because of {res["err"]}')
        else:
            self.print_log(f'Applying membership of {address} succesful')

            self.lastLeaderSeen = time.time()
            self.listPortFollower = res['listPortFollower']
            if (self.threadHeartbeat == None):
                self.threadHeartbeat = Thread(target=asyncio.run,args=[self.checkHeartbeat()])
                self.threadHeartbeat.start()

    # Remove membership from leader node
    async def removeMembership(self, address:Address) -> None:
        self.print_log(f'Removing membership of {address}')
        res = await HttpWrapper.singleRequestPostWithMaxTimeout(f'http://localhost:{address.port}/membership/remove',RaftNode.MAX_CNT_TIMEOUT, {'ipRaft':self.address.ip,'portRaft':self.address.port})
        
        if('err' in res):
            self.print_log(f'Failed to remove membership of {address} because of {res["err"]}')
        else:
            self.print_log(f'Removed membership of {address} succesful')
    
    # Notify membership to other followers in cluster
    async def notifyMembershipChange(self, port:int, mode:bool) -> None:
        if self.raftNodeType != RaftNode.RaftNodeType.LEADER:
            return
        self.print_log(f'Notifying membership of {port} to all follower')

        urlList = [f'http://localhost:{followerPort}/membership/notify' for followerPort in self.listPortFollower if followerPort != port]
        paramsList = [{'ipRaft':'localhost','portRaft':port,'mode':mode} for followerPort in self.listPortFollower if followerPort != port]
        
        resList = await HttpWrapper.multipleRequestPostWithMaxTimeout(urlList, paramsList, RaftNode.MAX_CNT_TIMEOUT)
        
        if 'err' in resList:
            self.print_log(f'Failed to notify membership of {port} to all follower because of {resList["err"]}')
        else:
            self.print_log(f'Notified membership of {port} to all follower succesful')
    
    # Leader handling apply membership request from follower
    async def receiveApplyMembership(self, port:int) -> bool:
        if(self.raftNodeType != RaftNode.RaftNodeType.LEADER):
            return False
        
        self.print_log(f'Receiving membership of {port}')

        if(port not in self.listPortFollower):
            self.listPortFollower.append(port)
            self.print_log(f'Added {port} to list of followers')
        else:
            self.print_log(f'{port} already in list of followers')

        Thread(target=asyncio.run,args=[self.notifyMembershipChange(port,True)]).start()

        return {'status':True,'listPortFollower':self.listPortFollower}

    # Leader handling remove membership request from follower
    async def receiveRemoveMembership(self, port:int) -> bool:
        if(self.raftNodeType != RaftNode.RaftNodeType.LEADER):
            return False
        
        self.print_log(f'Receiving remove membership of {port}')

        if(port in self.listPortFollower):
            self.listPortFollower.remove(port)
            self.print_log(f'Removed {port} from list of followers')
        else:
            self.print_log(f'{port} not in list of followers')

        Thread(target=asyncio.run,args=[self.notifyMembershipChange(port,False)]).start()
        return True
    
    def updateMembership(self, port:int, mode:bool) -> None:
        self.print_log(f'Receiving update membership of {port}')
        if(mode):   # add
            if port not in self.listPortFollower:
                self.listPortFollower.append(port)
                self.print_log(f'Added {port} to list of followers')
            else:
                self.print_log(f'{port} already in list of followers')
        else:       # remove
            if(port in self.listPortFollower):
                self.listPortFollower.remove(port)
                self.print_log(f'Removed {port} from list of followers')
            else:
                self.print_log(f'{port} not in list of followers')

    # Leader init heartbeat send
    def initAsLeader(self):
        Thread(target=asyncio.run,args=[self.heartbeat()]).start()
        self.listPortFollower = [self.address.port]
        if (self.threadHeartbeat == None):
                self.threadHeartbeat = Thread(target=asyncio.run,args=[self.checkHeartbeat()])
                self.threadHeartbeat.start()
    
    # When leader get elected start heartbeat send
    def changeToLeader(self):
        Thread(target=asyncio.run,args=[self.heartbeat()]).start()

    # When node initialized as follower, start apply membership
    def initAsFollower(self):
        asyncio.run(self.applyMembership(self.contactAddress))


    # HEARTBEAT AND APPEND ENTRIES

    # Execute request client to all follower -> only for leader
    async def execute(self,timestamp, ipClient, portClient, executionType:str, value:str) -> dict:
        if(self.raftNodeType != RaftNode.RaftNodeType.LEADER):
            self.print_log('Not a leader, cannot receive request')
            return {'result':'Not a leader',
                    'leaderAddress': f'http://{self.contactAddress}'}
        
        if(not self.isEntryInLog(timestamp)):
            self.print_log(f'Executing {executionType} with value {value}')
            listFollowerRet = await self.appendEntriesAll(timestamp,executionType,value)

        if(self.lastCommitIndex == len(self.log)-1):
            return {'result': executionType + " committed"}
        else:
            return {'result': 'timeout'}
    
    # Check if entry is already in log based on timestamp to ensure exactly once execution
    def isEntryInLog(self, timestamp:float) -> bool:
        for entry in self.log:
            if(entry['timestamp'] == timestamp):
                return True
        return False

    # Send heartbeat to all follower (only for leader)
    async def heartbeat(self):
        while(self.raftNodeType == RaftNode.RaftNodeType.LEADER):
            ret = await self.appendEntriesAll(None,None,None)
            
            # send to dashboard api
            try:
                url = f'http://localhost:{API_PORT}/raft'
                paramsList = {
                    'portLeader': self.address.port,
                    'listPortFollower' : self.listPortFollower
                }
                Thread(target=asyncio.run,args=[HttpWrapper.singleRequestPost(url, paramsList)]).start()
            except Exception as e:
                res = ""
            await asyncio.sleep(RaftNode.HEARTBEAT_TIMEOUT)

    # Check heartbeat from leader (if timeout, change to candidate and start election)
    async def checkHeartbeat(self):
        while(True):
            self.termAndLastLogIndexLock.acquire()
            if(self.raftNodeType == RaftNode.RaftNodeType.FOLLOWER and time.time() - self.lastLeaderSeen > RaftNode.HEARTBEAT_RECEIVE_TIMEOUT + self.randomizedElectionTimeout):
                self.print_log('Leader timeout')
                self.raftNodeType = RaftNode.RaftNodeType.CANDIDATE
                self.termAndLastLogIndexLock.release()
                await self.election()
            elif(self.raftNodeType == RaftNode.RaftNodeType.CANDIDATE):
                self.termAndLastLogIndexLock.release()
                await asyncio.sleep(self.HEARTBEAT_RECEIVE_TIMEOUT + self.randomizedElectionTimeout)
                self.randomizedElectionTimeout = random.random() * RaftNode.MAX_RANDOM_ELECTION_TIMEOUT
                # check if there is a leader
                self.termAndLastLogIndexLock.acquire()
                if(self.raftNodeType == RaftNode.RaftNodeType.FOLLOWER):
                    self.termAndLastLogIndexLock.release()
                    continue
                self.termAndLastLogIndexLock.release()

                await self.election()
            else:
                self.termAndLastLogIndexLock.release()
            await asyncio.sleep(RaftNode.HEARTBEAT_RECEIVE_INTERVAL_TIMEOUT)
    
    # Append entries -> if entry == [] is heartbeat and if entry is not [] then it is log replication
    async def appendEntriesAll(self,timestamp:float,executionType:str, value:str) -> None:
        if executionType == None:
            self.print_log(f'Heartbeat to all follower {self.listPortFollower}')
        else:
            self.print_log(f'Append entries to all follower')
        
        urlList = [f'http://localhost:{port}/entries/append' for port in self.listPortFollower if port != self.address.port]
        entry = {
            'timestamp':timestamp,
            'term':self.currentTerm,
            'idx':self.lastLogIndex+1,
            'leaderId':self.address.port,
            'executionType': executionType,
            'value': value,
        }
        paramsList = [{
            'term':self.currentTerm,
            'leaderId':self.address.port,
            'prevLogIndex':self.lastLogIndex,'prevLogTerm':-1 if self.lastLogIndex == -1 else self.log[len(self.log)-1]['term'], 
            'prevExecutionType': 'None' if self.lastLogIndex == -1 else self.log[len(self.log)-1]['executionType'],
            'prevValue': 'None' if self.lastLogIndex == -1 else self.log[len(self.log)-1]['value'],
            'entries':[] if executionType == None else [entry],
            'leaderCommit':self.lastCommitIndex} for port in self.listPortFollower if port != self.address.port]

        resList = await HttpWrapper.multipleRequestPostWithMaxTimeout(urlList,paramsList,1)
        self.print_log(resList)

        if executionType == None:
            # Update leader commit index
            self.updateCommitIndex(executionType,value,resList)

            # Update follower log if log inconsistent
            return await self.updateLogEntries(resList)

        self.lastLogIndex += 1
        self.log.append({
            'timestamp':timestamp,
            'term':self.currentTerm,
            'idx': self.lastLogIndex,
            'executionType': executionType,
            'value': value,
            'leaderId': self.address.port
        })

        # Update leader commit index
        self.updateCommitIndex(executionType,value,resList)
        
        # Update follower log if log inconsistent
        return await self.updateLogEntries(resList)
            
    # Update node commit index based on leader commit from append entries by leader
    def updateCommitIndex(self,executionType:str, value:str, resList:List[dict]) -> None:
        tmp = []
        for i in range(len(resList['result'])):
            if('err' not in resList['result'][i]):
                tmp.append(resList['result'][i]['lastLogIndex'])
        
        tmp.sort()

        toCommit = self.lastCommitIndex
        if((len(self.listPortFollower))//2 + 1 <= len(tmp)):
            toCommit = tmp[(len(self.listPortFollower))//2]

        for i in range(self.lastCommitIndex+1,toCommit+1):
            self.doExecution(self.log[i]['executionType'],self.log[i]['value'])
        
        self.lastCommitIndex = toCommit

    # Update follower log if log inconsistent -> only for leader
    async def updateLogEntries(self,resList: List[dict]) -> None:
        paramsList = []
        urlList = []
        self.print_log(f'Update log entries {resList}')
        for i in range(len(resList['result'])):
            if('err' not in resList['result'][i]):
                if(not resList['result'][i]['success']):
                    portToSend = resList['result'][i]['port']
                    urlList.append(f'http://localhost:{portToSend}/entries/append')
                    paramsList.append({
                        'term':self.currentTerm,
                        'leaderId':self.address.port,
                        'prevLogIndex':resList['result'][i]['lastLogIndex'],
                        'prevLogTerm':-1 if resList['result'][i]['lastLogIndex'] == -1 else self.log[resList['result'][i]['lastLogIndex']]['term'],
                        'prevExecutionType': "None" if resList['result'][i]['lastLogIndex'] == -1 else self.log[resList['result'][i]['lastLogIndex']]['executionType'],
                        'prevValue': "None" if resList['result'][i]['lastLogIndex'] == -1 else self.log[resList['result'][i]['lastLogIndex']]['value'],
                        'entries':[],
                        'leaderCommit':self.lastCommitIndex
                    })

                    for j in range(resList['result'][i]['lastLogIndex']+1,len(self.log)):
                        paramsList[len(paramsList)-1]['entries'].append({
                            'timestamp':self.log[j]['timestamp'],
                            'term':self.log[j]['term'],
                            'idx':self.log[j]['idx'],
                            'leaderId':self.address.port,
                            'executionType': self.log[j]['executionType'],
                            'value': self.log[j]['value'],
                        })
        self.print_log(f'paramsList: {paramsList}')                        
        ret = await HttpWrapper.multipleRequestPostWithMaxTimeout(urlList,paramsList,1,RaftNode.HEARTBEAT_SEND_TIMEOUT)
        return ret

    # Receive append entries from leader -> if entry == [] is heartbeat and if entry is not [] then it is log replication
    async def receiveAppendEntries(self,term:int,leaderId:int,prevLogIndex:int,prevLogTerm:int,prevExecutionType:str,prevValue:int,entries:list,leaderCommit:int) -> dict:
        self.lastLeaderSeen = time.time()
        if entries == []:
            self.print_log(f'Receiving heartbeat from {leaderId}')

            self.termAndLastLogIndexLock.acquire()

            if term > self.currentTerm:
                self.print_log(f'Surrender to {leaderId}')
                self.raftNodeType = RaftNode.RaftNodeType.FOLLOWER
                self.currentTerm = term
            elif term == self.currentTerm and leaderId < self.address.port and self.raftNodeType == RaftNode.RaftNodeType.LEADER:
                self.print_log(f'Surrender to {leaderId}')
                self.raftNodeType = RaftNode.RaftNodeType.FOLLOWER

            if(term == self.currentTerm):
                self.contactAddress = Address(str(leaderId), "localhost")
                self.randomizedElectionTimeout = random.random() * RaftNode.MAX_RANDOM_ELECTION_TIMEOUT
            
            self.termAndLastLogIndexLock.release()

            # Handle leadercommit changes
            toCommit = min(leaderCommit,self.lastLogIndex)
            for i in range(self.lastCommitIndex+1,toCommit+1):
                self.doExecution(self.log[i]['executionType'],self.log[i]['value'])
            
            self.lastCommitIndex = toCommit

            self.updateListLog(term,leaderId,prevLogIndex,prevLogTerm,prevExecutionType,prevValue,entries,leaderCommit)

            return {'term':self.currentTerm,'lastLogIndex':self.lastLogIndex, 'success':prevLogIndex == self.lastLogIndex, 'port':self.address.port}
        
        self.print_log(f'Receiving append entries from {leaderId}')
        
        self.termAndLastLogIndexLock.acquire()
        if(term < self.currentTerm):
            # Impossible ?
            self.termAndLastLogIndexLock.release()
            self.updateListLog(term,leaderId,prevLogIndex,prevLogTerm,prevExecutionType,prevValue,entries,leaderCommit)
            return {'term':self.currentTerm,'lastLogIndex':self.lastLogIndex, 'success':False, 'port':self.address.port}
        else:
            if term > self.currentTerm:
                self.print_log(f'Surrender to {leaderId}')
                self.raftNodeType = RaftNode.RaftNodeType.FOLLOWER
                self.currentTerm = term
            elif term == self.currentTerm and leaderId < self.address.port and self.raftNodeType == RaftNode.RaftNodeType.LEADER:
                self.print_log(f'Surrender to {leaderId}')
                self.raftNodeType = RaftNode.RaftNodeType.FOLLOWER
            
            if(term == self.currentTerm):
                self.contactAddress = Address(str(leaderId), "localhost")
                self.randomizedElectionTimeout = random.random() * RaftNode.MAX_RANDOM_ELECTION_TIMEOUT

            self.termAndLastLogIndexLock.release()
            
            return self.updateListLog(term,leaderId,prevLogIndex,prevLogTerm,prevExecutionType,prevValue,entries,leaderCommit)

    # Update follower log if log sent by leader is valid
    def updateListLog(self,term:int,leaderId:int,prevLogIndex:int,prevLogTerm:int,prevExecutionType:str,prevValue:int,entries:list,leaderCommit:int):
        idx = -1

        for i in range(len(self.log)):
            if(self.log[i]['idx'] == prevLogIndex and self.log[i]['term'] == prevLogTerm and self.log[i]['executionType'] == prevExecutionType and self.log[i]['value'] == prevValue):
                idx = i
                break

        if(idx != -1 or (idx == -1 and prevLogIndex == -1 and prevLogTerm == -1 and prevExecutionType == 'None' and prevValue == 'None')):
            self.print_log(f'Append log entries from {leaderId}')
            # Pop non valid log
            k = len(self.log)
            for _ in range(k-idx-1):
                self.log.pop()
            
            for i in range(len(entries)):
                if(self.isEntryInLog(entries[i]['timestamp'])):
                    continue
                self.log.append({
                    'timestamp':entries[i]['timestamp'],
                    'term':entries[i]['term'],
                    'idx': entries[i]['idx'],
                    'executionType': entries[i]['executionType'],
                    'value': entries[i]['value'],
                    'leaderId': entries[i]['leaderId']
                })
            
            if(len(self.log) == 0):
                self.lastLogIndex = -1
            else:
                self.lastLogIndex = self.log[len(self.log)-1]['idx']

            # Execute leader commit entries
            toCommit = min(len(self.log),leaderCommit)
            for i in range(self.lastCommitIndex+1,toCommit+1):
                self.doExecution(self.log[i]['executionType'],self.log[i]['value'])
            
            self.lastCommitIndex = toCommit
            return {'term':self.currentTerm,'lastLogIndex':self.lastLogIndex, 'success':True, 'port': self.address.port}

        else:
            self.print_log(f'Append log entries from {leaderId} failed because there exists inconsistency in previous log data {prevLogIndex} {prevLogTerm}')
            return {'term':self.currentTerm,'lastLogIndex':self.lastLogIndex, 'success':False, 'port': self.address.port}


    # LEADER ELECTION

    # Start election, called by candidate
    async def election(self) -> None:
        self.print_log('Election started')
        self.termAndLastLogIndexLock.acquire()
        self.currentTerm += 1
        self.currentVote = self.address.port
        self.termAndLastLogIndexLock.release()
        urlList = [f'http://localhost:{followerPort}/election' for followerPort in self.listPortFollower if followerPort != self.address.port]
        lastLogTerm = 0
        if(len(self.log)>0):
            lastLogTerm = self.log[len(self.log)-1]["term"] 
        paramsList = [{'currentTerm':self.currentTerm,'logLength': len(self.log), 'lastLogTerm': lastLogTerm, 'portRaft':self.address.port,'ipRaft':'localhost'} for followerPort in self.listPortFollower if followerPort != self.address.port]
        print("Mulai gan")
        resList = await HttpWrapper.multipleRequestPostWithMaxTimeout(urlList, paramsList, RaftNode.MAX_CNT_TIMEOUT,24)
        print("Selesai gan")
        total = 1
        print(resList)
        for res in resList['result']:
            if 'status' in res:
                total += 1 if res['status'] == True else 0
                self.print_log(f'Vote for term {self.currentTerm} is {res["status"]}')
            else:
                self.print_log(res)
                self.print_log(f'Error vote for term {self.currentTerm} is {res["err"]}')
        self.print_log(f'Total vote for term {self.currentTerm} is {total} {self.listPortFollower}')
        if(total >= (len(self.listPortFollower)+1) / 2):
            self.termAndLastLogIndexLock.acquire()
            self.raftNodeType = RaftNode.RaftNodeType.LEADER
            self.contactAddress = self.address
            self.termAndLastLogIndexLock.release()
            self.changeToLeader()

    # Receive election request, only accept if term valid and sender log is not behind
    def receiveElection(self, term:int, logLength:int, lastLogTerm: int, port:int) -> dict:
        self.termAndLastLogIndexLock.acquire()            
        myLastLogTerm = 0
        if(len(self.log)>0):
            myLastLogTerm = self.log[len(self.log)-1]["term"]
        self.print_log(f'{lastLogTerm} {myLastLogTerm} {logLength} {len(self.log)} {term} {self.currentTerm} {self.currentVote}')
        isLogOk = (lastLogTerm > myLastLogTerm) or (lastLogTerm == myLastLogTerm and logLength >= len(self.log))
        if(term > self.currentTerm):
            self.currentTerm = term
            self.raftNodeType = RaftNode.RaftNodeType.FOLLOWER
            self.lastLeaderSeen = time.time()
            self.print_log(f'Vote for port {port} is True')
            self.termAndLastLogIndexLock.release()
            self.currentVote = port
            if(isLogOk):
                return {'status':True}
            else:
                return {'status':False}
        self.termAndLastLogIndexLock.release()
        return {'status':False}

    
    # EXECUTION UTILITIES
        
    # Handle enqueue and dequeue
    def doExecution(self,executionType:str, value:str) -> None:
        self.print_log(f'Do Execution in queue for {executionType} with value {value}')
        if(executionType == ExecutionType.ENQUEUE.value):
            self.enqueue(value)
        else:
            self.dequeue()

    # Do enqueue
    def enqueue(self, value:str) -> None:
        self.print_log(f'Enqueue {value}')
        self.q.append(value)
        return {'result': "Enqueue completed"}

    # Do dequeue
    def dequeue(self) -> None:
        self.print_log(f'Dequeue')
        if(len(self.q) == 0):
            return {'result': "Queue is empty"}
        else:
            return {'result': self.q.popleft()}
        
    
    # LOG UTILITIES
    
    # Handle get log request from client
    def getLog(self) -> dict:
        self.print_log(f'Getting log')

        return {'result':  self.log}

    # Only for logging
    def print_log(self, message:str) -> None:
        GREEN = '\033[92m'
        ENDC = '\033[0m'
        print(f'{GREEN}[{datetime.now()}] [{self.address}]{ENDC} {message}')