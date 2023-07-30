import sys
from raft import RaftNode
from address import Address
from fastapi import FastAPI
import uvicorn
from pydantic import BaseModel
from enums import ExecutionType
from typing import Any,List

raft: RaftNode
app = FastAPI()

class AddressRequestBody(BaseModel):
    ipRaft: str
    portRaft: int

class MembershipRequestBody(BaseModel):
    ipRaft: str
    portRaft: int
    mode: str

@app.post("/membership/add")
async def addMembership(body: AddressRequestBody):
    res = await raft.receiveApplyMembership(body.portRaft)
    return res

@app.post("/membership/remove")
async def removeMembership(body: AddressRequestBody):
    res = await raft.receiveRemoveMembership(body.portRaft)
    return res

@app.post("/membership/notify")
async def notifyMembershipChange(body: MembershipRequestBody):
    res = raft.updateMembership(body.portRaft, body.mode)
    return res

class HeartbeatRequestBody(BaseModel):
    ipRaft: str
    portRaft: int
    currentTerm: int
    lastLogIndex: int

@app.post("/heartbeat")
async def heartbeat(body: HeartbeatRequestBody):
    res = raft.receiveHeartbeat(Address(body.portRaft,body.ipRaft), body.currentTerm, body.lastLogIndex)
    return res

class ElectionRequestBody(BaseModel):
    ipRaft: str
    portRaft: int
    currentTerm: int
    logLength: int
    lastLogTerm: int

@app.post("/election")
async def election(body: ElectionRequestBody):
    res = raft.receiveElection(body.currentTerm, body.logLength, body.lastLogTerm, body.portRaft)
    return res

class ExecuteRaftRequestBody(BaseModel):
    timestamp:float
    ipClient: str
    portClient: int
    executionType: str
    value: Any

@app.post("/execute")
async def execute(body: ExecuteRaftRequestBody):
    res = await raft.execute(body.timestamp,body.ipClient,body.portClient, body.executionType, body.value)
    return res

@app.post("/execute/do")
async def executeDo(body: ExecuteRaftRequestBody):
    res = raft.doExecution(body.timestamp,body.ipClient,body.portClient,body.executionType, body.value)
    return res

class AppendEntriesRequestBody(BaseModel):
    term: int
    leaderId: int
    prevLogIndex: int
    prevLogTerm: int
    prevExecutionType: str
    prevValue: Any
    entries: List[dict]
    leaderCommit: int

@app.post("/entries/append")
async def appendEntries(body: AppendEntriesRequestBody):
    res = await raft.receiveAppendEntries(body.term,body.leaderId,body.prevLogIndex,body.prevLogTerm,body.prevExecutionType,body.prevValue,body.entries,body.leaderCommit)
    return res

@app.get("/log")
async def log():
    res = raft.getLog()
    return res

if __name__ == "__main__":
    ip = sys.argv[1]
    port = sys.argv[2]
    contactIp = sys.argv[3]
    contactPort = sys.argv[4]

    raft = RaftNode(Address(port,ip),Address(contactPort,contactIp))
    uvicorn.run(app, host="localhost", port=int(port))
