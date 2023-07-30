from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from address import Address
import sys
from pydantic import BaseModel
from httpwrapper import HttpWrapper
import time
from typing import Union
from xmlrpc.client import ServerProxy

app = FastAPI()

origins = [
    "http://localhost:8090",
    "http://localhost:3000"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def read_root():
    return {"Hello": "World"}

class ExecuteRequestBody(BaseModel):
    portRaft: int
    executionType: str
    valueNum: int

class Raft(BaseModel):
    portLeader: int
    listPortFollower: list = []

app.state.raft = {}

@app.post("/execute/{portClient}")
async def requestExecute(portClient:int, body: ExecuteRequestBody):
    clientProxy = ServerProxy(f'http://localhost:{portClient}/')
    res = clientProxy.requestExecute(body.executionType, body.portRaft, body.valueNum)
    return res

@app.post("/log/{portClient}/{portRaft}")
async def requestLog(portClient:int, portRaft:int):
    clientProxy = ServerProxy(f'http://localhost:{portClient}/')
    res = clientProxy.requestLog(portRaft)
    return res

@app.get("/log/{portRaft}")
async def getLog(portRaft: int):
    # clientProxy = ServerProxy(f'http://localhost:{portRaft}/')
    # res = clientProxy.getLog()
    # return res
    print(f'Requesting log to {portRaft}')
    res = await HttpWrapper.singleRequestGetWithMaxTimeout(f'http://localhost:{portRaft}/log')
    return res

@app.post("/raft")
async def requestRaft(rafts: Raft):
    app.state.raft = rafts
    return {"return": "success"}

@app.get("/raft")
async def getRaft():
    print(app.state.raft)
    print(app.state.raft.portLeader)
    print(app.state.raft.listPortFollower)
    data = []
    # time.sleep(5)
    for port in app.state.raft.listPortFollower:
        portdata = {}
        portdata["address"] = port
        if port == app.state.raft.portLeader:
            portdata["type"] = "leader"
        else:
            portdata["type"] = "follower"
        log = await HttpWrapper.singleRequestGetWithMaxTimeout(f'http://localhost:{port}/log')
        portdata["log"] = log['result']
        print(portdata["log"])
        data.append(portdata)
    print(data)

    return data

if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=int(sys.argv[1]))