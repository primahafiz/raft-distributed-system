import sys
from client import Client
from address import Address
from fastapi import FastAPI
import uvicorn
from pydantic import BaseModel

client: Client
app = FastAPI()

class ExecuteRequestBody(BaseModel):
    portRaft: int
    executionType: str
    value: str

@app.post("/execute")
async def requestExecute(body: ExecuteRequestBody):
    res = await client.requestExecute(body.executionType, body.portRaft, body.value)
    return res

@app.get("/log/{portRaft}")
async def requestLog(portRaft:int):
    res = await client.requestLog(portRaft)
    return res

@app.post("/membership/remove")
async def requestRemoveNode(portRaft:int, toBeRemovedPort:int):
    res = await client.requestRemoveNode(portRaft, toBeRemovedPort)
    return res

if __name__ == "__main__":
    ip = sys.argv[1]
    port = sys.argv[2]

    client = Client(Address(port,ip))
    uvicorn.run(app, host="localhost", port=int(port))
