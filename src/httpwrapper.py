import aiohttp,asyncio
from typing import List
from threading import Thread

class HttpWrapper:
    def __init__(self) -> None:
        pass

    @staticmethod
    async def singleRequestPost(url:str, params:dict=None, timeout:int=10) -> dict:
        sessionTimeout = aiohttp.ClientTimeout(total=timeout)
        async with aiohttp.ClientSession(timeout=sessionTimeout) as session:
            async with session.post(url,json=params) as resp:
                return await resp.json()

    @staticmethod
    async def singleRequestGet(url:str, timeout:int=10) -> dict:
        sessionTimeout = aiohttp.ClientTimeout(total=timeout)
        async with aiohttp.ClientSession(timeout=sessionTimeout) as session:
            async with session.get(url) as resp:
                return await resp.json()
    
    @staticmethod
    async def singleRequestGetWithMaxTimeout(url:str, maxCountTimeout:int=5, timeout:int=10) -> dict:
        for _ in range(maxCountTimeout):
            try:
                return await HttpWrapper.singleRequestGet(url,timeout)
            except Exception as e:
                print("Request Timeout. Retrying...")
                pass
        return {'err':  "Request timeout"}

    @staticmethod
    async def singleRequestPostWithMaxTimeout(url:str, maxCountTimeout:int=5, params:dict=None, timeout:int=10) -> dict:
        for _ in range(maxCountTimeout):
            try:
                return await HttpWrapper.singleRequestPost(url,params,timeout)
            except Exception as e:
                print("Request Timeout. Retrying...")
                pass
        return {'err':  "Request timeout"}
    
    @staticmethod
    async def solveRet(res, finished, url, params, timeout, idx):
        try:
            res[idx] = await HttpWrapper.singleRequestPost(url, params, timeout)
            finished[idx] = True
            print(f"Request to {url} finished")            
        except Exception as e:
            print("exception =",e)

    @staticmethod
    async def multipleRequestPostWithMaxTimeout(urlList:List[str], paramsList:List[dict], maxCountTimeout:int=5, timeout:int=10) -> dict:

        finished = [False for i in range(len(urlList))]
        ret = [None for i in range(len(urlList))]
        res = [None for i in range(len(urlList))]
        for _ in range(maxCountTimeout):
            for i in range(len(urlList)):
                if not finished[i]:
                    print(f"Request to {urlList[i]} started")
                    ret[i] = Thread(target=asyncio.run,args=[HttpWrapper.solveRet(res, finished, urlList[i], paramsList[i], timeout, i)])
                    ret[i].start()

            for i in range(len(urlList)):
                if not finished[i]:
                    ret[i].join()
        for i in range(len(urlList)):
            if(not finished[i]):
                res[i] = {'err':  "Request timeout"}

            # try:
            #     res = await asyncio.gather(*ret)
            #     print("Successful",res)
            #     return {'result':  res}
            # except Exception as e:
            #     print(f'Exception: {repr(e)}')
            #     for i in range(len(urlList)):
            #         if(not finished[i]):
            #             res[i] = {'err':  "Request timeout"}
                
        return {'result':  res}