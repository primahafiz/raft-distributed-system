from typing import Any

class Address:
    def __init__(self, port:str, ip:str='localhost') -> None:
        self.port = int(port)
        self.ip = ip

    def getAddress(self) -> str:
        return f'{self.ip}:{self.port}'
    
    def __str__(self) -> str:
        return self.getAddress()
    
    def __eq__(self, other: any) -> bool:
        return True if self.getAddress() == other.getAddress() else False