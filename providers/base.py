from abc import ABC, abstractmethod
import asyncio

class MarketDataSource(ABC):
    def __init__(self, provider_name: str):
        self.provider_name = provider_name
        self.is_connected = False

    @abstractmethod
    async def connect(self) -> bool:
        """Authenticate with the provider."""
        pass

    @abstractmethod
    async def stream_data(self, symbol: str, queue: asyncio.Queue):
        """Stream data into the shared queue."""
        pass