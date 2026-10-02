import asyncio
import os
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

# Load environment variables (.env)
load_dotenv()

def display_market_data(payload: dict, console: Console):
    """Parses Tastytrade API nested dictionary and displays a static table."""
    items = payload.get("data", {}).get("items", [])
    if not items:
        return
        
    data = items[0]
    
    volume_raw = data.get('volume', 0)
    volume_fmt = f"{float(volume_raw):,.0f}" if volume_raw else "N/A"
    
    table = Table(
        title=f"Live Market Data: {data.get('symbol')}", 
        show_header=True, 
        header_style="bold green"
    )
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="magenta", justify="right")
    
    table.add_row("Last / Mark", f"{data.get('last')} / {data.get('mark')}")
    table.add_row("Bid x Size", f"{data.get('bid')} x {data.get('bid-size')}")
    table.add_row("Ask x Size", f"{data.get('ask')} x {data.get('ask-size')}")
    table.add_row("Day High", str(data.get('day-high-price')))
    table.add_row("Day Low", str(data.get('day-low-price')))
    table.add_row("Volume", volume_fmt)
    table.add_row("Updated", str(data.get('updated-at')))
    
    console.clear()
    console.print(table)

async def tastytrade_producer(queue: asyncio.Queue, symbol: str):
    """
    Polls Tastytrade API and places payload into the shared memory queue.
    Replace mock dictionary with actual client requests once authenticated.
    """
    while True:
        # Mock payload structured as returned by Tastytrade API
        response = {
            "data": {
                "items": [{
                    "symbol": symbol,
                    "last": "218.45", "mark": "218.47",
                    "bid": "218.40", "bid-size": "500",
                    "ask": "218.50", "ask-size": "300",
                    "day-high-price": "220.10", "day-low-price": "217.80",
                    "volume": 4500000, "updated-at": "2026-10-02T15:40:00Z"
                }]
            }
        }
        
        await queue.put({"provider": "tastytrade", "payload": response})
        await asyncio.sleep(1)

async def dashboard_consumer(queue: asyncio.Queue, console: Console):
    """Consumes tick data from async queue and updates UI."""
    while True:
        data_event = await queue.get()
        provider = data_event.get("provider")
        payload = data_event.get("payload")
        
        if provider == "tastytrade":
            display_market_data(payload, console)
            
        queue.task_done()

async def main():
    console = Console()
    data_queue = asyncio.Queue()
    target_symbol = "IWM"
    
    console.print(f"[yellow]Initializing Market Data Engine for {target_symbol}...[/yellow]")
    
    producer_task = asyncio.create_task(tastytrade_producer(data_queue, target_symbol))
    consumer_task = asyncio.create_task(dashboard_consumer(data_queue, console))
    
    try:
        await asyncio.gather(producer_task, consumer_task)
    except asyncio.CancelledError:
        console.print("[red]Shutting down engine tasks.[/red]")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExited gracefully.")