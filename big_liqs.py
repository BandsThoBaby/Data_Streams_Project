import asyncio
import json
import os
from datetime import datetime
from websockets import connect
from termcolor import cprint

# WebSocket URL and CSV file path
websocket_url = 'wss://fstream.binance.com/ws/!forceOrder@arr'
filename = 'binance_bigliqs.csv'

# Create CSV file with headers if it doesn't exist
if not os.path.isfile(filename):
    with open(filename, 'w') as f:
        f.write("symbol,side,order_type,time_in_force,original_quantity,price,average_price,order_status,order_last_filled_quantity,order_filled_accumulated_quantity,order_trade_time,usd_size\n")

# Async function to connect to Binance WebSocket and process messages
async def binance_liquidation(uri, filename):
    async with connect(uri) as websocket:
        while True:
            try:
                msg = await websocket.recv()  # Receive message
                order_data = json.loads(msg)['o']  # Parse JSON data

                # Extract relevant fields
                symbol = order_data['s']
                side = order_data['S']
                price = float(order_data['p'])
                quantity = float(order_data['q'])
                usd_size = price * quantity

                # Log to CSV if usd_size is above a certain threshold
                if usd_size > 10000:
                    with open(filename, 'a') as f:
                        f.write(f"{symbol},{side},{order_data['o']},{order_data['f']},{quantity},{price},{order_data['ap']},{order_data['X']},{order_data['l']},{order_data['z']},{order_data['T']},{usd_size}\n")
                    
                    # Define the color based on the trade side
                    color = 'blue' if side == 'SELL' else 'magenta'
                    cprint(f"{symbol} {side} at {price} for {quantity} (USD Size: {usd_size:,.2f})", 'white', f'on_{color}')
                    print('')  # Blank line for readability

            except Exception as e:
                print(f"Error: {e}")
                await asyncio.sleep(5)

# Main entry point to start the async loop
if __name__ == "__main__":
    asyncio.run(binance_liquidation(websocket_url, filename))