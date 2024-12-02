from flask import Flask, render_template
import asyncio
import websockets
import json
from datetime import datetime
from collections import defaultdict, deque
import threading
from flask_sock import Sock
import operator
import weakref
import time
import os

app = Flask(__name__)
sock = Sock(app)

STATS_FILE = 'firehose_stats.json'

# Store multiple minutes of data
minutes_data = []
MAX_MINUTES = 10

# Global state to store message counts
current_minute_stats = {
    'minute': None,
    'counts': defaultdict(int)
}

# Message rate tracking
message_times = deque(maxlen=1000)  # Track last 1000 messages for rate calculation

# Active WebSocket connections
active_connections = weakref.WeakSet()

def load_stats_from_disk():
    """Load stats from disk if they exist"""
    global minutes_data, current_minute_stats
    
    if not os.path.exists(STATS_FILE):
        return
    
    try:
        with open(STATS_FILE, 'r') as f:
            data = json.load(f)
            
        # Load current stats
        current = data.get('current', {})
        if current:
            current_minute_stats['minute'] = current.get('minute')
            # Convert counts back to defaultdict
            counts = current.get('counts', {})
            current_minute_stats['counts'] = defaultdict(int, counts)
        
        # Load history
        history = data.get('history', [])
        minutes_data = history[-MAX_MINUTES:]  # Only keep up to MAX_MINUTES
        
        print(f"Loaded stats from {STATS_FILE}")
    except Exception as e:
        print(f"Error loading stats: {e}")

def calculate_message_rate():
    """Calculate messages per second based on recent messages"""
    now = time.time()
    # Remove messages older than 5 seconds
    cutoff = now - 5
    while message_times and message_times[0] < cutoff:
        message_times.popleft()
    
    # Calculate rate over the remaining messages
    if len(message_times) < 2:
        return 0
    
    time_span = message_times[-1] - message_times[0]
    if time_span <= 0:
        return 0
    
    return len(message_times) / time_span

def save_stats_to_disk():
    """Save current stats and history to disk"""
    data = {
        'last_updated': datetime.now().isoformat(),
        'current': {
            'minute': current_minute_stats['minute'],
            'counts': dict(current_minute_stats['counts'])
        },
        'history': minutes_data
    }
    
    # Write to temporary file first, then move it to final location
    temp_file = f'{STATS_FILE}.tmp'
    try:
        with open(temp_file, 'w') as f:
            json.dump(data, f, indent=2)
        os.replace(temp_file, STATS_FILE)
    except Exception as e:
        print(f"Error saving stats: {e}")

async def periodic_save():
    """Periodically save stats to disk"""
    while True:
        save_stats_to_disk()
        await asyncio.sleep(5)

def sort_stats():
    # Convert current stats to sorted list of tuples
    stats = []
    for key, value in current_minute_stats['counts'].items():
        if ':' in key:  # Only include commit messages
            stats.append((key, value))
    
    # Sort by count in descending order
    return sorted(stats, key=lambda x: x[1], reverse=True)

async def broadcast_to_clients():
    """Send updates to all connected clients"""
    dead_connections = set()
    
    for ws in active_connections:
        try:
            ws.send(json.dumps({
                'current': {
                    'minute': current_minute_stats['minute'],
                    'stats': sort_stats(),
                    'message_rate': calculate_message_rate(),
                    'last_update': datetime.now().strftime('%H:%M:%S.%f')[:-3]
                },
                'history': minutes_data
            }))
        except Exception:
            dead_connections.add(ws)
    
    # Remove dead connections
    for ws in dead_connections:
        active_connections.discard(ws)

async def process_messages():
    uri = "wss://jetstream2.us-west.bsky.network/subscribe"
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                while True:
                    message = await websocket.recv()
                    message_times.append(time.time())
                    process_message(json.loads(message))
                    # Broadcast updates to clients
                    await broadcast_to_clients()
        except Exception as e:
            print(f"WebSocket error: {e}")
            await asyncio.sleep(5)

def process_message(message):
    global minutes_data
    current_time = datetime.now()
    current_minute = current_time.strftime('%Y-%m-%d %H:%M')
    
    if current_minute_stats['minute'] != current_minute:
        if current_minute_stats['minute'] is not None:
            # Store the previous minute's data
            minutes_data.append({
                'minute': current_minute_stats['minute'],
                'counts': dict(current_minute_stats['counts'])
            })
            # Keep only last MAX_MINUTES
            while len(minutes_data) > MAX_MINUTES:
                minutes_data.pop(0)
        
        current_minute_stats['minute'] = current_minute
        current_minute_stats['counts'] = defaultdict(int)
    
    # Process message based on its kind
    if message.get('kind') == 'commit':
        commit = message.get('commit', {})
        collection = commit.get('collection', 'unknown')
        operation = commit.get('operation', 'unknown')
        key = f"{collection}:{operation}"
        current_minute_stats['counts'][key] += 1

@app.route('/')
def home():
    return render_template('index.html')

@sock.route('/stats')
def stats_socket(ws):
    """Handle WebSocket connections for real-time stats updates"""
    try:
        # Add connection to active set
        active_connections.add(ws)
        
        # Send initial data
        ws.send(json.dumps({
            'current': {
                'minute': current_minute_stats['minute'],
                'stats': sort_stats(),
                'message_rate': calculate_message_rate(),
                'last_update': datetime.now().strftime('%H:%M:%S.%f')[:-3]
            },
            'history': minutes_data
        }))
        
        # Keep connection alive
        while True:
            # Wait for client ping
            message = ws.receive()
            if message == "ping":
                ws.send("pong")
    except Exception as e:
        print(f"WebSocket error: {e}")
    finally:
        # Clean up connection
        active_connections.discard(ws)

def run_websocket():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Create tasks for message processing and periodic saving
    tasks = [
        loop.create_task(process_messages()),
        loop.create_task(periodic_save())
    ]
    
    # Run all tasks concurrently
    loop.run_until_complete(asyncio.gather(*tasks))

if __name__ == '__main__':
    # Load existing stats before starting
    load_stats_from_disk()
    
    # Start WebSocket client in a separate thread
    websocket_thread = threading.Thread(target=run_websocket, daemon=True)
    websocket_thread.start()
    
    # Run Flask app
    app.run(debug=True, host='0.0.0.0', port=4200)
