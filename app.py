from flask import Flask, render_template, current_app
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
import logging
from logging.handlers import RotatingFileHandler

# Configure logging
if not os.path.exists('logs'):
    os.makedirs('logs')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
)
file_handler = RotatingFileHandler('logs/bsky_monitor.log', maxBytes=10240000, backupCount=10)
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
))
file_handler.setLevel(logging.INFO)

# Initialize Flask app with configuration
app = Flask(__name__)
app.config.update(
    SECRET_KEY=os.environ.get('SECRET_KEY', 'dev'),
    STATS_FILE=os.environ.get('STATS_FILE', 'firehose_stats.json'),
    MAX_MINUTES=int(os.environ.get('MAX_MINUTES', 10)),
    WS_HOST=os.environ.get('WS_HOST', 'wss://jetstream2.us-west.bsky.network/subscribe'),
    DEBUG=os.environ.get('FLASK_DEBUG', '0') == '1'
)

app.logger.addHandler(file_handler)
sock = Sock(app)

STATS_FILE = app.config['STATS_FILE']
MAX_MINUTES = app.config['MAX_MINUTES']

# Store multiple minutes of data
minutes_data = []
# Last broadcast timestamp
last_broadcast = 0

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
    global last_broadcast
    uri = app.config['WS_HOST']
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                while True:
                    message = await websocket.recv()
                    message_times.append(time.time())
                    process_message(json.loads(message))
                    
                    # Throttle broadcasts to once every 50ms
                    current_time = time.time()
                    if current_time - last_broadcast >= 0.05:  # 50ms = 0.05s
                        await broadcast_to_clients()
                        last_broadcast = current_time
        except Exception as e:
            current_app.logger.error(f"WebSocket error: {str(e)}", exc_info=True)
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
        current_app.logger.error(f"WebSocket error: {str(e)}", exc_info=True)
    finally:
        # Clean up connection
        active_connections.discard(ws)

def run_websocket():
    try:
        asyncio.run(process_messages())
    except Exception as e:
        current_app.logger.error(f"WebSocket error: {str(e)}", exc_info=True)
        # Add a small delay before reconnecting
        time.sleep(5)
        run_websocket()

if __name__ == '__main__':
    # Load existing stats before starting
    load_stats_from_disk()
    
    # Start the WebSocket client in a separate thread
    websocket_thread = threading.Thread(target=run_websocket)
    websocket_thread.daemon = True
    websocket_thread.start()
    
    # Start periodic save in another thread
    save_thread = threading.Thread(target=periodic_save)
    save_thread.daemon = True
    save_thread.start()
    
    # Use Werkzeug's development server only in development
    if app.config['DEBUG']:
        app.run(debug=True, use_reloader=False)
    else:
        # In production, this will be handled by Gunicorn
        app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 4200)))
