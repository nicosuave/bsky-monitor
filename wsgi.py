from app import app, run_websocket, load_stats_from_disk, periodic_save
import threading

# Create WSGI application
application = app

if __name__ == "__main__":
    # Production configuration for Flask dev server
    app.config['ENV'] = 'production'
    app.config['DEBUG'] = False

    # Load existing stats before starting
    load_stats_from_disk()
    
    # Start the WebSocket client in a separate thread
    websocket_thread = threading.Thread(target=run_websocket)
    websocket_thread.daemon = True
    websocket_thread.start()
    
    # Start the periodic save thread
    save_thread = threading.Thread(target=periodic_save)
    save_thread.daemon = True
    save_thread.start()

    # Start the Flask app
    app.run(
        host='0.0.0.0',
        port=4200,
        threaded=True,
        processes=1,  # Keep single process for WebSocket
        use_reloader=False  # Disable reloader in production
    )
