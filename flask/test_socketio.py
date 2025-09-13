from flask import Flask
from flask_socketio import SocketIO
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

socketio = SocketIO(app, cors_allowed_origins="*")

@app.route('/')
def index():
    return "Test SocketIO Server is running!"

@socketio.on('connect')
def on_connect():
    print('Client connected')

@socketio.on('disconnect')
def on_disconnect():
    print('Client disconnected')

if __name__ == '__main__':
    socketio.run(app, host='127.0.0.1', port=5000, debug=True)
