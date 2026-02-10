import sqlite3
import os
import requests
from flask import Flask, jsonify, request, g
from datetime import datetime
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Allows Appian to talk to this API

DB_FILE = "music_domain.db"

# --- CONFIGURATION ---
APPIAN_WEBHOOK_URL = os.environ.get("APPIAN_WEBHOOK_URL", "")
APPIAN_API_KEY = os.environ.get("APPIAN_API_KEY", "")

# --- DATABASE HELPERS ---
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DB_FILE)
        db.row_factory = sqlite3.Row
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def init_db():
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        
        # Artist Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS artist (
                artist_id INTEGER PRIMARY KEY AUTOINCREMENT,
                artist_name VARCHAR(255),
                status_id INTEGER DEFAULT 1,
                updated_on DATETIME,
                updated_by VARCHAR(255)
            )
        ''')
        # Album Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS album (
                album_id INTEGER PRIMARY KEY AUTOINCREMENT,
                artist_id INTEGER NOT NULL,
                album_name VARCHAR(255),
                updated_on DATETIME,
                updated_by VARCHAR(255),
                FOREIGN KEY(artist_id) REFERENCES artist(artist_id)
            )
        ''')
        # Song Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS song (
                song_id INTEGER PRIMARY KEY AUTOINCREMENT,
                artist_id INTEGER NOT NULL,
                album_id INTEGER NOT NULL,
                song_name VARCHAR(255),
                updated_on DATETIME,
                updated_by VARCHAR(255),
                FOREIGN KEY(artist_id) REFERENCES artist(artist_id),
                FOREIGN KEY(album_id) REFERENCES album(album_id)
            )
        ''')
        db.commit()

# Initialize immediately
if not os.path.exists(DB_FILE):
    init_db()
else:
    init_db()

def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# --- WEBHOOK TRIGGER ---
def trigger_appian_sync(artist_id
