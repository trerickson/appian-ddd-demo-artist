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
    """Initializes the database with tables and sample data if missing."""
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        
        # Create Tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS artist (
                artist_id INTEGER PRIMARY KEY AUTOINCREMENT,
                artist_name VARCHAR(255),
                status_id INTEGER DEFAULT 1,
                updated_on DATETIME,
                updated_by VARCHAR(255)
            )
        ''')
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
        
        # Check if empty, if so seed data (so you don't have to keep using Postman)
        cursor.execute("SELECT count(*) FROM artist")
        if cursor.fetchone()[0] == 0:
            cursor.execute("INSERT INTO artist (artist_name, updated_on, updated_by) VALUES ('The Beatles', ?, 'System')", (get_timestamp(),))
            cursor.execute("INSERT INTO artist (artist_name, updated_on, updated_by) VALUES ('Pink Floyd', ?, 'System')", (get_timestamp(),))
            cursor.execute("INSERT INTO artist (artist_name, updated_on, updated_by) VALUES ('Led Zeppelin', ?, 'System')", (get_timestamp(),))
            db.commit()
            print("Database initialized and seeded.")
        else:
            db.commit()

def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# --- WEBHOOK TRIGGER ---
def trigger_appian_sync(artist_id):
    if APPIAN_WEBHOOK_URL:
        try:
            print(f"FIRING WEBHOOK to Appian for Artist ID: {artist_id}")
            requests.post(
                APPIAN_WEBHOOK_URL, 
                json={"artistId": artist_id},
                headers={"Appian-API-Key": APPIAN_API_KEY, "Content-Type": "application/json"},
                timeout=5
            )
        except Exception as e:
            print(f"Webhook failed: {e}")

# --- APPIAN-OPTIMIZED GET FUNCTION ---
# This is the secret sauce. It handles both "Source" and "Sync" patterns.
def generic_get_appian(table_name, pk_column):
    # 1. Check for Sync Parameters (The "Batch" Request)
    ids_param = request.args
