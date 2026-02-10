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

# --- UNIVERSAL GET FUNCTION (The Standard Pattern) ---
def generic_get(table_name, pk_column):
    ids_param = request.args.get('ids')
    
    # 1. Parse Pagination (Default to Page 1, Limit 20)
    try:
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 20))
    except ValueError:
        page = 1
        limit = 20
    
    offset = (page - 1) * limit
    cur = get_db().cursor()
    
    if ids_param and ids_param.strip():
        # --- BATCH MODE (Sync Updates) ---
        # Fetch specific IDs. Ignore paging logic here.
        id_list = ids_param.split(',')
        placeholders = ','.join('?' for _ in id_list)
        
        query = f"SELECT * FROM {table_name} WHERE {pk_column} IN ({placeholders})"
        cur.execute(query, id_list)
        rows = cur.fetchall()
        
        total_count = len(rows)
        data = [dict(row) for row in rows]
        
    else:
        # --- PAGING MODE (Initial Load) ---
        # 1. Get Total Count (Crucial for Appian)
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_count = cur.fetchone()[0]
        
        # 2. Get Data Slice
        query = f"SELECT * FROM {table_name} LIMIT ? OFFSET ?"
        cur.execute(query, (limit, offset))
        rows = cur.fetchall()
        data = [dict(row) for row in rows]

    # --- RETURN STANDARD ENVELOPE ---
    # This structure { pagination: {...}, data: [...] } is what Appian expects.
    return jsonify({
        "pagination": {
            "total": total_count,
            "limit": limit,
            "offset": offset,
            "total_pages": (total_count + limit - 1) // limit if limit > 0 else 1,
            "current_page": page
        },
        "data": data
    })

# --- ROUTE HANDLERS ---

@app.route('/artists', methods=['GET'])
def get_artists(): return generic_get('artist', 'artist_id')

@app.route('/artists/<int:id>', methods=['GET'])
def get_artist(id):
    cur = get_db().cursor()
    cur.execute("SELECT * FROM artist WHERE artist_id = ?", (id,))
    row = cur.fetchone()
    # Return single object
    return jsonify(dict(row) if row else {})

@app.route('/albums', methods=['GET'])
def get_albums(): return generic_get('album', 'album_id')

@app.route('/albums/<int:id>', methods=['GET'])
def get_album(id):
    cur = get_db().cursor()
    cur.execute("SELECT * FROM album WHERE album_id = ?", (id,))
    row = cur.fetchone()
    return jsonify(dict(row) if row else {})

@app.route('/songs', methods=['GET'])
def get_songs(): return generic_get('song', 'song_id')

@app.route('/songs/<int:id>', methods=['GET'])
def get_song(id):
    cur = get_db().cursor()
    cur.execute("SELECT * FROM song WHERE song_id = ?", (id,))
    row = cur.fetchone()
    return jsonify(dict(row) if row else {})


# --- CREATE APIs (PUT) ---
# Use these to re-seed your data after deployment

@app.route('/artists', methods=['PUT'])
def create_artist():
    data = request.get_json() or {}
    db = get_db()
    cur = db.cursor()
    cur.execute(
        "INSERT INTO artist (artist_name, status_id, updated_on, updated_by) VALUES (?, ?, ?, ?)",
        (data.get('artist_name', 'Unknown'), 1, get_timestamp(), data.get('updated_by', 'Appian'))
    )
    db.commit()
    return jsonify({"id": cur.lastrowid, "message": "Artist created"}), 201

@app.route('/albums', methods=['PUT'])
def create_album():
    data = request.get_json() or {}
    db = get_db()
    cur = db.cursor()
    # Create Album
    cur.execute(
        "INSERT INTO album (artist_id, album_name, updated_on, updated_by) VALUES (?, ?, ?, ?)",
        (data.get('artist_id'), data.get('album_name'), get_timestamp(), data.get('updated_by', 'Appian'))
    )
    # Update Parent (Artist)
    cur.execute("UPDATE artist SET updated_on = ? WHERE artist_id = ?", (get_timestamp(), data.get('artist_id')))
    db.commit()
    trigger_appian_sync(data.get('artist_id'))
    return jsonify({"id": cur.lastrowid, "message": "Album created"}), 201

@app.route('/songs', methods=['PUT'])
def create_song():
    data = request.get_json() or {}
    db = get_db()
    cur = db.cursor()
    # Create Song
    cur.execute(
        "INSERT INTO song (artist_id, album_id, song_name, updated_on, updated_by) VALUES (?, ?, ?, ?, ?)",
        (data.get('artist_id'), data.get('album_id'), data.get('song_name'), get_timestamp(), data.get('updated_by', 'Postman'))
    )
    # Update Parent (Artist)
    cur.execute("UPDATE artist SET updated_on = ? WHERE artist_id = ?", (get_timestamp(), data.get('artist_id')))
    db.commit()
    trigger_appian_sync(data.get('artist_id'))
    return jsonify({"id": cur.lastrowid, "message": "Song created"}), 201

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
