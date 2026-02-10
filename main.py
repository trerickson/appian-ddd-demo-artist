# --- APPIAN-OPTIMIZED GET FUNCTION ---
def generic_get(table_name, pk_column):
    # 1. Look for Appian's Native "Sync" Parameter (ids)
    ids_param = request.args.get('ids')
    
    # 2. Look for Appian's Native "Source" Parameters (startIndex, batchSize)
    # Default to 1 and -1 (All) if missing
    try:
        start_index = int(request.args.get('startIndex', 1))
        batch_size = int(request.args.get('batchSize', -1))
    except ValueError:
        start_index = 1
        batch_size = -1
        
    cur = get_db().cursor()
    
    # --- SCENARIO A: SYNC (The "Batch" Update) ---
    if ids_param and ids_param.strip():
        id_list = ids_param.split(',')
        placeholders = ','.join('?' for _ in id_list)
        query = f"SELECT * FROM {table_name} WHERE {pk_column} IN ({placeholders})"
        cur.execute(query, id_list)
        rows = cur.fetchall()
        
        # Sync expects a RAW LIST. No "total" wrapper needed for this part.
        return jsonify([dict(row) for row in rows])
        
    # --- SCENARIO B: SOURCE (The "Grid" Load) ---
    else:
        # 1. Get Total (Required for DataSubset)
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_count = cur.fetchone()[0]
        
        # 2. Calculate SQL Logic from Appian Params
        # Appian is 1-based, SQL is 0-based.
        offset = max(0, start_index - 1)
        
        # Handle "Get All" (-1) vs "Paged"
        if batch_size == -1:
            query = f"SELECT * FROM {table_name} LIMIT -1 OFFSET ?"
            params = (offset,)
        else:
            query = f"SELECT * FROM {table_name} LIMIT ? OFFSET ?"
            params = (batch_size, offset)
            
        cur.execute(query, params)
        rows = cur.fetchall()
        
        # Return the ENVELOPE Appian needs for the Source
        return jsonify({
            "totalCount": total_count,
            "data": [dict(row) for row in rows]
        })
