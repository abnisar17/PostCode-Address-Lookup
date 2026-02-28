#!/bin/bash
# Live merge progress monitor — shows which step is running and write throughput
DB_CMD="docker exec postcodeaddresslookup-db-1 psql -U postgres -d postcode_lookup -t -A"

echo "============================================"
echo "  MERGE PROGRESS MONITOR (Ctrl+C to stop)"
echo "============================================"
echo ""

while true; do
    # Get current step
    STEP=$($DB_CMD -c "SELECT left(query, 80) FROM pg_stat_activity WHERE pid = 1907 AND state = 'active';" 2>/dev/null | head -1)

    if [ -z "$STEP" ]; then
        # PID 1907 no longer active — merge step might have finished or merge is done
        ACTIVE=$($DB_CMD -c "SELECT left(query, 80) FROM pg_stat_activity WHERE datname = 'postcode_lookup' AND state = 'active' AND query NOT LIKE 'SELECT%' AND query NOT LIKE 'autovacuum%';" 2>/dev/null | head -1)
        if [ -z "$ACTIVE" ]; then
            echo "[$(date +%H:%M:%S)] MERGE COMPLETE — no active queries found"
            break
        else
            STEP="$ACTIVE"
        fi
    fi

    # Detect which step from the query text
    if echo "$STEP" | grep -q "postcode_id"; then
        LABEL="Step 1/8: Linking postcodes"
    elif echo "$STEP" | grep -q "u.latitude"; then
        LABEL="Step 2/8: UPRN geocoding"
    elif echo "$STEP" | grep -q "p.latitude"; then
        LABEL="Step 3/8: Postcode geocoding"
    elif echo "$STEP" | grep -q "lr:"; then
        LABEL="Step 4/8: Linking price paid (exact)"
    elif echo "$STEP" | grep -q "pp.paon"; then
        LABEL="Step 4/8: Linking price paid (text)"
    elif echo "$STEP" | grep -q "ch:"; then
        LABEL="Step 5/8: Linking companies (exact)"
    elif echo "$STEP" | grep -q "companies"; then
        LABEL="Step 5/8: Linking companies (text)"
    elif echo "$STEP" | grep -q "fsa:"; then
        LABEL="Step 6/8: Linking food ratings (exact)"
    elif echo "$STEP" | grep -q "food_ratings"; then
        LABEL="Step 6/8: Linking food ratings (text)"
    elif echo "$STEP" | grep -q "voa:"; then
        LABEL="Step 7/8: Linking VOA (exact)"
    elif echo "$STEP" | grep -q "voa_ratings"; then
        LABEL="Step 7/8: Linking VOA (text)"
    elif echo "$STEP" | grep -q "confidence"; then
        LABEL="Step 8/8: Scoring confidence"
    elif echo "$STEP" | grep -q "multi_source"; then
        LABEL="Step 8/8: Multi-source bonus"
    elif echo "$STEP" | grep -q "EXISTS"; then
        LABEL="Step 8/8: Enrichment bonus"
    else
        LABEL="Working..."
    fi

    # Get duration
    DURATION=$($DB_CMD -c "SELECT now() - query_start FROM pg_stat_activity WHERE datname = 'postcode_lookup' AND state = 'active' AND query NOT LIKE 'SELECT%' AND query NOT LIKE 'autovacuum%' LIMIT 1;" 2>/dev/null | head -1 | xargs)

    # Get WAL throughput (MB written in last interval)
    WAL1=$($DB_CMD -c "SELECT pg_current_wal_lsn();" 2>/dev/null | head -1)
    sleep 5
    WAL2=$($DB_CMD -c "SELECT pg_current_wal_lsn();" 2>/dev/null | head -1)

    if [ -n "$WAL1" ] && [ -n "$WAL2" ]; then
        BYTES=$($DB_CMD -c "SELECT pg_wal_lsn_diff('$WAL2', '$WAL1');" 2>/dev/null | head -1 | xargs)
        if [ -n "$BYTES" ] && [ "$BYTES" -gt 0 ] 2>/dev/null; then
            MB_PER_SEC=$(echo "scale=1; $BYTES / 5 / 1048576" | bc 2>/dev/null)
            THROUGHPUT="${MB_PER_SEC} MB/s writes"
        else
            THROUGHPUT="idle"
        fi
    else
        THROUGHPUT="?"
    fi

    # Get wait event
    WAIT=$($DB_CMD -c "SELECT COALESCE(wait_event_type || ':' || wait_event, 'CPU') FROM pg_stat_activity WHERE datname = 'postcode_lookup' AND state = 'active' AND query NOT LIKE 'SELECT%' AND query NOT LIKE 'autovacuum%' LIMIT 1;" 2>/dev/null | head -1 | xargs)

    echo -e "[$(date +%H:%M:%S)] $LABEL | Duration: $DURATION | $THROUGHPUT | $WAIT"

    sleep 5
done
