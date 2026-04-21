#!/bin/bash
# Ingest ~1M documents into a local OpenSearch single-node cluster
# with optimized index (Parquet + Lucene composite engine).
#
# Usage: bash scripts/ingest_1m.sh [index_name] [optimized: true|false] [sort: true|false] [doc_count] [refresh_interval]
#
# Examples:
#   bash scripts/ingest_1m.sh my_index                            # 10M docs, 30s refresh, optimized, no sort
#   bash scripts/ingest_1m.sh my_index true true                  # 10M docs, 30s refresh, optimized, sorted
#   bash scripts/ingest_1m.sh my_index false true                 # 10M docs, 30s refresh, NOT optimized, sorted
#   bash scripts/ingest_1m.sh my_index true true 1000000 10s      # 1M docs, 10s refresh, optimized, sorted
#   bash scripts/ingest_1m.sh my_index false false 500000 -1      # 500K docs, refresh disabled, NOT optimized, no sort

set -euo pipefail

INDEX="${1:-benchmark_test}"
OPTIMIZED="${2:-true}"
SORT_ENABLED="${3:-false}"
TOTAL_DOCS="${4:-10000000}"
REFRESH_INTERVAL="${5:-30s}"
HOST="http://localhost:9200"
BATCH_SIZE=5000
PARALLEL=16
TMPDIR_BASE=$(mktemp -d)

trap "rm -rf ${TMPDIR_BASE}" EXIT
echo "dateformat=\""`date +"%Y-%m-%dT%H:%M:%S"`"\""
echo "=== Ingesting ${TOTAL_DOCS} docs into '${INDEX}' (optimized=${OPTIMIZED}, sort=${SORT_ENABLED}, refresh_interval=${REFRESH_INTERVAL}) ==="

curl -s -X DELETE "${HOST}/${INDEX}" -o /dev/null 2>&1 || true

# Build sort block if enabled — sorts on timestamp (long/date field)
SORT_BLOCK=""
if [ "$SORT_ENABLED" = "true" ]; then
  SORT_BLOCK=',
      "sort.field": "timestamp",
      "sort.order": "asc"'
  echo "Index sort enabled on field: timestamp (asc)"
fi

# Build optimized block
OPTIMIZED_BLOCK=""
if [ "$OPTIMIZED" = "true" ]; then
  OPTIMIZED_BLOCK=',
      "optimized.enabled": true'
else
  OPTIMIZED_BLOCK=',
      "optimized.enabled": false'
fi

curl -s -X PUT "${HOST}/${INDEX}" -H 'Content-Type: application/json' -d "{
  \"settings\": {
    \"number_of_shards\": 1,
    \"number_of_replicas\": 0,
    \"refresh_interval\": \"${REFRESH_INTERVAL}\",
    \"index\": {
      \"translog.durability\": \"async\",
      \"translog.flush_threshold_size\": \"1gb\"${SORT_BLOCK}${OPTIMIZED_BLOCK}
    }
  },
  \"mappings\": {
    \"properties\": {
      \"id\":           { \"type\": \"long\" },
      \"timestamp\":    { \"type\": \"date\", \"format\": \"epoch_millis\" },
      \"user_id\":      { \"type\": \"keyword\" },
      \"score\":        { \"type\": \"float\" },
      \"status\":       { \"type\": \"keyword\" },
      \"message\":      { \"type\": \"text\" },
      \"category\":     { \"type\": \"keyword\" },
      \"value\":        { \"type\": \"long\" },
      \"active\":       { \"type\": \"boolean\" },
      \"tags\":         { \"type\": \"keyword\" }
    }
  }
}" -o /dev/null

echo "Index '${INDEX}' created (optimized=${OPTIMIZED})."

STATUSES=("active" "pending" "completed" "failed" "archived")
CATEGORIES=("electronics" "clothing" "food" "sports" "books" "home" "auto" "health")
TAGS=("urgent" "normal" "low" "critical" "review" "new" "updated" "flagged")

send_batch() {
  local start=$1
  local end=$(( start + BATCH_SIZE ))
  if [ $end -gt $TOTAL_DOCS ]; then end=$TOTAL_DOCS; fi

  local tmpfile="${TMPDIR_BASE}/batch_${start}.ndjson"

  for (( i=start; i<end; i++ )); do
    local ts=$(( 1700000000000 + i * 1000 ))
    local uid="user_$(( i % 10000 ))"
    local score=$(( (i * 7 + 13) % 1000 ))
    local status="${STATUSES[$(( i % 5 ))]}"
    local cat="${CATEGORIES[$(( i % 8 ))]}"
    local val=$(( i * 3 + 17 ))
    local tag="${TAGS[$(( i % 8 ))]}"
    local active_str="false"
    if (( i % 2 == 0 )); then active_str="true"; fi

    echo '{"index":{}}'
    echo "{\"id\":${i},\"timestamp\":${ts},\"user_id\":\"${uid}\",\"score\":${score}.$(( i % 100 )),\"status\":\"${status}\",\"message\":\"Log entry number ${i} for testing bulk ingestion\",\"category\":\"${cat}\",\"value\":${val},\"active\":${active_str},\"tags\":[\"${tag}\"]}"
  done > "$tmpfile"

  local http_code
  http_code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${HOST}/${INDEX}/_bulk" \
    -H 'Content-Type: application/x-ndjson' --data-binary "@${tmpfile}")

  if [ "$http_code" != "200" ]; then
    echo "WARN: bulk at ${start} returned HTTP ${http_code}"
  fi
  rm -f "$tmpfile"
}

TOTAL_BATCHES=$(( (TOTAL_DOCS + BATCH_SIZE - 1) / BATCH_SIZE ))
START_TIME=$(date +%s)

echo "Sending ${TOTAL_BATCHES} bulk requests (${BATCH_SIZE} docs each, ${PARALLEL} parallel)..."

for (( batch=0; batch<TOTAL_BATCHES; batch++ )); do
  start_id=$(( batch * BATCH_SIZE ))
  send_batch $start_id &

  if (( (batch + 1) % PARALLEL == 0 )); then
    wait
    DOCS_SO_FAR=$(( (batch + 1) * BATCH_SIZE ))
    if [ $DOCS_SO_FAR -gt $TOTAL_DOCS ]; then DOCS_SO_FAR=$TOTAL_DOCS; fi
    ELAPSED=$(( $(date +%s) - START_TIME ))
    RATE=0
    if [ $ELAPSED -gt 0 ]; then RATE=$(( DOCS_SO_FAR / ELAPSED )); fi
    printf "\r  Progress: %d/%d docs (~%d docs/sec)" $DOCS_SO_FAR $TOTAL_DOCS $RATE
  fi
done

wait
echo ""

END_TIME=$(date +%s)
DURATION=$(( END_TIME - START_TIME ))

echo "Calling _refresh API..."
REFRESH_START=$(date +%s%3N 2>/dev/null || python3 -c 'import time; print(int(time.time()*1000))')
REFRESH_RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${HOST}/${INDEX}/_refresh")
REFRESH_HTTP_CODE=$(echo "$REFRESH_RESPONSE" | tail -1)
REFRESH_BODY=$(echo "$REFRESH_RESPONSE" | sed '$d')
REFRESH_END=$(date +%s%3N 2>/dev/null || python3 -c 'import time; print(int(time.time()*1000))')
REFRESH_DURATION_MS=$(( REFRESH_END - REFRESH_START ))

echo "  Refresh HTTP status: ${REFRESH_HTTP_CODE}"
echo "  Refresh duration: ${REFRESH_DURATION_MS}ms"
echo "  Refresh response: ${REFRESH_BODY}"

if [ "$REFRESH_HTTP_CODE" != "200" ]; then
  echo "WARN: _refresh returned non-200 status"
fi

echo ""
echo "=== Done ==="
echo "  Documents sent: ${TOTAL_DOCS}"
echo "  Optimized index: ${OPTIMIZED}"
echo "  Sort enabled: ${SORT_ENABLED}"
if [ "$SORT_ENABLED" = "true" ]; then
  echo "  Sort field: timestamp (asc)"
fi
echo "  Time: ${DURATION}s"
if [ $DURATION -gt 0 ]; then
  echo "  Throughput: $(( TOTAL_DOCS / DURATION )) docs/sec"
fi
echo ""
echo "Verify: curl ${HOST}/${INDEX}/_count"
echo "Search: curl '${HOST}/${INDEX}/_search?size=5&pretty'"
echo "Settings: curl '${HOST}/${INDEX}/_settings?pretty'"
