#!/bin/bash
set -e

# Grafana Dashboard Backup Script
# This script exports all Grafana dashboards to JSON files

GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"
GRAFANA_USER="${GRAFANA_USER:-admin}"
GRAFANA_PASSWORD="${GRAFANA_PASSWORD:-admin}"
OUTPUT_DIR="${OUTPUT_DIR:-./docker/grafana/provisioning/dashboards}"

echo "🔍 Fetching dashboards from Grafana at ${GRAFANA_URL}..."

# Get all dashboard UIDs
DASHBOARDS=$(curl -s -u "${GRAFANA_USER}:${GRAFANA_PASSWORD}" \
  "${GRAFANA_URL}/api/search?type=dash-db" | \
  jq -r '.[] | .uid')

if [ -z "$DASHBOARDS" ]; then
  echo "⚠️  No dashboards found or Grafana is not accessible"
  exit 1
fi

echo "📊 Found $(echo "$DASHBOARDS" | wc -l) dashboard(s)"

# Create output directory if it doesn't exist
mkdir -p "${OUTPUT_DIR}"

# Export each dashboard
for uid in $DASHBOARDS; do
  echo "📥 Exporting dashboard: ${uid}"

  # Fetch the dashboard
  DASHBOARD=$(curl -s -u "${GRAFANA_USER}:${GRAFANA_PASSWORD}" \
    "${GRAFANA_URL}/api/dashboards/uid/${uid}")

  # Get the title for filename
  TITLE=$(echo "$DASHBOARD" | jq -r '.meta.slug // .dashboard.title' | \
    tr '[:upper:]' '[:lower:]' | sed 's/ /-/g' | sed 's/[^a-z0-9-]//g')

  # Extract just the dashboard model (remove meta wrapper for provisioning)
  echo "$DASHBOARD" | jq '.dashboard' > "${OUTPUT_DIR}/${TITLE}.json"

  echo "  ✅ Saved to ${OUTPUT_DIR}/${TITLE}.json"
done

echo ""
echo "✨ Backup complete! Dashboards saved to ${OUTPUT_DIR}"
echo "💡 Tip: Commit these files to git to version control your dashboards"
