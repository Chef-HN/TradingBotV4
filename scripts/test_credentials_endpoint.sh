#!/bin/bash
# Test credentials API endpoints
#
# Usage:
#   bash scripts/test_credentials_endpoint.sh

API_URL="http://localhost:8092/api"
BEARER_TOKEN="test_token"  # Placeholder - in production, get real token

echo "=========================================="
echo "Testing Credentials API Endpoints"
echo "=========================================="

echo ""
echo "[1] POST /api/credentials - Save Bybit credentials"
curl -X POST "$API_URL/credentials" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $BEARER_TOKEN" \
  -d '{
    "exchange_name": "bybit",
    "api_key": "eOmOtjnyNSYr0eIhZY",
    "api_secret": "me2BuYO2ZnVV0YhY2F2k2IyzC91XlyVSsZgp",
    "created_by": "test_user"
  }' \
  -w "\nStatus: %{http_code}\n\n" | python -m json.tool

echo "[2] GET /api/credentials/bybit - Check status"
curl -X GET "$API_URL/credentials/bybit" \
  -H "Authorization: Bearer $BEARER_TOKEN" \
  -w "\nStatus: %{http_code}\n\n" | python -m json.tool

echo "[3] POST /api/credentials - Update Coinbase credentials"
curl -X POST "$API_URL/credentials" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $BEARER_TOKEN" \
  -d '{
    "exchange_name": "coinbase",
    "api_key": "organizations/013da8ef-201c-4c5c-89c1-8e392efed60d/apiKeys/xyz",
    "api_secret": "-----BEGIN EC PRIVATE KEY-----...",
    "api_passphrase": "my_passphrase",
    "created_by": "test_user"
  }' \
  -w "\nStatus: %{http_code}\n\n" | python -m json.tool

echo "[4] GET /api/credentials/coinbase - Check status"
curl -X GET "$API_URL/credentials/coinbase" \
  -H "Authorization: Bearer $BEARER_TOKEN" \
  -w "\nStatus: %{http_code}\n\n" | python -m json.tool

echo "[5] DELETE /api/credentials/bybit - Deactivate"
curl -X DELETE "$API_URL/credentials/bybit" \
  -H "Authorization: Bearer $BEARER_TOKEN" \
  -w "\nStatus: %{http_code}\n\n" | python -m json.tool

echo "[6] GET /api/credentials/bybit - Verify deactivation"
curl -X GET "$API_URL/credentials/bybit" \
  -H "Authorization: Bearer $BEARER_TOKEN" \
  -w "\nStatus: %{http_code}\n\n" | python -m json.tool

echo "=========================================="
echo "Tests complete"
echo "=========================================="
