#!/bin/bash
set -e

echo "🚀 Deploying AITHORIX Trading System"

# Load environment
source .env.production

# Pull latest code
git pull origin main

# Build Docker images
docker-compose -f docker-compose.yml -f docker-compose.prod.yml build

# Run database migrations
docker-compose run --rm trading alembic upgrade head

# Deploy services
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d

# Health check
sleep 30
curl -f http://localhost:8000/health || exit 1

echo "✅ Deployment complete!"
