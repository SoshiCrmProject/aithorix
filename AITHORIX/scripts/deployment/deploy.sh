#!/bin/bash
set -e
echo "Deploying AITHORIX Trading System..."
ansible-playbook deployment/ansible/deploy.yml
echo "Deployment complete!"
