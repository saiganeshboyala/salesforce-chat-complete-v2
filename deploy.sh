#!/bin/bash
# ── AWS EC2 Deployment Script ──────────────────────────
# Run this ON the EC2 instance after cloning the repo

set -e

echo "=== Salesforce Data Chat — Production Setup ==="

# 1. System dependencies
sudo apt update && sudo apt install -y python3.11 python3.11-venv python3-pip nodejs npm nginx certbot python3-certbot-nginx

# 2. Clone repo (skip if already done)
# git clone <your-repo-url> /home/ubuntu/salesforce-chat
cd /home/ubuntu/salesforce-chat

# 3. Backend setup
cd backend
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install gunicorn

# 4. Create .env (copy your local .env and update)
# cp .env.example .env
# nano .env  # edit with production values

# 5. Frontend build
cd ../frontend
npm install
npm run build
cd ..

# 6. Create systemd service
sudo tee /etc/systemd/system/sfchat.service > /dev/null <<'EOF'
[Unit]
Description=Salesforce Data Chat
After=network.target

[Service]
Type=exec
User=ubuntu
WorkingDirectory=/home/ubuntu/salesforce-chat/backend
Environment=PATH=/home/ubuntu/salesforce-chat/backend/.venv/bin:/usr/bin
ExecStart=/home/ubuntu/salesforce-chat/backend/.venv/bin/gunicorn app.main:app \
    --worker-class uvicorn.workers.UvicornWorker \
    --workers 2 \
    --bind 127.0.0.1:8000 \
    --timeout 120 \
    --access-logfile /var/log/sfchat/access.log \
    --error-logfile /var/log/sfchat/error.log
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

# 7. Create log directory
sudo mkdir -p /var/log/sfchat
sudo chown ubuntu:ubuntu /var/log/sfchat

# 8. Nginx reverse proxy
sudo tee /etc/nginx/sites-available/sfchat > /dev/null <<'NGINX'
server {
    listen 80;
    server_name chat.fyxo.ai;

    client_max_body_size 20M;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # SSE streaming support
        proxy_buffering off;
        proxy_cache off;
        proxy_read_timeout 300s;
        proxy_connect_timeout 75s;
    }
}
NGINX

sudo ln -sf /etc/nginx/sites-available/sfchat /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t && sudo systemctl reload nginx

# 9. Start the service
sudo systemctl daemon-reload
sudo systemctl enable sfchat
sudo systemctl start sfchat

echo ""
echo "=== Deployment complete ==="
echo ""
echo "Next steps:"
echo "  1. Add DNS A record: chat.fyxo.ai → $(curl -s ifconfig.me)"
echo "  2. Run: sudo certbot --nginx -d chat.fyxo.ai"
echo "  3. App will be live at: https://chat.fyxo.ai"
