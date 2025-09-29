#!/bin/bash

set -e

echo "🧱 Installing system dependencies..."
sudo apt update
sudo apt install -y \
    python3-gi \
    gir1.2-gexiv2-0.10 \
    libgexiv2-dev \
    mediainfo \
    python3-dev \
    python3-pip \
    python3-venv

echo "🐍 Creating Python virtual environment with system packages..."
python3 -m venv venv --system-site-packages

echo "🔁 Activating virtual environment..."
source venv/bin/activate

echo "📦 Installing Python dependencies..."
pip install --upgrade pip
pip install pymediainfo tqdm

echo "🔍 Verifying GExiv2 availability in virtual environment..."
if python3 -c "from gi.repository import GExiv2" &> /dev/null; then
    echo "✅ GExiv2 module is available."
else
    echo "❌ GExiv2 NOT available in venv. Try: sudo apt install python3-gi gir1.2-gexiv2-0.10"
    exit 1
fi

echo "✅ Environment setup complete"

