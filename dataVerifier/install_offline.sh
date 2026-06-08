#!/bin/bash
# Install Python dependencies from bundled packages (no internet required)
# Usage: bash install_offline.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Installing dependencies from local packages folder..."

python3 -m pip install --no-index --find-links="$SCRIPT_DIR/packages" -r "$SCRIPT_DIR/requirements.txt"

if [ $? -eq 0 ]; then
    echo
    echo "Installation successful."
else
    echo
    echo "Installation failed. Make sure Python 3.6+ is available."
    echo "See README.md for setup instructions."
fi
