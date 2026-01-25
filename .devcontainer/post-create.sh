#!/bin/bash

# Set PS1 in .bashrc
if ! grep -q "PS1='\\\$ '" ~/.bashrc; then
    echo "PS1='\$ '" >> ~/.bashrc
fi

# Also set it for current session
export PS1='$ '

# Install Claude CLI and opencode globally
echo "Installing Claude CLI..."
npm install -g @anthropic-ai/claude-code

echo "Installing opencode..."
npm install -g opencode

echo "Devcontainer setup complete!"
