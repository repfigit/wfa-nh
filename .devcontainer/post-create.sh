#!/bin/bash

# Set PS1 in .bashrc
if ! grep -q "PS1='\\\$ '" ~/.bashrc; then
    echo "PS1='\\\$ '" >> ~/.bashrc
fi

# Also set it for current session
export PS1='$ '

# Install any additional global packages if needed
# npm install -g some-package

echo "Devcontainer setup complete!"
