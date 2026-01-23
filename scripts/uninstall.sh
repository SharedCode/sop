#!/bin/bash

INSTALL_DIR="$HOME/.sop"

echo "Uninstalling SOP Data Manager..."

if [ -d "$INSTALL_DIR" ]; then
    echo "Removing installation directory: $INSTALL_DIR"
    rm -rf "$INSTALL_DIR"
else
    echo "SOP Data Manager is not installed in default location ($INSTALL_DIR)."
fi

# Remove from Shell Profile
SHELL_PROFILE=""
if [ -n "$ZSH_VERSION" ]; then
    SHELL_PROFILE="$HOME/.zshrc"
elif [ -n "$BASH_VERSION" ]; then
    SHELL_PROFILE="$HOME/.bashrc"
    if [ "$(uname)" == "Darwin" ]; then
        SHELL_PROFILE="$HOME/.bash_profile"
    fi
fi

if [ -n "$SHELL_PROFILE" ] && [ -f "$SHELL_PROFILE" ]; then
    # Create backup
    cp "$SHELL_PROFILE" "${SHELL_PROFILE}.bak"
    
    # Remove lines containing .sop/bin
    grep -v ".sop/bin" "${SHELL_PROFILE}.bak" > "$SHELL_PROFILE"
    
    echo "Removed PATH entry from $SHELL_PROFILE (Backup saved as ${SHELL_PROFILE}.bak)"
else
    echo "Could not find shell profile to clean up."
fi

echo "Uninstallation Complete."
