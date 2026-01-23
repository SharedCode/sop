#!/bin/bash
set -e

APP_NAME="SOP Data Manager"
INSTALL_DIR="$HOME/.sop"
BIN_DIR="$INSTALL_DIR/bin"
EXECUTABLE_NAME="sop-httpserver"

# Detect OS and Architecture to potentially select binary if we bundle multiple?
# For now, we assume the user is running this script from the unzipped folder containing the correct binary named 'sop-httpserver'

if [ ! -f "sop-httpserver" ]; then
    echo "Error: 'sop-httpserver' binary not found in current directory."
    exit 1
fi

echo "Installing $APP_NAME to $INSTALL_DIR..."

# Create Directories
mkdir -p "$BIN_DIR"

# Copy Binary
echo "Copying binary..."
cp "sop-httpserver" "$BIN_DIR/$EXECUTABLE_NAME"
# Remove Quarantine Attribute on macOS to allow execution
if [ "$(uname)" == "Darwin" ]; then
    xattr -d com.apple.quarantine "$BIN_DIR/$EXECUTABLE_NAME" 2>/dev/null || true
fi
chmod +x "$BIN_DIR/$EXECUTABLE_NAME"

# Create Initial Config if not exists
CONFIG_FILE="$INSTALL_DIR/config.json"
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Initializing default configuration..."
    echo '{}' > "$CONFIG_FILE"
fi

# Setup Path in Shell Profile
SHELL_PROFILE=""
if [ -n "$ZSH_VERSION" ]; then
    SHELL_PROFILE="$HOME/.zshrc"
elif [ -n "$BASH_VERSION" ]; then
    SHELL_PROFILE="$HOME/.bashrc"
    if [ "$(uname)" == "Darwin" ]; then
        SHELL_PROFILE="$HOME/.bash_profile"
    fi
fi

if [ -n "$SHELL_PROFILE" ]; then
    if ! grep -q "$BIN_DIR" "$SHELL_PROFILE"; then
        echo "Adding $BIN_DIR to $SHELL_PROFILE PATH..."
        echo "" >> "$SHELL_PROFILE"
        echo "# SOP AI Copilot Tools" >> "$SHELL_PROFILE"
        echo "export PATH=\"\$PATH:$BIN_DIR\"" >> "$SHELL_PROFILE"
        echo "Added to PATH. Please reload your shell or run 'source $SHELL_PROFILE'"
    else
        echo "PATH already configured in $SHELL_PROFILE"
    fi
fi

# Create Launcher Wrapper (Optional, to enforce CWD)
# We actually renamed the binary to 'sop-ai', but the app looks for config.json in CWD.
# So we need a wrapper script that changes directory to $INSTALL_DIR before running the actual binary.
mv "$BIN_DIR/$EXECUTABLE_NAME" "$BIN_DIR/${EXECUTABLE_NAME}-bin"

cat > "$BIN_DIR/$EXECUTABLE_NAME" <<EOF
#!/bin/bash
cd "$INSTALL_DIR"
exec "$BIN_DIR/${EXECUTABLE_NAME}-bin" "\$@"
EOF
chmod +x "$BIN_DIR/$EXECUTABLE_NAME"

echo "Installation Complete!"
echo "Run '$EXECUTABLE_NAME' to start the Data Manager."

# Interactive Start
read -p "Do you want to start SOP AI Copilot now? [Y/n] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Nn]$ ]]; then
    echo "Starting Server..."
    "$BIN_DIR/$EXECUTABLE_NAME" &
    echo "Server running in background."
fi
