#!/bin/bash

# Check if version argument is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <new_version>"
  echo "Example: $0 2.2.5"
  exit 1
fi

NEW_VERSION=$1
WORKSPACE_ROOT=$(pwd)

# Read current version from the main VERSION file
if [ ! -f "$WORKSPACE_ROOT/VERSION" ]; then
  echo "Error: VERSION file not found in $WORKSPACE_ROOT."
  exit 1
fi

CURRENT_VERSION=$(cat "$WORKSPACE_ROOT/VERSION" | tr -d '[:space:]')
echo "Current Global Version: $CURRENT_VERSION"
echo "Target New Version:   $NEW_VERSION"
echo "---------------------------------------------------"

# Helper function for safe replacement
replace_in_file() {
  local file="$1"
  local search="$2"
  local replace="$3"
  
  if [ -f "$file" ]; then
    echo "Updating $file..."
    # Use | as delimiter to avoid issues with / in tags
    # MacOS requires empty string argument for -i
    sed -i.bak "s|$search|$replace|g" "$file"
    rm "${file}.bak"
  else
    echo "Skipping $file (not found)"
  fi
}

# 1. Update Go Server VERSION file
echo "$NEW_VERSION" > "$WORKSPACE_ROOT/VERSION"
echo "Updated $WORKSPACE_ROOT/VERSION"

# 2. Update C# Binding VERSION file
echo "$NEW_VERSION" > "$WORKSPACE_ROOT/bindings/csharp/VERSION"
echo "Updated $WORKSPACE_ROOT/bindings/csharp/VERSION"

# 3. Update Python (pyproject.toml)
# Pattern: version = "X.Y.Z"
replace_in_file "$WORKSPACE_ROOT/bindings/python/pyproject.toml" "version = \"$CURRENT_VERSION\"" "version = \"$NEW_VERSION\""

# 4. Update Rust (Cargo.toml)
# Pattern: version = "X.Y.Z"
replace_in_file "$WORKSPACE_ROOT/bindings/rust/Cargo.toml" "version = \"$CURRENT_VERSION\"" "version = \"$NEW_VERSION\""

# 5. Update Java (pom.xml)
# Pattern: <version>X.Y.Z</version>
replace_in_file "$WORKSPACE_ROOT/bindings/java/pom.xml" "<version>$CURRENT_VERSION</version>" "<version>$NEW_VERSION</version>"

# 6. Update C# Projects (.csproj)
# Pattern: <Version>X.Y.Z</Version>
# Using find to get all csproj files in bindings/csharp
find "$WORKSPACE_ROOT/bindings/csharp" -name "*.csproj" | while read -r csproj_file; do
  replace_in_file "$csproj_file" "<Version>$CURRENT_VERSION</Version>" "<Version>$NEW_VERSION</Version>"
done

echo "---------------------------------------------------"
echo "Version update complete!"

# Sample cmd line: ./update_version.sh 2.2.5
