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
# Pattern: version = "X.Y.Z" -> Use Regex to be robust against version mismatch
if [ -f "$WORKSPACE_ROOT/bindings/python/pyproject.toml" ]; then
  echo "Updating pyproject.toml..."
  sed -i.bak "s|^version = \".*\"|version = \"$NEW_VERSION\"|g" "$WORKSPACE_ROOT/bindings/python/pyproject.toml"
  rm "$WORKSPACE_ROOT/bindings/python/pyproject.toml.bak"
fi

# 3b. Update Python (__init__.py)
# Pattern: __version__="X.Y.Z"
if [ -f "$WORKSPACE_ROOT/bindings/python/sop/__init__.py" ]; then
  echo "Updating python/sop/__init__.py..."
  sed -i.bak "s|^__version__=\".*\"|__version__=\"$NEW_VERSION\"|g" "$WORKSPACE_ROOT/bindings/python/sop/__init__.py"
  rm "$WORKSPACE_ROOT/bindings/python/sop/__init__.py.bak"
fi

# 4. Update Rust (Cargo.toml)
# Pattern: version = "X.Y.Z"
if [ -f "$WORKSPACE_ROOT/bindings/rust/Cargo.toml" ]; then
  echo "Updating Rust Cargo.toml..."
  # Only replace the package version (starts with version =), not dependencies
  sed -i.bak "s|^version = \".*\"|version = \"$NEW_VERSION\"|g" "$WORKSPACE_ROOT/bindings/rust/Cargo.toml"
  rm "$WORKSPACE_ROOT/bindings/rust/Cargo.toml.bak"
fi

# 5. Update Java (pom.xml)
# Pattern: <version>X.Y.Z</version> - Extract local version first safely
JAVA_POM="$WORKSPACE_ROOT/bindings/java/pom.xml"
if [ -f "$JAVA_POM" ]; then
  # Extract the first occurrence of <version>...</version> which is the project version
  JAVA_CURRENT=$(grep -m 1 "<version>" "$JAVA_POM" | sed -E 's/.*<version>(.*)<\/version>.*/\1/')
  if [ ! -z "$JAVA_CURRENT" ]; then
     echo "Updating Java pom.xml (Found version $JAVA_CURRENT)..."
     replace_in_file "$JAVA_POM" "<version>$JAVA_CURRENT</version>" "<version>$NEW_VERSION</version>"
  fi
fi

# 6. Update C# Projects (.csproj)
# Pattern: <Version>X.Y.Z</Version>
find "$WORKSPACE_ROOT/bindings/csharp" -name "*.csproj" | while read -r csproj_file; do
  # Extract version from this specific file to ensure match
  CS_CURRENT=$(grep -m 1 "<Version>" "$csproj_file" | sed -E 's/.*<Version>(.*)<\/Version>.*/\1/')
  if [ ! -z "$CS_CURRENT" ]; then
     echo "Updating $csproj_file (Found version $CS_CURRENT)..."
     replace_in_file "$csproj_file" "<Version>$CS_CURRENT</Version>" "<Version>$NEW_VERSION</Version>"
  fi
done

echo "---------------------------------------------------"
echo "Version update complete!"

# Sample cmd line: ./update_version.sh 2.2.5
