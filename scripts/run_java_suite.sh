#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "--- Building Go Bridge ---"
cd bindings/main
# Detect OS
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    LIB_NAME="libjsondb.so"
elif [[ "$OSTYPE" == "darwin"* ]]; then
    LIB_NAME="libjsondb.dylib"
elif [[ "$OSTYPE" == "cygwin" ]] || [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]]; then
    LIB_NAME="libjsondb.dll"
else
    echo "Unknown OS: $OSTYPE"
    exit 1
fi

go build -buildmode=c-shared -o ../../$LIB_NAME *.go
cd ../..

echo "--- Running Java Tests ---"
cd bindings/java
# Clean up previous test data
rm -rf test_btree*
mvn test -DargLine="-Djna.library.path=../../"

echo "--- Running Java Examples ---"
EXAMPLES=(
    "BTreeBasic"
    "BTreeBatched"
    "BTreePaging"
    "BTreeComplexKey"
    "BTreeMetadata"
    "ConcurrentTransactionsDemoStandalone"
    "ConcurrentTransactionsDemoClustered"
    "CassandraDemo"
    "LoggingDemo"
)

for example in "${EXAMPLES[@]}"; do
    echo "Running $example..."
    # Clean up data folders before each run to ensure clean state
    rm -rf sop_data*
    mvn exec:java -Dexec.mainClass="com.sharedcode.sop.examples.$example" -Djna.library.path=../../ -Dorg.slf4j.simpleLogger.defaultLogLevel=error
done

echo "--- Java Suite Complete ---"
