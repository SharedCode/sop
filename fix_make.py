import os
import re

def process_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()

    # We replace "_vecs" with VectorStoreSuffix in template code
    if "ai/vector/store.go" in filepath:
        content = re.sub(r'\"/vecs\"', 'database.VectorStoreSuffix', content)

    with open(filepath, 'w') as f:
        f.write(content)

process_file('ai/vector/store.go')
