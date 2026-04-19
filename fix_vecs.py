import os
import re

def process_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()

    # We need to add the constant definition if it doesn't exist, maybe in database.go
    if "VectorStoreSuffix" not in content and "ai/database/database.go" in filepath:
        content = content.replace('var (\n\tErrNotFound = errors.ErrNotFound', 'const VectorStoreSuffix = "_vecs"\n\nvar (\n\tErrNotFound = errors.ErrNotFound')
    
    # We replace "_vecs" with VectorStoreSuffix in ai/database/database.go
    if "ai/database/database.go" in filepath:
        content = content.replace('"_vecs"', 'VectorStoreSuffix')
    
    # We replace "_vecs" with database.VectorStoreSuffix in main.go
    if "tools/httpserver/main.go" in filepath:
        content = content.replace('"_vecs"', 'database.VectorStoreSuffix')

    with open(filepath, 'w') as f:
        f.write(content)

process_file('ai/database/database.go')
process_file('tools/httpserver/main.go')
