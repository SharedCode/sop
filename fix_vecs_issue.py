import os
import re

def process_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()

    # See if we can add constant
    if "const VectorStoreSuffix = " not in content:
        content = content.replace('var (\n\tErrNotFound = errors.ErrNotFound', 'const VectorStoreSuffix = "_vecs"\n\nvar (\n\tErrNotFound = errors.ErrNotFound')

        # Maybe the replace didn't work because of spaces or it's somewhere else.
        if "const VectorStoreSuffix = " not in content:
            # Let's just put it under the imports
            content = re.sub(r'import \((.*?)\)', r'import (\1)\n\nconst VectorStoreSuffix = "_vecs"\n\n', content, flags=re.DOTALL, count=1)
            
    with open(filepath, 'w') as f:
        f.write(content)

process_file('ai/database/database.go')
