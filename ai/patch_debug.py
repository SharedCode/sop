import sys

with open('agent/copilottools.utils.go', 'r') as f:
    text = f.read()

text = text.replace("result := make(map[string]any)", 
'''result := make(map[string]any)
\tfmt.Printf("DEBUG flattenItem: k=%#v (%T) v=%#v (%T)\\n", key, key, value, value)''')

with open('agent/copilottools.utils.go', 'w') as f:
    f.write(text)
