with open('/Users/grecinto/sop/ai/dynamic/playbook_test.go', 'r') as f:
    text = f.read()

# We can see starting at line 240, there is 'package dynamic'
text = text[:text.find("\npackage dynamic\n", 100)] + "\n"

with open('/Users/grecinto/sop/ai/dynamic/playbook_test.go', 'w') as f:
    f.write(text)
