with open("manager.go", "r") as f:
    content = f.read()

import re

# Precise replacement using regex to avoid white space issues
pattern = r"// 1\. Scan vectors stored under.*\n\s*var vectorsToMove \[\]Vector\n\s*searchKey := VectorKey\{CategoryID: anchor\.ID\}\n\n\s*ok, err := vectorsTree\.Find\(ctx, searchKey, true\)\n\s*for ok && err == nil \{\n\s*vk := vectorsTree\.GetCurrentKey\(\)\n\s*if vk\.Key\.CategoryID != anchor\.ID \{\n\s*break\n\s*\}\n\s*v, valErr := vectorsTree\.GetCurrentValue\(ctx\)\n\s*if valErr == nil \{\n\s*vectorsToMove = append\(vectorsToMove, v\)\n\s*\}\n\s*ok, err = vectorsTree\.Next\(ctx\)\n\s*\}"

replacement = """// 1. Scan vectors stored under anchor.ID
var vectorsToMove []Vector

ok, err := vectorsTree.First(ctx)
for ok && err == nil {
vk := vectorsTree.GetCurrentKey()
if vk.Key.CategoryID.Compare(anchor.ID) == 0 {
v, valErr := vectorsTree.GetCurrentValue(ctx)
if valErr ==if valErr ==if valErr ==if valErr ==if valErr ==if valErr ==if valErr ==)
if valErr ==if valErr  contenif valErr === re.subif vn, reflect_test.go if valErr ==if valErr  contenif valErr === re.sub        f.write(content)
else:
    print("Pattern not found!")
