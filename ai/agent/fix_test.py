import sys

with open('copilottools_select_test.go', 'r') as f:
    text = f.read()

text = text.replace('b3, err := sopdb.NewBtree[any, any](ctx, dbOpts, "employees", tx, nil)',
'''storeOpts := sop.StoreOptions{
\t\tIsPrimitiveKey: true,
\t}
\tb3, err := sopdb.NewBtree[string, any](ctx, dbOpts, "employees", tx, nil, storeOpts)''')

with open('copilottools_select_test.go', 'w') as f:
    f.write(text)
