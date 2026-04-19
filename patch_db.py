import os

db_path = "ai/database/database.go"
with open(db_path, "r") as f:
    content = f.read()

if '"strings"' not in content:
    content = content.replace("import (", "import (\n\t\"strings\"", 1)

new_code = """
// GetStores returns the standard stores, excluding system and vector databases.
func (db *Database) GetStores(ctx context.Context) ([]string, error) {
	trans, err := db.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return nil, err
	}
	defer trans.Rollback(ctx)

	stries, err := trans.GetStores(ctx)
	if err != nil {
		return nil, err
	}

	var stores []string
	for _, store := range stries {
		if !strings.HasSuffix(store, "_vecs") && !strings.Contains(store, "/") {
			stores = append(stores, store)
		}
	}
	return stores, nil
}

// GetPlaybooks returns only the vector databases (playbooks).
func (db *Database) GetPlaybooks(ctx context.Context) ([]string, error) {
	trans, err := db.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return nil, err
	}
	defer trans.Rollback(ctx)

	stries, err := trans.GetStores(ctx)
	if err != nil {
		return nil, err
	}

	var playbooks []string
	for _, store := range stries {
		if strings.HasSuffix(store, "_vecs") {
			playbooks = append(playbooks, strings.TrimSuffix(store, "_vecs"))
		}
	}
	return playbooks, nil
}
"""

if "GetStores(ctx context.Context) ([]string, error)" not in content:
    with open(db_path, "w") as f:
        f.write(content + "\n" + new_code)
    print("Patched " + db_path)
else:
    print("Already patched")
