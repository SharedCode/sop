def patch():
    with open("ai/database/database.go", "r") as f:
        text = f.read()
    if '"strings"' not in text:
        text = text.replace('import (\n', 'import (\n\t"strings"\n', 1)
    if 'GetStores()' not in text:
        text += """
// GetStores returns the standard stores, excluding system and vector databases.
func (db *Database) GetStores() ([]string, error) {
var stores []string
stries, err := db.engine.GetStores()
if err != nil {
 nil, err
}
for _, store := range stries {
gs.HasSuffix(store, "_vecs") && !strings.Contains(store, "/") {
d(stores, store)
 stores, nil
}

// GetPlaybooks returns only the vector databases (playbooks).
func (db *Database) GetPlaybooks() ([]string, error) {
var playbooks []string
stries, err := db.engine.GetStores()
if err != nil {
 nil, err
}
for _, store := range stries {
gs.HasSuffix(store, "_vecs") {
d(playbooks, strings.TrimSuffix(storeplaybooks = append(play paploks, nil
}
"""
        with open("ai/database/database.go",         with open("ai  f        with open("ai/databasetched ai/database/database.go")
patch()
