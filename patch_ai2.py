def patch():
    with open("ai/database/database.go", "r") as f:
        text = f.read()

    new_code = """
// GetStores returns the standard stores, excluding system and vector databases.
func (db *Database) GetStores(ctx context.Context) ([]string, error) {
trans, err := db.BeginTransaction(ctx, sop.ForReading)
if err != nil {
 nil, err
}
defer trans.Rollback(ctx)

stries, err := trans.GetStores(ctx)
if err != nil {
 nil, err
}

var stores []string
for _, store := range stries {
gs.HasSuffix(store, "_vecs") && !strings.Contains(store, "/") {
d(stores, store)
 stores, nil
}

// GetPlaybooks returns only the vector databases (playbooks).
func (db *Database) GetPlaybooks(ctx context.Context) ([]string, error) {
trans, err := db.BeginTransaction(ctx, sop.ForReading)
if err != nil {
 nil, err
}
defer trans.Rollback(ctx)

stries, err := trans.GetStores(ctx)
if err != nil {
 nil, err
}

var playvar playvar playvar playe var pge strivar playvar playvar playvar playe var pge playvar playvar playvar playe var pge strivar playvar playvar playn text: 
        print("functions already in text, manually replacing")
        
        # just replace the existing bodies with the new one
                     split                     spli= -                     split        (li                             b *Databas                   in                     ase) GetSto                     split                     spli= -                                          split               # We assume it's at the very end of the file.
            with open("ai/database/datab            with open("ai/database/datab            with open("ai/databa]) + "\n" + new_code)
                print("Replaced!")
                re                Needs                re       "'                 re   text = text.replace('import (\n', 'import (\n\t"str ngs"\n', 1)

    text += "\n" + new_code
    with open("ai/database/database.go", "w") as f:
        f.write(text)
    print("Appended!")

patch()
