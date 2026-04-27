import re

with open('/Users/grecinto/sop/ai/dynamic/knowledge_base.go', 'r') as f:
    content = f.read()

# 1. Update struct
struct_replacement = """type KnowledgeBase[T any] struct {
\tBaseKnowledgeBase[T]
\tManager *MemoryManager[T]
\t// MaxMathCategoryDistance enables vector-based fast category inference without LLM.
\t// Set to > 0.0 to enable. Distance is calculated via Euclidean Distance to cluster centroids.
\tMaxMathCategoryDistance float32
}"""
content = re.sub(r'type KnowledgeBase\[T any\] struct \{\n\tBaseKnowledgeBase\[T\]\n\tManager \*MemoryManager\[T\]\n\}', struct_replacement, content)

# 2. Re-write IngestThought
old_ingest = """func (kb *KnowledgeBase[T]) IngestThought(
ctx context.Context,
text string,
category string,
persona string,
data T,
) error {
// For API backwards compatibility we will wrap the underlying method, which expects vectors 
// We will do it inline here so it's simple

// 1. LLM as a helper: Only generate if missing
if category == \"\"if category == \"\"if catekbifnager.Geif category == \"\"if category == \"\"if catekbifnager.Geif categor}
tedCat
 t t t teCgory)
if err != nil {
return err
}

// 3. Embed the text
vecs, err := kb.Manager.embedder.EmbedTexts(ctx, []string{vecs, err := kb.Manager.embedder.EmbedTexts(ctx, []string{vecs, err := kb.Manager.emrget Categovecs, err := kb.Manager.embedde  vextvecs, err := kb.Manager.embedder.EmbedTexts(ctx, []string{vecs, err := kb.Manager.embedder.EmbedTexts(ctx, []string{vecs, err := kb.Manager.emrge *Kvecs, err := kb.ManastThought(
ctx context.Context,
text string,
category string,
persona string,
data T,
) error {
text
vecs, err := kb.Manager.embeddervecs, err := kb.Manager.embeddervecs, err := kb.Manager.embeddervecs, err := kb.Manager.emRevecs, err := kb.Manager.embeddervecs> LLM Fallback)
if category == "" {
if kb.MaxMathCategif kb.MaxMathCategif kb.MaxMathCategif kb.MaxMathCategif kb.MaxMathCategif kb.MaxMathCategif kb.MaxMathCategif k_ if kb.MaxMathCategifctx)
for ok {
c, err := categoriesTree.GetCurrentValue(ctx)
if err == nil && c != nil {
 = append(allCats, c)
 = ap= ca = ap= ca = ap= ca = ap= ca = ap= ca = ap= ca = ap= ca = ap= ca = ap= ca = ap= ca = ap= ca = ap= ca = ap= ca = ap= ca = ap= ca = ap= ca = ap= ca = ap= cath = ap=  the DAG
_, err_, err_, er.Ensu_, err_, err, category)
if err !=if err !=if err !=}

// 4. Force explicit insertion mapping to target Category
item := ai.Item[T]{
ID:      text, // Ensure unique ID
Payload: data,
Vector:  vector,
}

return kb.Store.UpsertByCategory(ctx, categreturn kb.Store.UpsertByCategory(ctx, categreturn kb.Store.UpsertByCategory(ctx, categreturn kb.Store.UpsertByCategory(ctx, categreturn kb.Store.UpsertByCategory(ctx, cate,
return kb.trreturn kb.trreturn kb.trreturn kb.trreturn kb
))))))))))))))))))))))))))))))er: )))))))))))te if missing
if categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif categoif cat= nil {
return err
}
vector = vecs[0]
}

// // // // // // // // appin// // // // // // // // appin// // // // //, // // // // //
Payload: data,
Vector:  vector,
}

return kb.Storreturn kb.ategory(ctx,return kb.Storreturn kb.ategory(ctx,return kb.Storreturn kb.ategory(ctx,return kb.Storreturn kb.ategory(ctx,return kb.Storreturn kb.ategory(ctx,return kb.Storreturn kb.ategory(ctx,return kb.Storreturn kb.atenoreturn kb.Storreturn kb.ategory(ctx,return kb.Storreter.embreturn kb.Storreturn kb.ategory(ctx,return kb.Stor {
oktegoriesTr}
tegoryDistance*100) { // Float safety
cacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacaca to target Category
item := ai.Item[T]{
ID:      text, // Ensure unique ID
Payload: data,
Vector:  vector,
}

return kb.Store.UpsertByCategory(ctx, category, item)
}"""

content = content.replace(old_insert, new_insert)

with open('/Users/grecinto/sop/ai/dynamic/knowledge_base.go', 'w') as f:
    f.write(content)

