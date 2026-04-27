with open('/Users/grecinto/sop/ai/dynamic/knowledge_base.go', 'r') as f:
    content = f.read()

# 1. Update struct
old_struct = """type KnowledgeBase[T any] struct {
\tBaseKnowledgeBase[T]
\tManager *MemoryManager[T]
}"""

new_struct = """type KnowledgeBase[T any] struct {
\tBaseKnowledgeBase[T]
\tManager *MemoryManager[T]
\t
\t// MaxMathCategoryDistance enables vector-based fast category inference without LLM.
\t// Set to > 0.0 to enable. Distance is calculated via Euclidean distance to cluster centroids.
\tMaxMathCategoryDistance float32
}"""

content = content.replace(old_struct, new_struct)

# 2. Re-write IngestThought
old_ingest = """func (kb *KnowledgeBase[T]) IngestThought(
\tctx context.Context,
\ttext string,
\tcategory string,
\tpersona string,
\tdata T,
) error {
\t// For API backwards compatibility we will wrap the underlying method, which expects vectors 
\t// We will do it inline here so it's simple

\t// 1. LLM as a helper: Only generate if missing
\tif category ==\tif category ==\tif catego :\tif category ==\tif category ==\tif catego :\tif category ==\tif cate\t\\tif category ==\tif t\tcategory = generate\tif category ==\tif category ==\tif catego :\tifhe\tif category ==\ti.Man\tif category ==\tif categategory)\tif category ==\tif category ==\tif catego :\tifbe\tif category ==\t err \tif category ==\tifer.EmbedTexts(ctx, []string{text})
\tif err != nil {
\t\treturn err
\t}

\t// 4. Force explicit in\t// 4. Force explicit in\t// 4. Force explicit in\t// 4. Force explicit in\t// 4. Force explID
\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\ vecs[0],
\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\ vecs[0],
plicit egoplicit egoplicit egoplicit =plicit egoplicit egoplicit egoplicit =plicit egoplicit egexplicit egoplicit egopinplicit egoory splicit egoplicia stplicit egoplic,
) error {
\t// 1. Embed the text
\tvecs, err := kb.Manager.embedder.EmbedTexts(ctx, []string{text})
\tif err != nil {
\tif err != nil {
Manager.embedder.EmbedTex/ 2.Manager.embedder.EmbedTex/ 2.Manager.embedderM FalManager.embedder.EmbedTex/ 2.Manager.embedder.EmbedTex/ 2.Manager.embedderM FalManager.embedder.EmbedTex/ 2.Manager.embedder.EmbedTex/ 2.Manager.embedderM FalManager.embedder.EmbedTex/ 2.Manager.embedder.EmbedTex/ 2.Manager.embedderM FalManager.embedder.EmbedTex/ 2.Manager.embedder.EmbedTex/ 2.Manager.embedderM FalManager.embedder.EmbedTex/ 2.Manager.embedder.EmbedTex/ 2.Manager.embedderM FalMokManager.emgoriesTrManager.embe
\t\t\t\\t\t\t\\t\t\t\\t\t\t\\t\t\t\\t\t\t\\t\t\t\\t\t\t\\t\t\t\\t\t\t\\t\t\t\\t\t\t\\t\t\t\\t\t\t\\t\t\t\\t\t\t\\t\t\t\\t\t\t\yD\t\t\t\\t\t\t\\t\t\t\\t\t\t\\t\t\t\\t\tName
\t\t\t\t}
\t\t\t}
\t\t}

\t\tif category == "" {
\t\t\tgeneratedCat, err := kb.Manager.GenerateCategory(ctx, text, persona)
\t\t\tif err != nil {
\t\t\t\treturn err
\t\t\t}
\t\t\tcategory = generatedCat
\t\t}
\t}

\t//\t//\t//\t the Category\t//\t//\t//\t the Category\t//\t//\t//\t the Category\t//\t//\t/go\t//\t//\t//\t tnil {
\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\ter\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\t\treturn err\tne\t\tretu
\t}

\t// 2. Ens\t// 2. Ens\t// 2. Enss \t// 2. Ens\t// 2. Ens\t// 2. Enss \t// 2. Ens\t// 2. Ens\t// 2. Enss \r !=\t// 2. Ens\eturn err
\t// 2. Ens\t// ed the text\t// 2. Ens\t// ed the text\ti\t//n(vecto\t// 2. Ens\t// ed the text\t// 2. Ens\t// ed the text\ti\t//n(vecto\t// 2. Ens\t// ed the text\t// 2. Ens\t// ed r
\t// 2. Ens\t// ed the text\t// 2. Ens\t// ed the text\ti\t//n(vecto\t// 2. Ens\t// ed the text\t// 2. Ens\t// ed the text\ti\t//n(vecto\t// 2. Ens\t// ed the text\t// 2. Ens\t// ed r
 vector,
\t}

\treturn kb.Store.UpsertByCategory(ctx, category, item)
}"""

new_insert = """func (kb *KnowledgeBase[T]) Insert(
\tctx context.Context,
\ttext string,
\tcategory string,
\tpersona string,
\tvector []float32,
\tdata T,
) error {
\t// 1. Embed the text if vector is not provided
\tif len(vector) == 0 {
\t\tvecs, err := kb.Manager.embedder.EmbedTexts(ctx, []string{text})
\t\tif err != nil {
\t\t\treturn err
\t\t}
\t\tvector = vecs[0]
\t}

\t// 2. Resolve Category (Math Model Fast-Path -> LLM Fallback)
\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tiil \tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif c\tif  categoriesTree.GetCurrentValue(ctx)
\t\t\t\t\tif err == nil && c != ni\t\t\t\t\tif err == nil && c != ni\t\t\t\t\tif err == nil && c != ni\t\t\t\t\tif err == nil && c != ni\t\t\t\t\tif err == nil && c != ni\t\t\t\t\tif err == nil && c != ni\t\t\t\t\tif c\t\t\t\!= nil \t\t\t\t\tif err == nil && c != ni\t\t\t\t\tif err == go\y = c\t\t\t\t\tif err == nil && c != ni\t\t\t\t\tiategory\t\t\t\t\tif err == nte\t\t\t\t\ti= kb\t\t\t\t\tif rateCategory(ctx, text, \t\t\t\t\tif err =err != nil {
\t\t\t\treturn err
\t\t\t}
\t\t\tcategory = generatedCat
\t\t}
\t}

\t// 3. Ensure\t// 3. Ensure\t// 3. Ensure\t// 3. Ensure\t// 3. Ensure\t// 3. Ensry\t// 3. Ensure\t//if\t// != nil {
\t\tretur\t\tretur\t\tretur\Force explicit insertion mapping to target Category
\titem := ai.Item[T]{
\t\tID:      text, // Ensure unique ID
\t\tPayload: data,
\t\tVector:  vector,
\t}

\treturn kb.Store.UpsertByCategory(ctx, category, item)
}"""

content = content.replace(old_insert, new_insert)

with open('/Users/grecinto/sop/ai/dynamic/knowledge_base.go', 'w') as f:
    f.write(content)

