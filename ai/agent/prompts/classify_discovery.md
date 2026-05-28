Classify the user's intent into a Context Outline.

Respond with JSON only in this schema:
{
  "entity": "Omni",
  "domain": "Stores",
  "db_artifacts": ["users_table"],
  "stores_artifacts": ["users_table"],
  "spaces_artifacts": [],
  "layers": [
    {"name": "Single-Domain", "crud": ["C", "R"]}
  ]
}

Context Outline Options:
- Entities: "Omni" (general assistance)
- Domains: 
  - "Stores": Programmatic access to databases, building AST scripts, querying JSON data, mutating records in a Store.
    Available Store Artifacts: [%s]
  - "Spaces": Searching knowledge bases, platform documentation, answering business model questions, OR explicitly managing data within a "Space". If the user explicitly mentions "Space" or "Spaces" (e.g. "my Tasks space"), route to Spaces.
    Available SpaceArtifacts: [%s]
- Operational Layers:
  - "Single-Domain": Operations restricted to a single domain (either Stores or Spaces).
  - "Cross-Domain": Operations coordinating across multiple domains (mixing Stores and Spaces).
    *Disambiguation: If the words "store" or "space" are just part of normal content or a category name, keep the request Single-Domain. Use Cross-Domain only when the request operates across both Stores and Spaces.*

For Cross-Domain requests, populate `stores_artifacts` and `spaces_artifacts` separately. Use `db_artifacts` only for single-domain compatibility or when there is only one relevant domain-specific artifact list.

  Select only the necessary components for the user's request. Tag Layers with the CRUD actions (C, R, U, D) requested by the user. Leave arrays empty if none apply.
