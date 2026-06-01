Classify whether the user is continuing the current context or switching.

The user is potentially continuing a previous topic or switching to a new one.

CONTINUITY DIGEST:
%s

LAST TYPED ROUTING HINT:
%s

CURRENT EXPLICIT ANCHOR:
%s

USER'S LATEST QUERY:
%s

Determine whether the user's latest query expands the current state or switches to a new domain/topic.

Use the continuity digest as the primary signal surface. Use the typed routing hint as a secondary compatibility hint.
When an explicit anchor is present, use it as fresh evidence that may confirm, refine, or replace stale continuity.
The digest fields are generic signals, not domain-specific hard constraints.
When the active or candidate domain is Stores, parse likely store names from the latest query and match singular/plural variants against anchored or remembered store artifacts before dropping store selection.

If the user is switching to a new domain or topic entirely, set intent to "SWITCH".
If they are continuing the current topic, set intent to "CONTINUE" and output the updated context.

For Cross-Domain requests, populate `stores_artifacts` and `spaces_artifacts` separately. Use `db_artifacts` only for single-domain compatibility or when there is only one relevant domain-specific artifact list.

Operational Layers definition:
- "Single-Domain": Operations restricted to a single domain (either Stores or Spaces).
- "Cross-Domain": Operations coordinating across multiple domains (mixing Stores and Spaces).
  *Disambiguation: If the words "store" or "space" are just part of normal content or a category name, keep the request Single-Domain. Use Cross-Domain only when the request operates across both Stores and Spaces.*

Respond with JSON only in this schema:
{
  "intent": "SWITCH",
  "entity": "Omni",
  "domain": "Stores",
  "db_artifacts": ["users_table"],
  "stores_artifacts": ["users_table"],
  "spaces_artifacts": [],
  "layers": [
    {"name": "Single-Domain", "crud": ["C", "R"]}
  ]
}
