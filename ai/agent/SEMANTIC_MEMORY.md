# Semantic Memory Structure & Rules

This document defines the structured Knowledge Schema used by the AI Agent to store, retrieve, and apply semantic learnings. This structure allows the LLM to refine its understanding of user intent and domain specifics over time.

## 1. The Structure

All semantic knowledge is stored in the `llm_knowledge` B-Tree using specific **Namespaces** (Categories). The `Value` of each entry MUST be a valid JSON string adhering to the schemas below.

### Namespace: `vocabulary`
**Purpose**: Maps user-specific terms, synonyms, or domain slang to actual database fields or store names.
**Key Format**: `{TargetResource}:{UserTerm}` 
  - Example: `users:full_name`, `orders:cost`, `global:client`

**Value Schema**:
```json
{
  "target": "target_field_name",
  "type": "synonym", 
  "description": "Explanation of why this mapping exists",
  "confidence": 1.0,
  "source": "user_instruction" // or "inference"
}
```

**Example Entry**:
- **Key**: `users:client_name`
- **Value**: 
```json
{ "target": "name", "type": "synonym", "description": "Users refer to the 'name' field as 'client_name'" }
```

---

### Namespace: `rule`
**Purpose**: Defines business logic, default filters, or constraints that apply to specific concepts.
**Key Format**: `{ConceptName}`
  - Example: `vip_user`, `recent_orders`, `active_account`

**Value Schema**:
```json
{
  "condition": "filter expression or SQL fragment",
  "applies_to": ["store_name"],
  "description": "Natural language explanation of the rule"
}
```

**Example Entry**:
- **Key**: `vip_user`
- **Value**:
```json
{ "condition": "orders_count > 100 AND status == 'active'", "applies_to": ["users"], "description": "VIP users must have over 100 orders and be active" }
```

---

### Namespace: `correction`
**Purpose**: Records past failures to prevent recurring mistakes.
**Key Format**: `{ErrorSignature}`
  - Example: `join_users_orders`, `date_format_iso`

**Value Schema**:
```json
{
  "instruction": "Specific instruction on what to do/avoid",
  "trigger": "When asking about X..."
}
```

## 2. Usability Rules (System Prompts)

The following logic applies when the AI loads this knowledge:

### Rule 1: Resolution Priority
1.  **Direct Match**: If the user's term matches a `vocabulary` Key exactly, AUTOMATICALLY replace the term with the `target` in the Value.
2.  **Context Match**: If the user mentions a Concept found in the `rule` namespace, AUTOMATICALLY append the `condition` to the query filter.

### Rule 2: Active Learning
- If the user corrects the AI (e.g., "No, by 'cost' I mean the 'total_amount' field"), the AI MUST:
    1.  Generate a `manage_knowledge` call.
    2.  Set `namespace="vocabulary"`.
    3.  Set `key="{Store}:cost"`.
    4.  Set `value` to the structure defined above.
    5.  Confirm to the user: "I have learnt that 'cost' refers to 'total_amount' for future queries."

### Rule 3: Transparency
- When applying a Semantic Mapping or Rule, the AI should optionally include a comment in the reasoning (e.g., "Applying learnt rule: 'vip_user'").

## 3. Implementation Guide

To implement this, the System Prompt Generator (`service.go`) must be updated to:
1.  Load keys from `vocabulary` and `rule`.
2.  Parse the JSON Values.
3.  Format them into a specific "Semantic Map" section in the Prompt.

**Format for Prompt**:
```text
[Semantic Memory]
The following terms have special meanings in this database:
- "client_name" (users) -> Mapped to field: "name"
- "cost" (orders) -> Mapped to field: "amount"

[Business Rules]
- "vip_user": Apply filter "count > 100"
```
