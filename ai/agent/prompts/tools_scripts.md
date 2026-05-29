# Script Authoring Tools
Use these tools when the user wants to create or update a reusable script instead of only running a one-off query.

## Tool Choice
- Use `create_script` to create a brand-new named script.
- Use `save_script` to replace or overwrite a full existing script definition.
- Use `save_last_step` to append the most recent executed tool call into an existing script.
- Use `refactor_last_interaction` when the last interaction already contains multiple tool calls that should be converted into a reusable script or block.
- Use `get_script_details` or `list_scripts` before overwriting when you need to inspect an existing script.

## Payload Shape
For `create_script` and `save_script`, provide the reusable script steps as `script`.
Legacy alias `steps` is accepted, but prefer `script`.
- Provide reusable script steps under the `script` field.

## Authoring Guidance
- When the script is a reusable database workflow, prefer a single `execute_script` command step that contains the full atomic AST.
- Keep the script steps reusable and self-contained; avoid conversational text inside stored command steps.
- For a read-only reusable report, keep the inner AST read-only unless the user explicitly asks for mutations.
- If the user explicitly says "named ..." or "save as a script", favor `create_script` over direct execution-only tools.
