#!/bin/bash
sed -i '' 's/_kb_/_space_/g' prompts/tools_spaces.md
sed -i '' 's/search_kb/search_space/g' prompts/tools_spaces.md
sed -i '' 's/search_kb/search_space/g' *.go
sed -i '' 's/upsert_kb_items/upsert_space_items/g' *.go
sed -i '' 's/delete_kb_categories/delete_space_categories/g' *.go
sed -i '' 's/delete_kb_items/delete_space_items/g' *.go
sed -i '' 's/list_kb_categories/list_space_categories/g' *.go
sed -i '' 's/list_kb_items/list_space_items/g' *.go
