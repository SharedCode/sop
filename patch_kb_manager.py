import re

with open('tools/httpserver/knowledge_manager.go', 'r') as f:
    data = f.read()

data = re.sub(
    r"type PreloadKnowledgeRequest struct \{.*?\}",
    "type PreloadKnowledgeRequest struct {\n\tExpertise         string          `json:\"expertise_id\"`\n\tDatabaseName      string          `json:\"database_name\"`\n\tKnowledgeBaseName string          `json:\"knowledge_base_name,omitempty\"`\n\tCustomData        json.RawMessage `json:\"custom_data,omitempty\"`\n}",
    data,
    flags=re.DOTALL
)

with open('tools/httpserver/knowledge_manager.go', 'w') as f:
    f.write(data)
