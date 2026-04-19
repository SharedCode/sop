import sys
import re

with open('ai/agent/copilot.go', 'r') as f:
    orig = f.read()

pattern = r'(// Active Domains from Both Local UserDB and Global SystemDB\n\s*// Extract available domains, add them to context, then do vector search if any match\n\s*if p := ai\.GetSessionPayload\(ctx\); p != nil && p\.ActiveDomain != "" {).*?(// Inject Current Database Schema/Stores)'

def repl(m):
    return r"""// Active Domains from Both Local UserDB and Global SystemDB
// Extract available domains, add them to context, then do vector search if any match
if p := ai.GetSessionPayload(ctx); p != nil && p.ActiveDomain != "" {
s := strings.Split(p.ActiveDomain, ",")
 := range domains {
 = strings.TrimSpace(domain)
 == "" || domain == "custom" {
tinue
tify the DB containing this domain
DB *database.Database
s{a.systemDB.Config()}
dbOpfor _, dbOpfor _, dbOpfor _, dbOpfo dfor
_,}}}= _,}}}= _,}}}= _,Tra}}ctx,}}} _,}}} _,}}} _,}}}= , tx, vector.Config{UsageMode: ai.BuildOnceQueryMany})
il && a.sif err == nil && a.sif err == nil && a.sif err == nil ddif err == nil && a.sif err == nil &&ain().Embedder().EmbedTexts(ctx, []stif err == nil && a.sif errleif err == nil && a.sif err == ny(il && a.sif err == nil && a.if err == nil && a.sif Active Playbook Context (%s):\n", domain)
_, hit :_, hit :_, hit :_, hit :_, hit :_, hit :_, hit :_, hit :_, hit :_, hit :_, hit :_, hit :_, hit :_, hit :_, hit :('ai/agent/copilot.go', 'w') as f:
    f.write(new_content)
    
print("Updated via regex!")
