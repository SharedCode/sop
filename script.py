import sys

with open("ai/agent/copilot.go", "r") as f:
    orig = f.read()

start = orig.find("// Active Domains from Both Local UserDB and Global SystemDB")
end = orig.find("// Inject Current Database Schema/Stores", start)

old_str = orig[start:end]

new_str = """// Active Domains from Both Local UserDB and Global SystemDB
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
range a.databases {
d(dbOptsList, dbOpts)
range dbOptsList {
ewDatabase(dbOpts)
ames(ctx, tempDB)
 := falhasDomain := falhasDomain := falb = := hasDhasDomain := falhasDomain := falhasDomain := falb = B
DB.BeginTransaction(ctx, s err := domainDx, domain, tx, vector.Config{UsageMode: ai.BuildOnceQueryMany})
il && a.sif err == nil && a.sif err == nil && a.sif err == nibeddif err == nil && a.sif err == nil omaiif err == nil && a.sif string{query})
il && leif err == nil && leif err ==ry(il && leif err == nil && lerr == nil && leifnAif err == nil && leif err == nil && leifit := range hits {
<hit.it.Payif hit.Score <<hit.it.Payif hit.Score <hit.Score <hit.it.Payif hit.Score <ew_str))
print("Successfully modified copilot.go")
