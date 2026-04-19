#!/bin/bash
sed -i '' '470,526c\
// Active Domains from Both Local UserDB and Global SystemDB\
// Extract available domains, add them to context, then do vector search if any match\
if p := ai.GetSessionPayload(ctx); p != nil && p.ActiveDomain != "" {\
s := strings.Split(p.ActiveDomain, ",")\
 := range domains {\
 = strings.TrimSpace(domain)\
 == "" || domain == "custom" {\
tinue\
tify the DB containing this domain\
DB *database.Database\
s{a.systemDB.Config()}\
range a.databases {\
d(dbOptsList, dbOpts)\
range dbOptsList {\
ewDatabase(dbOpts)\
ames(ctx, tempDB)\
 := false\
range kbs {\
 {\
 = true\
 {\
DBdomainDBdomainDBdomainDBdomainDB ni\
DB.Bif DB.Bif DB.B, err := domainDB.OpenVectorStore(ctx, domain, tx, vector.Config{UsageMode: ai.BuildOnceQuerif DB.Bif DB.Bif ainDB.Bif a.service.Domain()vecs, err := a.service.Domain({quvecs, err := a.service.Domain()0 ()vecs, errrr == nil && len(hits) > 0 {\
"tent"].(string); ok {\
text (Score: %.2f): %s\\n", hit.Score, valStr)\
t/copilot.go
