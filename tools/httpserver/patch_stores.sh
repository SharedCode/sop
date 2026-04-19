sed -i '' -e '946,949c\
        var visibleStores []string\
        for _, s := range stores {\
                if !strings.Contains(s, "/") && !strings.HasSuffix(s, "_vecs") {\
                        visibleStores = append(visibleStores, s)\
                }\
        }\
\
        json.NewEncoder(w).Encode(visibleStores)' main.go
