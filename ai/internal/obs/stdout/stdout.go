package stdout

import (
	"fmt"
	"time"
)

type Simple struct{}

func (s *Simple) log(level, msg string, kv ...any) {
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	fmt.Printf("%s level=%s msg=\"%s\"", ts, level, msg)
	for i := 0; i+1 < len(kv); i += 2 {
		fmt.Printf(" %v=%v", kv[i], kv[i+1])
	}
	fmt.Println()
}

func (s *Simple) Debug(msg string, kv ...any) { s.log("DEBUG", msg, kv...) }
func (s *Simple) Info(msg string, kv ...any)  { s.log("INFO", msg, kv...) }
func (s *Simple) Warn(msg string, kv ...any)  { s.log("WARN", msg, kv...) }
func (s *Simple) Error(msg string, kv ...any) { s.log("ERROR", msg, kv...) }
