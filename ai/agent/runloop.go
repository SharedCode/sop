package agent

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/sharedcode/sop/ai"
)

// RunLoop starts an interactive Read-Eval-Print Loop (REPL) for the agent.
// It reads user input from r, processes it using the agent's Ask method,
// and writes the response to w.
// The loop continues until the user enters "exit" or the input stream ends.
func RunLoop(ctx context.Context, agent ai.Agent[map[string]any], r io.Reader, w io.Writer) error {
	scanner := bufio.NewScanner(r)

	for {
		fmt.Fprint(w, "\nUser> ")
		if !scanner.Scan() {
			break
		}
		input := strings.TrimSpace(scanner.Text())
		if input == "exit" {
			break
		}
		if input == "reset" {
			fmt.Fprint(w, "\033[H\033[2J")
			continue
		}
		if input == "" {
			continue
		}

		// Inject writer into context for streaming output
		loopCtx := context.WithValue(ctx, ai.CtxKeyWriter, w)

		answer, err := agent.Ask(loopCtx, input)
		if err != nil {
			fmt.Fprintf(w, "Error: %v\n", err)
		} else {
			fmt.Fprintf(w, "\nAI Assistant: %s\n", answer)
		}
	}
	return scanner.Err()
}
