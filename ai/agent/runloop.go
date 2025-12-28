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
func RunLoop(ctx context.Context, agent ai.Agent[map[string]any], r io.Reader, w io.Writer, prompt string, assistantName string) error {
	scanner := bufio.NewScanner(r)

	if prompt == "" {
		prompt = "User> "
	}
	if assistantName == "" {
		assistantName = "AI Assistant"
	}

	for {
		fmt.Fprintf(w, "\n%s", prompt)
		if !scanner.Scan() {
			break
		}
		input := strings.TrimSpace(scanner.Text())
		if input == "help" {
			fmt.Fprintln(w, "Available commands:")
			fmt.Fprintln(w, "  exit             - Exit the session")
			fmt.Fprintln(w, "  reset            - Clear the screen")
			fmt.Fprintln(w, "  help             - Display this help message")
			continue
		}
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
			fmt.Fprintf(w, "\n%s: %s\n", assistantName, answer)
		}
	}
	return scanner.Err()
}
