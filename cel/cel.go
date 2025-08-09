package cel

import (
	"fmt"
	"reflect"

	"github.com/google/cel-go/cel"
)

// Evaluator holds a CEL expression and compiled program used to compare map-based keys.
type Evaluator struct {
	Expression string
	program    cel.Program
}

// NewEvaluator compiles a CEL expression that can compare mapX and mapY values and returns an Evaluator.
// Both mapX and mapY are expected to be map[string]any variables in the program context.
func NewEvaluator(name string, expression string) (*Evaluator, error) {
	if name == "" {
		return nil, fmt.Errorf("name can't be emptry string")
	}
	if expression == "" {
		return nil, fmt.Errorf("expression can't be emptry string")
	}

	env, err := cel.NewEnv(
		// Declare variables based on the expected context (JSON/map[string]any) data to be evaluated against.
		cel.Variable("mapX", cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable("mapY", cel.MapType(cel.StringType, cel.AnyType)),
	)
	if err != nil {
		return nil, fmt.Errorf("error creating CEL environment: %v", err)
	}

	ast, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("error compiling CEL expression: %v", issues.Err())
	}
	p, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("error creating Program: %v", err)
	}
	return &Evaluator{
		Expression: expression,
		program:    p,
	}, nil
}

// Evaluate executes the compiled CEL expression against the provided mapX and mapY and returns an int result.
func (e *Evaluator) Evaluate(mapX map[string]any, mapY map[string]any) (int, error) {
	out, _, err := e.program.Eval(map[string]any{
		"mapX": mapX,
		"mapY": mapY,
	})
	if err != nil {
		return 0, fmt.Errorf("error evaluating CEL expression: %v", err)
	}
	nv, err := out.ConvertToNative(reflect.TypeOf(int(0)))
	if err != nil {
		return 0, fmt.Errorf("error ConvertToNative, got err: %v", err)
	}

	if v, ok := nv.(int); !ok {
		return 0, fmt.Errorf("error converting to int, nv: %v", nv)
	} else {
		return v, nil // Return the evaluation result
	}
}
