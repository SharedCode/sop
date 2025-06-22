package cel

import (
	"fmt"
	"reflect"

	"github.com/google/cel-go/cel"
)

// Evaluator struct contains the CEL expression & the cel program used to evaluate expression vs. input variables.
type Evaluator struct {
	Expression string
	program    cel.Program
}

// Instantiate a new CEL evaluator for use in SOP B-tree construction of the nodes' slots key & value pairs.
// expression param is expected to be an expression that can compare mapX(key X) vs mapY(key Y) of the entries
// to be managed by the B-tree.
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

// Evaluates the CEL expression passed in on initialization vs a provided data (key X & key Y) context.
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
