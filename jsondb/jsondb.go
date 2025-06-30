package jsondb

import (
	"context"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/cel"
	"github.com/SharedCode/sop/in_red_fs"
)

type PagingDirection int

const (
	Forward = iota
	Backward
)

// Paging Info specifies fetching details.
type PagingInfo struct {
	// -1 or 0 means to fetch data starting from the current "cursor" location.
	// > 0 means to traverse to that page offset and fetch data from that "cursor" location.
	PageOffset int `json:"page_offset"`
	// Number of data elements(Keys or Items) to fetch.
	PageSize int `json:"page_size"`
	// Direction of fetch is either forward(0) or backwards(1).
	Direction PagingDirection `json:"direction"`
}

// JSON DB that can take in any JSON data marshalled as map[string]any on Key & Value pair.
type JsonDB struct {
	btree.BtreeInterface[map[string]any, any]
	evaluator    *cel.Evaluator
	compareError error
}

func (j *JsonDB) proxyComparer(mapX map[string]any, mapY map[string]any) int {
	if j.evaluator != nil {
		return j.celComparer(mapX, mapY)
	}
	return defaultComparer(mapX, mapY)
}

// Comparer for map[string]any key type using CEL expression.
func (j *JsonDB) celComparer(mapX map[string]any, mapY map[string]any) int {
	r, err := j.evaluator.Evaluate(mapX, mapY)
	if err != nil {
		j.compareError = err
	}
	return r
}

// Default Comparer of Items can compare two maps with no nested map.
func defaultComparer(mapX map[string]any, mapY map[string]any) int {
	for k, v := range mapX {
		i := btree.Compare(v, mapY[k])
		if i != 0 {
			return i
		}
	}
	return 0
}

// Instantiates a Btree for schema-less usage. I.e. - JSONy type of data marshaled by Go as map[string]any
// data type for key & value pairs.
// And using user provided CEL expression as comparer. If not provided, will use default comparer that compares each field of the key.
func NewBtree(ctx context.Context, so sop.StoreOptions, t sop.Transaction, celExpressionComparer string) (*JsonDB, error) {
	var comparer btree.ComparerFunc[map[string]any]
	j := JsonDB{}
	if celExpressionComparer == "" {
		comparer = defaultComparer
	} else {
		e, err := cel.NewEvaluator("comparer", celExpressionComparer)
		if err != nil {
			return nil, err
		}
		j.evaluator = e
		comparer = j.celComparer
	}

	b3, err := in_red_fs.NewBtreeWithReplication[map[string]any, any](ctx, so, t, comparer)
	if err != nil {
		return nil, err
	}

	j.BtreeInterface = b3
	return &j, nil
}

// Open an existing B-tree w/ option to using user provided CEL expression as comparer.
// If CEL expression is not provided, will use default comparer that compares each field of the key.
func OpenBtree(ctx context.Context, name string, t sop.Transaction) (*JsonDB, error) {
	j := JsonDB{}

	b3, err := in_red_fs.OpenBtreeWithReplication[map[string]any, any](ctx, name, t, j.proxyComparer)
	if err != nil {
		return nil, err
	}

	// Resurrect the CEL expression evaluator if CEL expression was originally provided when creating B-tree.
	ce := b3.GetStoreInfo().CELexpression
	if ce != "" {
		e, err := cel.NewEvaluator("comparer", ce)
		if err != nil {
			return nil, err
		}
		j.evaluator = e
	}

	j.BtreeInterface = b3
	return &j, nil
}
