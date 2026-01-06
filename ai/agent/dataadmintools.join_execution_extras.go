package agent

import (
	"encoding/json"

	"github.com/sharedcode/sop/jsondb"
)

func (jc *JoinRightCursor) GetIndexSpecs() map[string]*jsondb.IndexSpecification {
	specs := make(map[string]*jsondb.IndexSpecification)

	// Get spec from Left
	if provider, ok := jc.left.(SpecProvider); ok {
		leftSpecs := provider.GetIndexSpecs()
		for k, v := range leftSpecs {
			specs[k] = v
		}
	}

	// Get spec from Right
	info := jc.right.GetStoreInfo()
	if info.MapKeyIndexSpecification != "" {
		var spec jsondb.IndexSpecification
		if err := json.Unmarshal([]byte(info.MapKeyIndexSpecification), &spec); err == nil {
			specs["right_key"] = &spec
		}
	}
	return specs
}
