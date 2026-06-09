package memory

// Preference stores a durable user preference that can be persisted in LTM,
// projected into MRU, and finally copied into request-scoped runtime state.
// Typed value lanes avoid ambiguous any-typed payloads at the memory boundary.
type Preference struct {
	Key          string   `json:"key"`
	BoolValue    *bool    `json:"bool_value,omitempty"`
	StringValue  string   `json:"string_value,omitempty"`
	NumberValue  *float64 `json:"number_value,omitempty"`
	UpdatedAtUTC int64    `json:"updated_at_utc,omitempty"`
	Source       string   `json:"source,omitempty"`
}

const PreferenceKeyVerbose = "verbose"

// NewBoolPreference creates a typed boolean preference record.
func NewBoolPreference(key string, value bool) Preference {
	return Preference{
		Key:       key,
		BoolValue: &value,
	}
}

// Bool returns the stored boolean value and whether the preference is boolean-typed.
func (p Preference) Bool() (bool, bool) {
	if p.BoolValue == nil {
		return false, false
	}
	return *p.BoolValue, true
}
