package btree

// Item Serializer should be provided if needing to control Key, Value pair serialization.
// Otherwise, SOP will assume and use a built-in data type support. E.g. - String Key and String Value.
type ItemSerializer struct {
	KeyInfo              	string
	SerializeKey         	func(k interface{}) ([]byte, error)
	DeSerializeKey       	func(kData []byte) (interface{}, error)
	StringSerializeKey   	func(k interface{}) (string, error)
	StringDeSerializeKey 	func(kData string) (interface{}, error)

	CompareKey 				func(k1 interface{}, k2 interface{}) (int, error)

	ValueInfo              	string
	SerializeValue         	func(v interface{}) ([]byte, error)
	DeSerializeValue       	func(vData []byte) (interface{}, error)
	StringSerializeValue   	func(v interface{}) (string, error)
	StringDeSerializeValue 	func(vData string) (interface{}, error)
}

