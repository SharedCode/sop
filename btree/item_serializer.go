package btree

import (
	"fmt"
)

// Item Serializer should be provided if needing to control Key, Value pair serialization.
// Otherwise, SOP will assume and use a built-in data type support. E.g. - String Key and String Value.
type ItemSerializer struct {
	KeyInfo              	string
	SerializeKey         	func(k interface{}) ([]byte, error)
	DeSerializeKey       	func(kData []byte) (interface{}, error)
	StringSerializeKey   	func(k interface{}) (string, error)
	StringDeSerializeKey 	func(kData string) (interface{}, error)

	// CompareKey is a function that knows how to compare two key instances.
	// Comparers dictate the ordering or sorting of items based on their keys.
	CompareKey 				func(k1 interface{}, k2 interface{}) (int, error)

	ValueInfo              	string
	SerializeValue         	func(v interface{}) ([]byte, error)
	DeSerializeValue       	func(vData []byte) (interface{}, error)
	StringSerializeValue   	func(v interface{}) (string, error)
	StringDeSerializeValue 	func(vData string) (interface{}, error)
}

func (itemSer *ItemSerializer) IsValid() bool{
	if itemSer.CompareKey == nil{return false}
	// Key
	if itemSer.StringSerializeKey != nil {
		if itemSer.StringDeSerializeKey == nil{return false}
	} else if itemSer.SerializeKey != nil{
		if itemSer.DeSerializeKey == nil {return false}
	} else {return false}

	// Value
	if itemSer.StringSerializeValue != nil{
		if itemSer.StringDeSerializeValue == nil {return false}
	} else if itemSer.SerializeValue != nil{
		if itemSer.DeSerializeValue == nil {return false}
	} else {return false}

	return true
}

// CreateDefaultKVTypeHandlers create Key/Value type converters to/from string.
// Later, we can define more exhaustive built-in type handlers, like for int, float, double, date, etc...
func (ItemSerializer *ItemSerializer) CreateDefaultKVTypeHandlers(){

	// provide default key/value type handlers (string)
	ItemSerializer.StringSerializeKey = func(k interface{}) (string, error){
		return fmt.Sprintf("%s", k), nil
	}
	ItemSerializer.StringDeSerializeKey = func(kData string) (interface{}, error){
		return kData, nil
	}
	ItemSerializer.StringSerializeValue = func(v interface{}) (string, error){
		return fmt.Sprintf("%s", v), nil
	}
	ItemSerializer.StringDeSerializeValue = func(vData string) (interface{}, error){
		return vData, nil
	}
	ItemSerializer.CompareKey = func(a interface{}, b interface{}) (int, error){
		s1, _ := ItemSerializer.StringSerializeKey(a)
		s2, _ := ItemSerializer.StringSerializeKey(b)
		if s1 == s2 {return 0, nil}
		if s1 < s2 {return -1, nil}
		return 1, nil
	}
	// SOP provided serializers for "string" KV type.
	ItemSerializer.KeyInfo = "string"
	ItemSerializer.ValueInfo = "string"
}