package gobroker

import "encoding/json"

var StdJSONCodec = &stdCodec{}

type stdCodec struct {
}

func (j *stdCodec) Decode(data []byte) (interface{}, error) {
	var i interface{}
	err := json.Unmarshal(data, &i)
	return i, err
}

func (j *stdCodec) Encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}
