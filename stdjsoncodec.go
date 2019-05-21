package gobroker

import "encoding/json"

var StdJSONCodec = &stdCodec{}

type stdCodec struct {
}

func (j *stdCodec) Decode(src []byte, dst interface{}) error {
	return json.Unmarshal(src, dst)
}

func (j *stdCodec) Encode(src interface{}) ([]byte, error) {
	return json.Marshal(src)
}
