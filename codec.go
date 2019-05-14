package gobroker

// Codec provides adapter for byte-level encode and decode message body content type
type Codec interface {
	Decode(data []byte) (interface{}, error)
	Encode(v interface{}) ([]byte, error)
}
