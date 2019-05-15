package gobroker

// Codec provides adapter for byte-level encode and decode message body content type
type Codec interface {
	Decode(src []byte, dst interface{}) error
	Encode(src interface{}) ([]byte, error)
}
