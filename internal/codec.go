package internal

type Codec[Item any] interface {
	Encode(buffer Buffer[Item]) ([]byte, error)
	Decode(data []byte, buffer Buffer[Item]) error
}
