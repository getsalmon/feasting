package data_types


type KafkaCompatible interface {
	GetKey() ([]byte, error)
	GetValue() ([]byte, error)
}
