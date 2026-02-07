package protocol

const (
	FLAG_OK              byte = 0x01
	FLAG_NOT_OK          byte = 0x02
	FLAG_HAS_NEXT        byte = 0x03
	FLAG_NO_NEXT         byte = 0x04
	FLAG_FAIL            byte = 0x05
	FLAG_INVALID_SESSION byte = 0x44
)
