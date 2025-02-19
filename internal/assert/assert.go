package internal

import "errors"

func Assert(condition bool, message string) {
	if !condition {
		panic("assertion failed: " + message)
	}
}

func CheckLimit(key []byte, keySizeLimit int, value []byte, valSizeLimit int) error {
	if len(key) > keySizeLimit || len(value) > valSizeLimit {
		return errors.New("the sizes of key or value is exceeding the range allowed")
	}

	return nil
}