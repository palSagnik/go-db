package internal

func Assert(condition bool, message string) {
	if !condition {
		panic("assertion failed: " + message)
	}
}	