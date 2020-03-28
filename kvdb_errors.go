package kvdb

type ErrorType uint8

const (
	DatabaseStateInvalid ErrorType = iota + 1
	DatabaseKeyExists  ErrorType = iota + 1
)

func (err *ErrorType) Error() string {
	return err.String()
}
