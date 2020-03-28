package kvdb

type ErrorType uint8

const (
    Database_State_Invalid       ErrorType = iota + 1
)
