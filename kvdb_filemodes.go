package kvdb

type Filemode uint8

const (
    Mode_Read       Filemode = iota + 1
    Mode_Write      Filemode = iota + 1
    Mode_ReadWrite  Filemode = iota + 1
)
