package kvdb

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"strconv"
)

//FIXME Almost certainly not threadsafe
/*
 * Restrictions :
 *		- The length of name should fit in 1 byte. So the maximum length is math.MaxUint8
 */
type Database struct {
	valid bool
	name  string            //Filename where this is read from/written to is <name>.kvdb
	data  map[string][]byte //FIXME Do we want to read the entire file in memory when importing? Not feasable for large databases
}

// Get database name
func (db *Database) Name() string {
	return db.name
}

// Set database name
func (db *Database) SetName(name string) *ErrorType {
	if len(name) > math.MaxUint8 {
		err := DatabaseNameOutOfBounds
		return &err
	}
	db.name = name
	return nil
}

/*
 * Create and return a new database with a given name
 * Returns null if creation is not successful
 */
func New(name string) *Database {
	if len(name) > math.MaxUint8 {
		return nil
	}
	return &Database{valid: true, name: name, data: make(map[string][]byte)}
}

// Insert key value pair into the database. Overwrite existing value is parameter is true. No validation on the data is performed.
func (db *Database) Insert(key string, value []byte, overwrite bool) *ErrorType {
	if db.valid == false {
		err := DatabaseStateInvalid
		return &err
	}
	_, ok := db.data[key]
	if overwrite == false && ok == true {
		err := DatabaseKeyExists
		return &err
	}
	db.data[key] = value
	return nil
}

// Get a value for a key from the map. Return DatabaseKeyNotPresent if not in map
func (db *Database) Get(key string) ([]byte, *ErrorType) {
	if db.valid == false {
		err := DatabaseStateInvalid
		return nil, &err
	}
	value, ok := db.data[key]
	if ok == false {
		err := DatabaseKeyNotPresent
		return nil, &err
	}
	return value, nil
}

/* File format
The byte order is fixed to little endian
1 byte  -> Length of filename(n)
n bytes -> filename
For each record -
	4 bytes - Length of key(k)
	k bytes - Key
	4 bytes - Length of value(v)
	v bytes - Value
*/

//FIXME For the time being, this overrites exising files and dumps the entire data into the file
//FIXME Better error handling
//Export writes out the database to a file
func (db *Database) Export(filename string) error {
	fd, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer fd.Close()

	name_len := len(db.Name())
	name_byte := make([]byte, 1)
	name_byte[0] = byte(name_len)
	_, err = fd.Write(name_byte)
	if err != nil {
		return err
	}
	name := []byte(db.Name())
	length, err := fd.Write(name)
	if err != nil {
		return err
	}
	if length != name_len {
		return errors.New("Wrote " + strconv.Itoa(length) + " bytes, expected to write " + strconv.Itoa(name_len) + " bytes")
	}
	for key, value := range db.data {
		key_len := make([]byte, 4)
		binary.LittleEndian.PutUint32(key_len, uint32(len(key)))
		_, err = fd.Write(key_len)
		if err != nil {
			return err
		}
		_, err = fd.Write([]byte(key))
		if err != nil {
			return err
		}
		value_len := make([]byte, 4)
		binary.LittleEndian.PutUint32(value_len, uint32(len(value)))
		_, err = fd.Write(value_len)
		if err != nil {
			return err
		}
		_, err = fd.Write([]byte(value))
		if err != nil {
			return err
		}
	}
	return nil
}

//FIXME I hate how this is written
//FIXME For now, create a new database with the filename as the db name
//Open reads in a database from disk, and creates a new one if it can't find one with the supplied filename
func Open(filename string) (*Database, error) {
	fd, err := os.OpenFile(filename, os.O_RDONLY, 0755)
	if err != nil {
		//Cannot open. Create new and return
		return New(filename), nil
	}
	defer fd.Close()

	name_len := make([]byte, 1)
	length, err := fd.Read(name_len)
	if length != 1 || err != nil {
		return nil, err
	}
	name := make([]byte, uint8(name_len[0]))
	length, err = fd.Read(name)
	if length != int(name_len[0]) || err != nil {
		return nil, err
	}
	db := New(string(name))

	length = 0
	err = nil
	for {
		key_len_bytes := make([]byte, 4)
		length, err = fd.Read(key_len_bytes)
		if length == 0 && err == io.EOF {
			//EOF
			return db, nil
		}
		if uint32(length) != 4 {
			return nil, errors.New(db.Name() + "Failed to read key length " + strconv.Itoa(length))
		}
		if err != nil {
			return nil, errors.New(db.Name() + "Failed to read key length : " + err.Error())
		}
		key_len := binary.LittleEndian.Uint32(key_len_bytes)
		key := make([]byte, key_len)
		length, err = fd.Read(key)
		if uint32(length) != key_len {
			return nil, errors.New(db.Name() + "Failed to read key" + strconv.Itoa(length) + " wanted to read " + strconv.Itoa(int(key_len)) + "bytes")
		}
		if err != nil {
			return nil, errors.New(db.Name() + "Failed to read key: " + err.Error())
		}
		value_len_bytes := make([]byte, 4)
		length, err = fd.Read(value_len_bytes)
		if uint32(length) != 4 {
			return nil, errors.New(db.Name() + "Failed to read value length" + strconv.Itoa(length))
		}
		if err != nil {
			return nil, errors.New(db.Name() + "Failed to read value length: " + err.Error())
		}
		value_len := binary.LittleEndian.Uint32(value_len_bytes)
		value := make([]byte, value_len)
		length, err = fd.Read(value)
		if uint32(length) != value_len {
			return nil, errors.New(db.Name() + "Failed to read value" + strconv.Itoa(length) + "for key " + string(key) + " wanted to read " + strconv.Itoa(int(key_len)) + "bytes")
		}
		if err != nil {
			return nil, errors.New(db.Name() + "Failed to read value: " + "for key " + string(key) + err.Error())
		}
		db.Insert(string(key), value, false)
	}
	return nil, nil
}
