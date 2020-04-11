package kvdb

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"strconv"
)

// Database encapsulates the underlying datastore, and other related metadata.
//FIXME Almost certainly not threadsafe
/*
 * Restrictions :
 *		- The length of name should fit in 1 byte. So the maximum length is math.MaxUint8
 */
type Database struct {
	valid bool
	name  string
	data  shardedMap
}

// Name is the getter for the database name
func (db *Database) Name() string {
	return db.name
}

// SetName is the setter for the database name
func (db *Database) SetName(name string) *ErrorType {
	if len(name) > math.MaxUint8 {
		err := DatabaseNameOutOfBounds
		return &err
	}
	db.name = name
	return nil
}

//New creates and return a database with a given name. Returns nil if creation is not successful
func New(name string) *Database {
	if len(name) > math.MaxUint8 {
		return nil
	}
	return &Database{valid: true, name: name, data: newShardMap()}
}

// Insert key value pair into the database. Overwrite existing value is parameter is true. No validation on the data is performed.
func (db *Database) Insert(key string, value []byte, overwrite bool) *ErrorType {
	if db.valid == false {
		err := DatabaseStateInvalid
		return &err
	}
	err := insertIntoShardedMap(db.data, key, value, overwrite)
	if err != nil {
		err := DatabaseKeyExists
		return &err
	}
	return nil
}

// Get a value for a key from the map. Return DatabaseKeyNotPresent if not in map
func (db *Database) Get(key string) ([]byte, *ErrorType) {
	if db.valid == false {
		err := DatabaseStateInvalid
		return nil, &err
	}
	value, err := getFromShardedMap(db.data, key)
	if err != nil {
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

//Export writes out the database to a file
//FIXME For the time being, this overwrites exising files and dumps the entire data into the file
//FIXME Better error handling
func (db *Database) Export(filename string) error {
	fd, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer fd.Close()

	nameLen := len(db.Name())
	nameByte := make([]byte, 1)
	nameByte[0] = byte(nameLen)
	_, err = fd.Write(nameByte)
	if err != nil {
		return err
	}
	name := []byte(db.Name())
	length, err := fd.Write(name)
	if err != nil {
		return err
	}
	if length != nameLen {
		return errors.New("Wrote " + strconv.Itoa(length) + " bytes, expected to write " + strconv.Itoa(nameLen) + " bytes")
	}
	var i uint32 = 0
	keyLen := make([]byte, 4)
	valueLen := make([]byte, 4)
	for ; i < shards; i++ {
		for key, value := range (*db.data)[i].data {
			binary.LittleEndian.PutUint32(keyLen, uint32(len(key)))
			_, err = fd.Write(keyLen)
			if err != nil {
				return err
			}
			_, err = fd.Write([]byte(key))
			if err != nil {
				return err
			}
			binary.LittleEndian.PutUint32(valueLen, uint32(len(value)))
			_, err = fd.Write(valueLen)
			if err != nil {
				return err
			}
			_, err = fd.Write([]byte(value))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

//Open reads in a database from disk, and creates a new one in memory and on disk if it can't find one with the supplied filename
//FIXME I hate how this is written
func Open(filename string, create bool) (*Database, error) {
	fd, err := os.OpenFile(filename, os.O_RDONLY, 0755)
	if err != nil {
		if os.IsNotExist(err) {
			//Doesn't exist. Create a new one and return
			db := New(filename)
			db.Export(filename)
			return db, nil
		}
		return nil, err
	}
	defer fd.Close()

	nameLen := make([]byte, 1)
	length, err := fd.Read(nameLen)
	if length != 1 || err != nil {
		return nil, err
	}
	name := make([]byte, uint8(nameLen[0]))
	length, err = fd.Read(name)
	if length != int(nameLen[0]) || err != nil {
		return nil, err
	}
	db := New(string(name))

	length = 0
	err = nil
	keyLenBytes := make([]byte, 4)
	valueLenBytes := make([]byte, 4)
	for {
		length, err = fd.Read(keyLenBytes)
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
		keyLen := binary.LittleEndian.Uint32(keyLenBytes)
		key := make([]byte, keyLen)
		length, err = fd.Read(key)
		if uint32(length) != keyLen {
			return nil, errors.New(db.Name() + "Failed to read key" + strconv.Itoa(length) + " wanted to read " + strconv.Itoa(int(keyLen)) + "bytes")
		}
		if err != nil {
			return nil, errors.New(db.Name() + "Failed to read key: " + err.Error())
		}
		length, err = fd.Read(valueLenBytes)
		if uint32(length) != 4 {
			return nil, errors.New(db.Name() + "Failed to read value length" + strconv.Itoa(length))
		}
		if err != nil {
			return nil, errors.New(db.Name() + "Failed to read value length: " + err.Error())
		}
		valueLen := binary.LittleEndian.Uint32(valueLenBytes)
		value := make([]byte, valueLen)
		length, err = fd.Read(value)
		if uint32(length) != valueLen {
			return nil, errors.New(db.Name() + "Failed to read value" + strconv.Itoa(length) + "for key " + string(key) + " wanted to read " + strconv.Itoa(int(keyLen)) + "bytes")
		}
		if err != nil {
			return nil, errors.New(db.Name() + "Failed to read value: " + "for key " + string(key) + err.Error())
		}
		db.Insert(string(key), value, false)
	}
}
