package kvdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strconv"
)

// Database encapsulates the underlying datastore, and other related metadata.
/*Restrictions :
- The length of name should fit in 1 byte. So the maximum length is math.MaxUint8
*/
type Database struct {
	name string
	data shardedMap
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
	return &Database{name: name, data: newShardMap()}
}

//ToRawMap returns all the data as a raw map
func (db *Database) ToRawMap() map[string][]byte {
	return getRawMap(db.data)
}

//Delete removes a key from the database.
//It behaves the same way as the in build delete wherein it doesn't return anything and does nothing if the key doesn't exist
func (db *Database) Delete(key string) {
	deleteFromShardedMap(db.data, key)
}

// Insert key value pair into the database. Overwrite existing value is parameter is true. No validation on the data is performed.
func (db *Database) Insert(key string, value []byte, overwrite bool) *ErrorType {
	err := insertIntoShardedMap(db.data, key, value, overwrite)
	if err != nil {
		err := DatabaseKeyExists
		return &err
	}
	return nil
}

// Get a value for a key from the map. Return DatabaseKeyNotPresent if not in map
func (db *Database) Get(key string) ([]byte, *ErrorType) {
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

func writeChunk(chunk []byte, bufw *bufio.Writer) error {
	lenByteArr := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenByteArr, uint32(len(chunk)))
	_, err := bufw.Write(lenByteArr)
	if err != nil {
		return err
	}
	_, err = bufw.Write([]byte(chunk))
	return nil
}

func tempDir(dest string) string {
	tempdir := os.Getenv("TMPDIR")
	if tempdir == "" {
		//Ensures the tmp file is on the same device as the final file so as to not fail on rename
		tempdir = filepath.Dir(dest)
	}
	return tempdir
}

//Export writes out the database to a file. This overwrites exising files and dumps the entire data into the file
func (db *Database) Export(filename string) (err error) {
	//fd, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0755)
	fd, err := ioutil.TempFile(tempDir(filename), "tmp-")
	if err != nil {
		return
	}

	//Defer file Close() while still checking for errors from the close call
	defer func() {
		// If returning with an error, cleanup the tmpfile
		if err != nil {
			fd.Close()
			os.Remove(fd.Name())
		}
	}()

	//User buffered writers
	bufw := bufio.NewWriter(fd)

	nameLen := len(db.Name())
	nameByte := make([]byte, 1)
	nameByte[0] = byte(nameLen)
	_, err = bufw.Write(nameByte)
	if err != nil {
		return err
	}
	name := []byte(db.Name())
	length, err := bufw.Write(name)
	if err != nil {
		return err
	}
	if length != nameLen {
		return errors.New("Wrote " + strconv.Itoa(length) + " bytes, expected to write " + strconv.Itoa(nameLen) + " bytes")
	}
	err = db.data.writeShardedMap(bufw)
	if err != nil {
		return
	}

	//Flush the buffered writer
	err = bufw.Flush()
	if err != nil {
		return
	}

	//Set correct permissions before renaming. TempFile creates a file with 0600
	err = fd.Chmod(0644)
	if err != nil {
		return
	}
	fd.Sync()
	err = fd.Close()
	if err != nil {
		return
	}

	return os.Rename(fd.Name(), filename)
}

func readChunk(fd *os.File) ([]byte, error) {
	lenBytes := make([]byte, 4)
	length, err := fd.Read(lenBytes)
	if length == 0 && err == io.EOF {
		//EOF
		return nil, err
	}
	if uint32(length) != 4 {
		return nil, errors.New("Failed to read chunk length " + strconv.Itoa(length))
	}
	if err != nil {
		return nil, errors.New("Failed to read chunk length : " + err.Error())
	}
	chunkLen := binary.LittleEndian.Uint32(lenBytes)
	chunk := make([]byte, chunkLen)
	length, err = fd.Read(chunk)
	if uint32(length) != chunkLen {
		return nil, errors.New("Failed to read chunk" + strconv.Itoa(length) + " wanted to read " + strconv.Itoa(int(chunkLen)) + "bytes")
	}
	if err != nil {
		return nil, errors.New("Failed to read chunk: " + err.Error())
	}
	return chunk, nil
}

//Open reads in a database from disk, and creates a new one in memory and on disk if it can't find one with the supplied filename
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
	for {
		key, err := readChunk(fd)
		if key == nil && err == io.EOF {
			//EOF
			return db, nil
		}
		value, err := readChunk(fd)
		if err != nil {
			return nil, err
		}
		db.Insert(string(key), value, false)
	}
}
