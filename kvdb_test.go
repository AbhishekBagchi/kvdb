package kvdb

import (
	"bytes"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func generateRandomBytes(length int) []byte {
	var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return b
}

func getDummyDb(name string) *Database {
	db := New(name)
	for i := 0; i < 350; i++ {
		key := string(generateRandomBytes(50))
		value := generateRandomBytes(100)
		db.Insert(key, value, false)
	}
	return db
}

func TestSetName(t *testing.T) {
	t.Parallel()
	db := New("test_db")
	err := db.SetName("new_name")
	if db.Name() != "new_name" {
		t.Error("Expected new_name, got ", db.Name())
	}
	if err != nil {
		t.Error("Name shorter than MaxUint8, should not be an error")
	}
	longName := "Kq6NtYrFJQNj1U61PIP2G4OYnTAY0CM1jBfcDTEJEQOPgrNiuHhusJnVUwEriwfhMkR6bJOIKXWa3cYZWIF6g7wDW5CjNB6vHYWVwYF7V8XAtygFNIfzYnhYnZQ3xuQ3BDHwYqDAw5YlscGPMLLMcLrzFTtYvq8YTvHzNwNk12JnZdF80n0skKIHOqogAg5sgrBXeLuRHCZmmXIIJmTabotjPPLLRy3gjasdtHTUeSJoAByjUgLHLpKTP77wfJ7a"
	err = db.SetName(longName)
	if err == nil {
		t.Error("Setting a name " + strconv.Itoa(len(longName)) + " characters long. Should throw an error")
	}
}

func TestNew(t *testing.T) {
	t.Parallel()
	longName := "Kq6NtYrFJQNj1U61PIP2G4OYnTAY0CM1jBfcDTEJEQOPgrNiuHhusJnVUwEriwfhMkR6bJOIKXWa3cYZWIF6g7wDW5CjNB6vHYWVwYF7V8XAtygFNIfzYnhYnZQ3xuQ3BDHwYqDAw5YlscGPMLLMcLrzFTtYvq8YTvHzNwNk12JnZdF80n0skKIHOqogAg5sgrBXeLuRHCZmmXIIJmTabotjPPLLRy3gjasdtHTUeSJoAByjUgLHLpKTP77wfJ7a"
	db := New(longName)
	if db != nil {
		t.Error("Setting a name " + strconv.Itoa(len(longName)) + " characters long. Should return nil")
	}
	db = New("test_db")
	if db.Name() != "test_db" {
		t.Error("Expected test_db, got ", db.Name())
	}
}

func TestInsert(t *testing.T) {
	t.Parallel()
	db := New("test_db")
	value := []byte("kvdb")
	err := db.Insert("key", value, false)
	if err != nil {
		t.Error("Insert on an empty db failed")
	}
	db = getDummyDb("test_db")
	err = db.Insert("key", value, false)
	err = db.Insert("key", value, false)
	if err != nil && *err != DatabaseKeyExists {
		t.Error("Insert when key exists should return DatabaseKeyExists")
	}
	err = db.Insert("key", value, true)
	if err != nil {
		t.Error("Insert when overwrite is set should return nil")
	}
	err = db.Insert("key", []byte(""), true)
	if err != nil {
		t.Error("Insert when overwrite is set should return nil")
	}
}

func TestGet(t *testing.T) {
	t.Parallel()
	db := getDummyDb("test_db")
	val, err := db.Get("key")
	if *err != DatabaseKeyNotPresent {
		t.Error("Get when key doesn't exists should return DatabaseKeyNotPresent as error")
	}
	if val != nil {
		t.Error("Get when key doesn't exists should return nil for value")
	}

	db.Insert("key", []byte("kvdb"), false)
	db.Insert("key2", []byte("key2"), false)
	db.Insert("key3", []byte(""), false)

	_, err = db.Get("key")
	if err != nil {
		t.Error("Key inserted. err should be nil")
	}

	val, _ = db.Get("key2")
	if bytes.Compare(val, []byte("key2")) != 0 {
		t.Error("Expected key2, got ", val)
	}

	val, _ = db.Get("key3")
	if bytes.Compare(val, []byte("")) != 0 {
		t.Error("Expected empty byte array \"\", got ", val)
	}
}

func TestOpen(t *testing.T) {
	var filename string = "open_test.kvdb"
	db, err := Open(filename, true)
	if err != nil {
		t.Error("Error in opening database")
	}
	if db.Name() != filename {
		t.Error("Name set to the wrong value")
	}
	//Cleanup
	err = os.Remove(filename)
	if err != nil {
		t.Error("Error on cleaning up files " + err.Error())
	}
}

func TestSerialization(t *testing.T) {
	db := getDummyDb("test_db")
	db.Insert("key", []byte("kvdb"), false)
	db.Insert("key2", []byte("key2"), false)
	db.Insert("key3", []byte(""), false)
	var filename string = "test.kvdb"
	err := db.Export(filename)
	if err != nil {
		t.Error("Error in exporting database")
	}
	newDB, err := Open(filename, false)
	if err != nil {
		t.Error("Returned err on opening")
		t.Log(err)
	}
	if newDB == nil {
		t.Error("Returned nil on opening")
	}
	eq := reflect.DeepEqual(db.data, newDB.data)
	if eq == false {
		t.Error("Data does not match")
	}
	//Cleanup
	err = os.Remove(filename)
	if err != nil {
		t.Error("Error on cleaning up files " + err.Error())
	}
}
