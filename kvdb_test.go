package kvdb

import (
	"bytes"
	"math/rand"
	"os"
	"reflect"
	"sort"
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

func getDummyDbKeys(name string, size int) (*Database, []string) {
	keys := make([]string, size)
	db := New(name)
	for i := 0; i < size; i++ {
		key := string(generateRandomBytes(50))
		keys[i] = key
		value := generateRandomBytes(100)
		db.Insert(key, value, false)
	}
	return db, keys
}

func getDummyDb(name string, size int) *Database {
	db := New(name)
	for i := 0; i < size; i++ {
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
	db = getDummyDb("test_db", 500)
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
	db := getDummyDb("test_db", 500)
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

	defer func() {
		//Cleanup
		err := os.Remove(filename)
		if err != nil {
			t.Error("Error on cleaning up files " + err.Error())
		}
	}()

	db, err := Open(filename, true)
	if err != nil {
		t.Error("Error in opening database")
	}
	if db.Name() != filename {
		t.Error("Name set to the wrong value")
	}
}

func TestSerialization(t *testing.T) {
	db := getDummyDb("test_db", 500)
	db.Insert("key", []byte("kvdb"), false)
	db.Insert("key2", []byte("key2"), false)
	db.Insert("key3", []byte(""), false)
	var filename string = "test.kvdb"

	defer func() {
		//Cleanup
		err := os.Remove(filename)
		if err != nil {
			t.Error("Error on cleaning up files " + err.Error())
			t.FailNow()
		}
	}()

	err := db.Export(filename)
	if err != nil {
		t.Error("Error in exporting database")
		t.FailNow()
	}
	newDB, err := Open(filename, false)
	if err != nil {
		t.Error("Returned err on opening")
		t.Log(err)
		t.FailNow()
	}
	if newDB == nil {
		t.Error("Returned nil on opening")
		t.FailNow()
	}
	eq := reflect.DeepEqual(db.data, newDB.data)
	if eq == false {
		t.Error("Data does not match")
		t.FailNow()
	}
}

func TestKeys(t *testing.T) {
	db, ogKeys := getDummyDbKeys("test_db", 1000)
	keys := db.Keys()
	sort.Strings(ogKeys)
	sort.Strings(keys)
	if len(keys) != len(ogKeys) {
		t.Errorf("Inserted %v keys, Keys() returned %v keys.", len(ogKeys), len(keys))
		t.FailNow()
	}
	for i, v := range ogKeys {
		if v != keys[i] {
			t.Errorf("Expected %v, got %v", v, keys[i])
			t.FailNow()
		}
	}
}

func BenchmarkSerialization(b *testing.B) {
	db := getDummyDb("test_db", 1000)
	db.Insert("key", []byte("kvdb"), false)
	db.Insert("key2", []byte("key2"), false)
	db.Insert("key3", []byte(""), false)
	b.ResetTimer()
	var filename string = "benchmark.kvdb"

	defer func() {
		//Cleanup
		err := os.Remove(filename)
		if err != nil {
			b.Error("Error on cleaning up files " + err.Error())
			b.FailNow()
		}
	}()

	err := db.Export(filename)
	if err != nil {
		b.Error("Error in exporting database")
		b.FailNow()
	}
	newDB, err := Open(filename, false)
	if err != nil {
		b.Error("Returned err on opening")
		b.Log(err)
		b.FailNow()
	}
	if newDB == nil {
		b.Error("Returned nil on opening")
		b.FailNow()
	}
	eq := reflect.DeepEqual(db.data, newDB.data)
	if eq == false {
		b.Error("Data does not match")
		b.FailNow()
	}
}
