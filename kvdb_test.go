package kvdb

import "testing"
import "bytes"

func TestSetName(t *testing.T) {
	db := New("test_db")
	db.SetName("new_name")
	if db.Name() != "new_name" {
		t.Error("Expected new_name, got ", db.Name())
	}
}

func TestNew(t *testing.T) {
	db := New("test_db")
	if db.Name() != "test_db" {
		t.Error("Expected test_db, got ", db.Name())
	}
}

func TestInsert(t *testing.T) {
	db := New("test_db")
	value := []byte("kvdb")
	err := db.Insert("key", value, false)
	if err != nil {
		t.Error("Insert on an empty db failed")
	}
	err = db.Insert("key", value, false)
	if *err != DatabaseKeyExists {
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
	db := New("test_db")
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
