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
	value := []byte{'k', 'v', 'd', 'b'}
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

	value := []byte{'k', 'v', 'd', 'b'}
	err = db.Insert("key", value, false)
	if err != nil {
		t.Error("Insert on an empty db failed")
	}

	val, err = db.Get("key")
	if err != nil {
		t.Error("Key inserted. err should be nil")
	}
	if bytes.Compare(val, value) != 0 {
		t.Error("Get doesn't return the inserted value")
	}
}
