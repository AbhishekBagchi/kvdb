package kvdb

import "testing"

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
