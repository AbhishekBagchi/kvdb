package kvdb

import "testing"

func TestNew(t *testing.T) {
	db := New("test_db")
	if db.Name() != "test_db" {
		t.Error("Expected test_db, got ", db.Name())
	}
}
