package kvdb

//FIXME Almost certainly not threadsafe
/* FIXME
 * I am not sure what the expected way of doing this is. I do not want users to directly work on the database structure.
 * One option is to export the Database structure, but not expose any of the internal members (Does that even work? Need to check)
 * The other option is to export an interface, and define an internal structure that impliments that interface, with those interface functions being exported instead.
 * This has the problem of adding an extra leyer of abstraction. Maybe the performance impact for this is low enough to not matter?
 * Going with option 1 for now, exporting the structure but not the internals
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
func (db *Database) SetName(name string) {
	db.name = name
}

// Create and return a new database with a given name
func New(name string) *Database {
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
