package kvdb

//FIXME Almost certainly not threadsafe
type Database struct {
    valid bool;
    name string; //Filename where this is read from/written to is <name>.kvdb
    data map[string][]byte; //FIXME Do we want to read the entire file in memory in one go? Not feasable for large databases
    db_filemode Filemode;
}
