package kvdb

import (
	"errors"
	"hash/fnv"
)

var shards uint32 = 20

/*
The sharding has does not have to be cryptographically secure. However, speedy would be ideal
Of all the options the standard golang library seems to have, the ideal ones seem to be maphash and fnv.
Maphash is seeded, which is not something I want to deal with
FNV seems ideal. Not sure how to pick between FNV1 and FNV1-a
The SMHasher tool talks about other faster hashing implimentations, but they'll be overkill for now
*/

//FIXME is a struct because it'll eventually contain a RW mutex(?)
// mapShard is the basic block representing a shard of the overall map
type mapShard struct {
	data map[string][]byte
}

type shardedMap = *[]mapShard

func newShardMap() shardedMap {
	m := make([]mapShard, shards)
	var i uint32 = 0
	for ; i < shards; i++ {
		m[i] = mapShard{data: make(map[string][]byte)}
	}
	return &m
}

func getShardId(key string) uint32 {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	return (hash.Sum32() % shards)
}

func insertIntoShardedMap(m shardedMap, key string, value []byte, overwrite bool) error {
	shard := getShardId(key)
	_, ok := (*m)[shard].data[key]
	if overwrite == false && ok == true {
		return errors.New("Key exists. overwrite set to false")
	}
	(*m)[shard].data[key] = value
	return nil
}

func getFromShardedMap(m shardedMap, key string) ([]byte, error) {
	shard := getShardId(key)
	value, ok := (*m)[shard].data[key]
	if ok == false {
		return nil, errors.New("Key not found in database")
	}
	return value, nil
}
