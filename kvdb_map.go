package kvdb

import (
	"bufio"
	"errors"
	"hash/fnv"
	"sync"
)

var shards uint32 = 20

/*
The sharding has does not have to be cryptographically secure. However, speedy would be ideal
Of all the options the standard golang library seems to have, the ideal ones seem to be maphash and fnv.
Maphash is seeded, which is not something I want to deal with
FNV seems ideal. Not sure how to pick between FNV1 and FNV1-a
The SMHasher tool talks about other faster hashing implimentations, but they'll be overkill for now
*/

// mapShard is the basic block representing a shard of the overall map
type mapShard struct {
	sync.RWMutex
	data    map[string][]byte
	numKeys int
}

//type shardedMap = []mapShard
type shardedMap struct {
	shards []mapShard
}

func newShardMap() shardedMap {
	m := shardedMap{shards: make([]mapShard, shards)}
	var i uint32 = 0
	for ; i < shards; i++ {
		m.shards[i] = mapShard{data: make(map[string][]byte), numKeys: 0}
	}
	return m
}

func (m shardedMap) writeShardedMap(bufw *bufio.Writer) error {
	for shard := range m.shards {
		m.shards[shard].RLock()
		for key, value := range m.shards[shard].data {
			err := writeChunk([]byte(key), bufw)
			err = writeChunk([]byte(value), bufw)
			if err != nil {
				return err
			}
		}
		m.shards[shard].RUnlock()
	}
	return nil
}

func getRawMap(m shardedMap) map[string][]byte {
	return_data := make(map[string][]byte)
	for shard := range m.shards {
		m.shards[shard].RLock()
		for key, value := range m.shards[shard].data {
			return_data[key] = value
		}
		m.shards[shard].RUnlock()
	}
	return return_data
}

func getShardID(key string) uint32 {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	return (hash.Sum32() % shards)
}

func deleteFromShardedMap(m shardedMap, key string) {
	shard := getShardID(key)
	shardedMap := &m.shards[shard]
	shardedMap.Lock()
	defer shardedMap.Unlock()
	delete(shardedMap.data, key)
}

func insertIntoShardedMap(m shardedMap, key string, value []byte, overwrite bool) error {
	shard := getShardID(key)
	shardedMap := &m.shards[shard]
	shardedMap.RLock()
	_, ok := shardedMap.data[key]
	shardedMap.RUnlock()
	if overwrite == false && ok == true {
		return errors.New("Key exists. overwrite set to false")
	}
	shardedMap.Lock()
	defer shardedMap.Unlock()
	shardedMap.data[key] = value
	shardedMap.numKeys++
	return nil
}

func getFromShardedMap(m shardedMap, key string) ([]byte, error) {
	shard := getShardID(key)
	shardedMap := &m.shards[shard]
	shardedMap.RLock()
	defer shardedMap.RUnlock()
	value, ok := m.shards[shard].data[key]
	if ok == false {
		return nil, errors.New("Key not found in database")
	}
	return value, nil
}
