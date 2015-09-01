package atomicgoleveldb

import "fmt"
import "hash/fnv"

import "github.com/syndtr/goleveldb/leveldb"
import "github.com/syndtr/goleveldb/leveldb/opt"

const DEFAULT_NUM_BUCKETS = 16
const DEFAULT_BUFFER_SIZE = 8

type Config struct {
	nBuckets uint
	bufferSz uint
}

func NewConfig() *Config {
	return new(Config)
}

func (c *Config) NumBuckets() uint {
	if c.nBuckets != 0 {
		return c.nBuckets
	} else {
		return DEFAULT_NUM_BUCKETS
	}
}

func (c *Config) BufferSize() uint {
	if c.bufferSz != 0 {
		return c.bufferSz
	} else {
		return DEFAULT_BUFFER_SIZE
	}
}

func (c *Config) WithBuckets(n uint) {
	c.nBuckets = n
}

func (c *Config) WithBuffer(n uint) {
	c.bufferSz = n
}

type req struct {
	do func(*leveldb.DB) (interface{}, error)
	r  chan doRet
}

type doRet struct {
	i interface{}
	e error
}

type DB struct {
	db       *leveldb.DB
	requests []chan req
	stop     chan bool
	closed   bool
}

func Wrap(db *leveldb.DB, config *Config) *DB {
	ret := &DB{
		db:       db,
		requests: make([]chan req, 0, config.NumBuckets()),
		stop:     make(chan bool, 1),
	}

	for len(ret.requests) < cap(ret.requests) {
		ret.requests = append(ret.requests, make(chan req, config.BufferSize()))
	}

	for i, _ := range ret.requests {
		go func(idx int) {
			reqs := ret.requests[idx]
			for {
				req := <-reqs
				result, err := req.do(ret.db)
				req.r <- doRet{
					i: result,
					e: err,
				}
			}
		}(i)
	}

	go func() {
		_ = <-ret.stop
		for i, _ := range ret.requests {
			close(ret.requests[i])
		}
	}()

	return ret
}

// TODO: Make sure this is threadsafe
func (db *DB) Close() error {
	db.closed = true
	defer func() {
		db.stop <- true
	}()
	return db.db.Close()
}

func (db *DB) getRange(key []byte) int {
	hasher := fnv.New64()
	hasher.Write(key)
	return int(hasher.Sum64()) % len(db.requests)
}

func (db *DB) Get(key []byte, ro *opt.ReadOptions) (value []byte, err error) {
	if db.closed {
		return nil, fmt.Errorf("Get(): db is already closed")
	}
	idx := db.getRange(key)
	result := make(chan doRet, 1)
	db.requests[idx] <- req{
		r: result,
		do: func(db *leveldb.DB) (interface{}, error) {
			val, er := db.Get(key, ro)
			return interface{}(val), er
		},
	}

	ret := <-result
	value = ret.i.([]byte)
	err = ret.e
	return
}

type Tx struct {
	key string
}
