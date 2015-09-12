package golevelkv

import "bytes"
import "fmt"
import "hash/fnv"

import "github.com/syndtr/goleveldb/leveldb"
import "github.com/syndtr/goleveldb/leveldb/opt"
import "github.com/syndtr/goleveldb/leveldb/util"

const DEFAULT_NUM_BUCKETS = 16
const DEFAULT_BUFFER_SIZE = 8

// The configuration defines how many buckets and how big each bucket's channel
// will be. These affect the overhead and scalability of the database.
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

	for n, _ := range ret.requests {
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
		}(n)
	}

	go func() {
		_ = <-ret.stop
		for i, _ := range ret.requests {
			close(ret.requests[i])
		}
	}()

	return ret
}

func (db *DB) getRange(key []byte) int {
	hasher := fnv.New64()
	hasher.Write(key)
	return int(uint(hasher.Sum64()) % uint(len(db.requests)))
}

// TODO: Make sure this is threadsafe
func (db *DB) Close() error {
	db.closed = true
	defer func() {
		db.stop <- true
	}()
	return db.db.Close()
}

func (db *DB) wrapOperation(key []byte, f func(db *leveldb.DB) (interface{}, error)) doRet {
	idx := db.getRange(key)
	result := make(chan doRet, 1)
	db.requests[idx] <- req{
		r:  result,
		do: f,
	}
	ret := <-result
	return ret
}

func (db *DB) Get(key []byte, ro *opt.ReadOptions) (value []byte, err error) {
	if db.closed {
		return nil, fmt.Errorf("Get(): db is already closed")
	}
	ret := db.wrapOperation(key, func(db *leveldb.DB) (interface{}, error) {
		val, er := db.Get(key, ro)
		return interface{}(val), er
	})

	value = ret.i.([]byte)
	err = ret.e
	return
}

func (db *DB) Delete(key []byte, wo *opt.WriteOptions) error {
	if db.closed {
		return fmt.Errorf("Get(): db is already closed")
	}
	ret := db.wrapOperation(key, func(db *leveldb.DB) (interface{}, error) {
		er := db.Delete(key, wo)
		return nil, er
	})

	return ret.e
}

func (db *DB) Has(key []byte, ro *opt.ReadOptions) (ret bool, err error) {
	if db.closed {
		return false, fmt.Errorf("Has(): db is already closed")
	}
	r := db.wrapOperation(key, func(db *leveldb.DB) (interface{}, error) {
		bl, er := db.Has(key, ro)
		return interface{}(bl), er
	})

	ret = r.i.(bool)
	err = r.e
	return
}

func (db *DB) Put(key, value []byte, wo *opt.WriteOptions) error {
	if db.closed {
		return fmt.Errorf("Has(): db is already closed")
	}
	ret := db.wrapOperation(key, func(db *leveldb.DB) (interface{}, error) {
		er := db.Put(key, value, wo)
		return nil, er
	})

	return ret.e
}

func (db *DB) Cas(key, value []byte, old []byte, ro *opt.ReadOptions, wo *opt.WriteOptions) (swapped bool, err error) {
	if db.closed {
		return false, fmt.Errorf("Cas(): db is already closed")
	}
	ret := db.wrapOperation(key, func(db *leveldb.DB) (interface{}, error) {
		val, e := db.Get(key, ro)
		if e != nil {
			return false, e
		}
		if bytes.Equal(val, old) {
			if er := db.Put(key, value, wo); er != nil {
				return interface{}(false), er
			}
			return true, nil
		} else {
			return false, nil
		}
	})

	swapped = ret.i.(bool)
	err = ret.e
	return
}

func (db *DB) Cas2(key, value []byte, old []byte, ro *opt.ReadOptions, wo *opt.WriteOptions) (retStr []byte, swapped bool, err error) {
	type cas2ret struct {
		bs []byte
		b  bool
	}
	if db.closed {
		return nil, false, fmt.Errorf("Cas2(): db is already closed")
	}
	ret := db.wrapOperation(key, func(d *leveldb.DB) (interface{}, error) {
		val, e := d.Get(key, ro)
		if e != nil {
			return val, e
		}
		if bytes.Equal(val, old) {
			if er := d.Put(key, value, wo); er != nil {
				return &cas2ret{val, false}, er
			}
			return &cas2ret{value, true}, nil
		} else {
			return &cas2ret{val, false}, nil
		}
	})

	r := ret.i.(*cas2ret)
	retStr = r.bs
	swapped = r.b
	err = ret.e
	return
}

type Tx struct {
	key []byte
	db  *leveldb.DB
}

func (tx *Tx) Get(ro *opt.ReadOptions) ([]byte, error) {
	return tx.db.Get(tx.key, ro)
}

func (tx *Tx) Set(value []byte, wo *opt.WriteOptions) error {
	return tx.db.Put(tx.key, value, wo)
}

func (tx *Tx) Has(ro *opt.ReadOptions) (bool, error) {
	return tx.db.Has(tx.key, ro)
}

func (tx *Tx) Delete(wo *opt.WriteOptions) error {
	return tx.db.Delete(tx.key, wo)
}

func (db *DB) Do(key []byte, f func(*Tx) (interface{}, error)) (interface{}, error) {
	ret := db.wrapOperation(key, func(levelDb *leveldb.DB) (interface{}, error) {
		tx := Tx{
			key: key,
			db:  levelDb,
		}
		return f(&tx)
	})
	return ret.i, ret.e
}

// Wrappers for all the functions at a normal DB has (and that we can guarantee
// to be safe to run)
func (db *DB) CompactRange(r util.Range) error {
	return db.db.CompactRange(r)
}

func (db *DB) GetProperty(name string) (string, error) {
	return db.GetProperty(name)
}

func (db *DB) GetSnapshot() (*leveldb.Snapshot, error) {
	return db.GetSnapshot()
}

func (db *DB) SetReadOnly() error {
	return db.SetReadOnly()
}

func (db *DB) SizeOf(ranges []util.Range) (leveldb.Sizes, error) {
	return db.SizeOf(ranges)
}
