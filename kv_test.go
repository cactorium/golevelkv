package levelkv

import "fmt"
import "os"
import "testing"

import "github.com/syndtr/goleveldb/leveldb"
import "github.com/syndtr/goleveldb/leveldb/storage"

var db *DB

func TestMain(m *testing.M) {
	ldb, err := leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		fmt.Printf("Unable to create levelDB database: %v\n", err)
		os.Exit(-1)
	}
	db = Wrap(ldb, NewConfig())

	r := m.Run()
	db.Close()
	os.Exit(r)
}

func TestPutGet(t *testing.T) {
}
