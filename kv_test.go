package levelkv

import "bytes"
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
	testKey, testVal := []byte("bananas"), []byte("monkeys")
	putErr := db.Put(testKey, testVal, nil)
	if putErr != nil {
		t.Fatalf("Database put failed: %v\n", putErr)
	}

	getVal, getErr := db.Get(testKey, nil)
	if getErr != nil {
		t.Fatalf("Database get failed: %v\n", getErr)
	}
	if bytes.Compare(getVal, testVal) != 0 {
		t.Errorf("Database get received %v instead of %v\n", getVal, testVal)
	}
}

func TestPutGetPutGet(t *testing.T) {
	testKey, testVal := []byte("apples"), []byte("bananas")
	putErr := db.Put(testKey, testVal, nil)
	if putErr != nil {
		t.Fatalf("Database put failed: %v\n", putErr)
	}

	getVal, getErr := db.Get(testKey, nil)
	if getErr != nil {
		t.Fatalf("Database get failed: %v\n", getErr)
	}
	if bytes.Compare(getVal, testVal) != 0 {
		t.Errorf("Database get received %v instead of %v\n", getVal, testVal)
	}

	testVal2 := []byte("oranges")
	putErr2 := db.Put(testKey, testVal2, nil)
	if putErr2 != nil {
		t.Fatalf("Database put failed: %v\n", putErr2)
	}

	getVal2, getErr2 := db.Get(testKey, nil)
	if getErr2 != nil {
		t.Fatalf("Database get failed: %v\n", getErr2)
	}
	if bytes.Compare(getVal2, testVal2) != 0 {
		t.Errorf("Database get received %v instead of %v\n", getVal2, testVal2)
	}
}

func TestMultiPutGet(t *testing.T) {
	testKey, testVal := []byte("apples"), []byte("bananas")
	testVal2 := []byte("oranges")

	// Interlace requests between the two go routines
	finished := make(chan bool, 2)
	go func() {
		for i := 0; i < 1000; i++ {
			putErr := db.Put(testKey, testVal, nil)
			if putErr != nil {
				t.Fatalf("Database put failed: %v\n", putErr)
			}

			getVal, getErr := db.Get(testKey, nil)
			if getErr != nil {
				t.Fatalf("Database get failed: %v\n", getErr)
			}
			if bytes.Compare(getVal, testVal) != 0 && bytes.Compare(getVal, testVal2) != 0 {
				t.Errorf("Database get received %v instead of %v\n", getVal, testVal)
			}
		}
		finished <- true
	}()

	go func() {
		for i := 0; i < 1000; i++ {
			putErr2 := db.Put(testKey, testVal2, nil)
			if putErr2 != nil {
				t.Fatalf("Database put failed: %v\n", putErr2)
			}

			getVal2, getErr2 := db.Get(testKey, nil)
			if getErr2 != nil {
				t.Fatalf("Database get failed: %v\n", getErr2)
			}
			if bytes.Compare(getVal2, testVal2) != 0 && bytes.Compare(getVal2, testVal) != 0 {
				t.Errorf("Database get received %v instead of %v\n", getVal2, testVal2)
			}
		}
		finished <- true
	}()

	_ = <-finished
	_ = <-finished
}
