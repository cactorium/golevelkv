package golevelkv

import "sync/atomic"
import "bytes"
import "fmt"
import "os"
import "strconv"
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

func TestPutHasDelete(t *testing.T) {
	testKey, testVal := []byte("bananas"), []byte("monkeys")
	putErr := db.Put(testKey, testVal, nil)
	if putErr != nil {
		t.Fatalf("Database put failed: %v\n", putErr)
	}

	has, hasErr := db.Has(testKey, nil)
	if hasErr != nil {
		t.Fatalf("Database has failed: %v\n", hasErr)
	}
	if !has {
		t.Fatal("Database does not have a value for a key it just stored")
	}

	delErr := db.Delete(testKey, nil)
	if delErr != nil {
		t.Fatalf("Database delete failed: %v\n", delErr)
	}

	has2, hasErr2 := db.Has(testKey, nil)
	if hasErr2 != nil {
		t.Fatalf("Database has failed: %v\n", hasErr2)
	}
	if has2 {
		t.Fatal("Database has a value for a key it just deleted")
	}
}

func TestCas(t *testing.T) {
	testKey, testVal := []byte("apples2"), []byte("bananas")
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
	casSuccess, casErr := db.Cas(testKey, testVal2, testVal, nil, nil)
	if casErr != nil {
		t.Fatalf("Database cas failed: %v\n", casErr)
	}
	if !casSuccess {
		t.Fatalf("Database cas failed to swap")
	}

	getVal2, getErr2 := db.Get(testKey, nil)
	if getErr2 != nil {
		t.Fatalf("Database get failed: %v\n", getErr2)
	}
	if bytes.Compare(getVal2, testVal2) != 0 {
		t.Errorf("Database get received %v instead of %v\n", getVal2, testVal2)
	}

	casSuccess2, casErr2 := db.Cas(testKey, testVal2, testVal, nil, nil)
	if casErr2 != nil {
		t.Fatalf("Database cas failed: %v\n", casErr2)
	}
	if casSuccess2 {
		t.Fatalf("Database cas should have failed")
	}

}

func TestCas2(t *testing.T) {
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
	cas, casb, casErr := db.Cas2(testKey, testVal2, testVal, nil, nil)
	if casErr != nil {
		t.Fatalf("Database cas failed: %v\n", casErr)
	}
	if bytes.Compare(cas, testVal) == 0 {
		t.Fatalf("Database cas failed to swap")
	}
	if !casb {
		t.Fatalf("Database cas failed to swap")
	}

	getVal2, getErr2 := db.Get(testKey, nil)
	if getErr2 != nil {
		t.Fatalf("Database get failed: %v\n", getErr2)
	}
	if bytes.Compare(getVal2, testVal2) != 0 {
		t.Errorf("Database get received %v instead of %v\n", getVal2, testVal2)
	}

	cas2, cas2b, casErr2 := db.Cas2(testKey, testVal2, testVal, nil, nil)
	if casErr2 != nil {
		t.Fatalf("Database cas failed: %v\n", casErr2)
	}
	if bytes.Compare(cas2, testVal2) != 0 {
		t.Fatalf("Database cas should have failed")
	}
	if cas2b {
		t.Fatalf("Database cas should have failed")
	}

	getVal3, getErr3 := db.Get(testKey, nil)
	if getErr3 != nil {
		t.Fatalf("Database get failed: %v\n", getErr3)
	}
	if bytes.Compare(getVal3, testVal2) != 0 {
		t.Errorf("Database get received %v instead of %v\n", getVal3, testVal2)
	}
}

func TestMultiCas2(t *testing.T) {
	testKey, testVal := []byte("apples"), []byte(strconv.Itoa(0))
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

	ops := uint64(0)
	nRoutines := 4
	nIters := 1000
	finished := make(chan bool, nRoutines)
	for r := 0; r < nRoutines; r++ {
		go func(n int) {
			defer func() { finished <- true }()
			lastSeenVal := 0

			for i := 0; i < nIters; i++ {
				success := false
				for !success {
					id := atomic.AddUint64(&ops, 1)
					oldVal := []byte(strconv.Itoa(lastSeenVal))
					newVal := []byte(strconv.Itoa(lastSeenVal + 1))
					cas, casSuccess, casErr := db.Cas2(testKey, newVal, oldVal, nil, nil)
					if casErr != nil {
						t.Fatalf("Database cas failed: %v\n", casErr)
						return
					}
					if casSuccess {
						t.Logf("%v,%v,%v: %v => %v\n", n, i, id, string(oldVal), string(newVal))
						success = true
					} else {
						t.Logf("%v,%v,%v: CAS failed", n, i, id)
					}
					newLastVal, convErr := strconv.Atoi(string(cas))
					if convErr != nil {
						t.Fatalf("Database value could not be read: %v\n", convErr)
						return
					}
					lastSeenVal = newLastVal
				}
			}
		}(r)
	}

	for r := 0; r < nRoutines; r++ {
		_ = <-finished
	}

	getVal2, getErr2 := db.Get(testKey, nil)
	expected := []byte(strconv.Itoa(nRoutines * nIters))
	if getErr2 != nil {
		t.Fatalf("Database get failed: %v\n", getErr2)
	}
	if bytes.Compare(getVal2, expected) != 0 {
		t.Errorf("Database get received %v instead of %v\n", string(getVal2), string(expected))
	}
}

func TestTx(t *testing.T) {
	testKey, testVal := []byte("apples"), []byte("bananas")
	testVal2 := []byte("oranges")
	putErr := db.Put(testKey, testVal, nil)
	if putErr != nil {
		t.Fatalf("Database put failed: %v\n", putErr)
	}

	txVal, txErr := db.Do(testKey, func(tx *Tx) (interface{}, error) {
		ret, e := tx.Get(nil)
		if e != nil {
			return nil, e
		}
		if has, e2 := tx.Has(nil); e2 != nil || !has {
			if e2 != nil {
				return nil, e2
			} else {
				return nil, fmt.Errorf("Database has returned false when true was expected\n")
			}
		}
		if e3 := tx.Delete(nil); e3 != nil {
			return nil, e3
		}
		if has, e4 := tx.Has(nil); e4 != nil || has {
			if e4 != nil {
				return nil, e4
			} else {
				return nil, fmt.Errorf("Database has returned true when false was expected\n")
			}
		}
		if e5 := tx.Set(testVal2, nil); e5 != nil {
			return nil, e5
		}
		return ret, nil
	})

	if txErr != nil {
		t.Fatalf("Database get failed: %v\n", txErr)
	}
	if bytes.Compare(txVal.([]byte), testVal) != 0 {
		t.Errorf("Database get received %v instead of %v\n", txVal, testVal)
	}

	getVal2, getErr2 := db.Get(testKey, nil)
	if getErr2 != nil {
		t.Fatalf("Database get failed: %v\n", getErr2)
	}
	if bytes.Compare(getVal2, testVal2) != 0 {
		t.Errorf("Database get received %v instead of %v\n", getVal2, testVal2)
	}
}
