This is just a wrapper around goleveldb to provide atomic per-key transactions! This
works by just funnelling all the requests into a finite number of goroutines,
and having each goroutine process requests for a certain subset of the key
range. This forces per-key serialization on top of the database, letting us have
transactions.
