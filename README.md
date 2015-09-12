# GoLevelKV
An embeddable key-value store built on top of goleveldb to provide compare-and-swap
operations and key-based transactions!

## Description
This is just a wrapper around goleveldb to provide atomic per-key transactions!
This works by just bucketing all the requests into a finite number of goroutines,
and having each goroutine process requests for a certain subset of the key
range. This forces per-key serialization on top of the database, letting us have
transactions.

# TODOS
- Use a cooler system than bucketing, like lockless per-key queues to improve
scalability (and awesomeness)
- Extend the API a little bit possibly
- 100% test coverage
- Benchmark performance
- Build a Raft-distributed layer on top of this (will be in a different repository)
- Allow for more sophisticated operations (serializable transactions covering several keys)
