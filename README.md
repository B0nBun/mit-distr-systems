# Distributed systems

This repository contains my solutions for programming labs from [6.5840: Distributed Systems (Spring 2023)](https://pdos.csail.mit.edu/6.824/)


> 6.5840 is a core 12-unit graduate subject with lectures, readings, programming labs, an optional project, a mid-term exam, and a final exam. 
> It will present abstractions and implementation techniques for engineering distributed systems. Major topics include fault tolerance, 
> replication, and consistency. Much of the class consists of studying and discussing case studies of distributed systems.

All of the details regarding the building/compilation/running/testing process and the tasks themselves can be found on [the page of the course](https://pdos.csail.mit.edu/6.824/).

**I do not study at MIT** and do these labs purely for my own enjoyment and growth, so there may be some inconsistencies and mistakes that I didn't notice.

## Lab 1: MapReduce ([paper](https://storage.googleapis.com/gweb-research2023-media/pubtools/4449.pdf))

The code is located in: [src/mr/](src/mr/)

For easier debugging and testing, I added an `mr.sh` script. It requires [GNU parallel](https://www.gnu.org/software/parallel/) to work and starts the map-reduce with selected plugin and selected number of workers (picking `src/main/pg-*` as input).
```
Usage: ./mr.sh [plugin] [worker-count]
       - plugin defaults to 'wc'
       - worker-count defaults to 8
       All of the plugins are located in 'src/mrapps'
```
```
./mr.sh grep 1 # Run grep with 1 worker (pattern is specified in src/mrapps/grep.go)
```

## Lab 2: Raft ([paper](https://raft.github.io/raft.pdf))

The code is located in: [src/raft/](src/raft/)

For easier debugging testing, I added an `raft-test.sh` script. It requires [GNU parallel](https://www.gnu.org/software/parallel/) to work. Using environment variables `PATTERN, TIMEOUT, ITERS` it starts up `go test -run "$PATTERN"` `$ITERS` number of times in parallel. Each test suit/job has a timeout of `$TIMEOUT` seconds and logs it's output into `raft/logs/{n}.log` where `n` is a job index. Raft debug logs are controlled with `RAFT_LOGS=true` env variable (The environment variables are hardcoded directly into the script).

All the tests are located in [src/raft/test_test.go](src/raft/test_test.go). They weren't written by me and are a part of the Lab 2 task

Finished implementation passes all of the tests.

## Lab 3: K/V service

> todo i guess
