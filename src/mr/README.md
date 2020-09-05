### This is a distributed version of map reduce.
- The master and worker accepts the map and reduce function that are under `../mrapps`
- The worker will create one mapper for every file and an intermediate file for the reduce task
- Once the map phase is completed the master will assign reduce tasks to the worker
- The number of reduce task is configured in `mrmaster.go`

### The sequence of execution is as follows:
- Build the user map reduce function, for ex., word-count problem:
```
go build -buildmode=plugin ../mrapps/wc.go
```
- `mrmaster.go` calls `master.go` and it starts a RPC listener `mr-socket`
```
go run mrmaster.go pg-*.txt
```
- `master` initializes the map tasks which are equal to the number of input files
- Now, start the multiple `worker`(s) in different terminal
```
go build -buildmode=plugin ../mrapps/wc.go
go run mrworker.go wc.so
```
- The `worker` when started registers itself with `master` using RPC and get a `worker-id` in response
- The `worker` requests one task at a time from the master using a RPC and executes either the map function or the reduce function depending on if the state `master` is `MAP_PHASE` or `REDUCE_PHASE`
- The `master` remains in the `MAP_PHASE` until all the map tasks are complete. When the `MAP_PHASE` are completed the `master` state changes to `REDUCE_PHASE`.
- In the `MAP_PHASE`, `worker` generates an intermediate files for every mapper and stores in the file system (distributed file system HDFS) for the `reduce` task.
- In the `REDUCE_PHASE`, `worker` reads the intermediate mapper output and depending on the configuration of the number of mapper it generates that many number of output files by applying the `reduce` function.

#### Learning: This is my first go-lang application

##### Errors faced:
- Error while making an RPC using a non-pointer: [Go RPC Error: reading body gob: attempt to decode into a non-pointer](https://stackoverflow.com/questions/28686307/go-rpc-error-reading-body-gob-attempt-to-decode-into-a-non-pointer)
- Exported and unexported variables in Golang [mr.workerId has no exported fields](https://stackoverflow.com/questions/40256161/exported-and-unexported-fields-in-go-language)
- [Declared but not used](https://stackoverflow.com/questions/21743841/how-to-avoid-annoying-error-declared-and-not-used)
- [Function vs method in Golang syntax](https://stackoverflow.com/questions/45641999/parameter-before-the-function-name-in-go)
- https://stackoverflow.com/questions/34031801/function-declaration-syntax-things-in-parenthesis-before-function-name/34350336
- Setting of `GOPATH` [Cannot resolve file “fmt”](https://stackoverflow.com/questions/50008856/idea-display-cannot-resolve-file-fmt)
- [Importing local package](https://stackoverflow.com/questions/35480623/how-to-import-local-packages-in-go)
