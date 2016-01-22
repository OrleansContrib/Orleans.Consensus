# OrleansRaft

A Raft implementation using Orleans Grains. Just for fun!

> DO NOT USE

## Usage

run the `test.cmd` in the `bin\debug` directory. This will start a three silo cluster.

## How it works

* A bootstrap provider registers a grain at silo startup. The Silo's Identity is used as the grain's key.
* The grain uses the `IManagementGrain` to get a list of silos in the cluster (thereby the identities of the other grains).
* The grain uses the Raft consensus algorithm to elect a leader amongst the grains.

## TODO

* Allow a user then to exploit the leader, by hooking in to election events, thus allowing a singleton in the cluster.
* Work out how changes in cluster membership change the election process.
* Lots of testing.

## License

MIT