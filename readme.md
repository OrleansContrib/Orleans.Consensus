# Orleans.Raft

Raft implementation using [Orleans](https://github.com/dotnet/orleans).

> Incomplete proof-of-concept

## Usage

Open project in VS, hit F5, watch the debug output.

## TODO

* Extensive automated testing.
* Use Orleans to provide cluster membership.
* Implement Joint-Consensus for cluster membership change from the Raft paper.
* Implement a physical log (it's all in-memory at the moment) - and make sure it's cross-platform! Probably have one physical log per cluster based on Sqlite or another xplat embeddable db (suggestions?).
* Implement log compaction.
* Extend the Raft log repair algorithm (where leaders fix followers' logs) so that the client returns a Progress Vector on failure instead of just false. Then use that progress vector on the leader to select the best position in the client's log to update `nextIndex` to.
* Implement non-voting followers ("listeners" ?). This allows for new servers to be added without decreasing the ratio of up-to-date followers and lagging followers. Lagging followers slow down the commit process without significantly increasing fault tolerance: they are counted in the quorum and yet do not have current log entries yet and so a higher proportion of the up-to-date followers need to acknowledge each new log entry in order for it to be committed. Adding too many lagging replicas too quickly would cause the cluster to stall while they are brought up-to-date. 

## License

MIT