namespace Orleans.Consensus
{
    public interface ISettings {
        int MinElectionTimeoutMilliseconds { get; }
        int MaxElectionTimeoutMilliseconds { get; }
        int HeartbeatTimeoutMilliseconds { get; }

        /// <summary>
        /// The maximum number of log entries which will be included in an append request.
        /// </summary>
        int MaxLogEntriesPerAppendRequest { get; }

        /// <summary>
        /// Gets a value indicating whether or not committed operations should be applied to the state machine on a
        /// server which is currently a follower.
        /// </summary>
        bool ApplyEntriesOnFollowers { get; }
    }
}