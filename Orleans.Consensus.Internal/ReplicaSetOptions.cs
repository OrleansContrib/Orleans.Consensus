namespace Orleans.Consensus
{
    public class ReplicaSetOptions
    {
        // TODO: Use less insanely high values.

        /// <summary>
        /// The minimum election timeout.
        /// </summary>
        /// <remarks>Each replica set uses an election timer which is set to a value between
        /// <see cref="MinElectionTimeoutMilliseconds"/> and <see cref="MaxElectionTimeoutMilliseconds"/>.</remarks>
        public int MinElectionTimeoutMilliseconds { get; set; } = 600;

        /// <summary>
        /// The maximum election timeout.
        /// </summary>
        /// <remarks>Each replica set uses an election timer which is set to a value between
        /// <see cref="MinElectionTimeoutMilliseconds"/> and <see cref="MaxElectionTimeoutMilliseconds"/>.</remarks>
        public int MaxElectionTimeoutMilliseconds { get; set; } = 1200;

        /// <summary>
        /// The duration between heartbeats from leaders to followers.
        /// </summary>
        public int HeartbeatTimeoutMilliseconds { get; set; } = 200;
        
        /// <summary>
        /// The maximum number of log entries which will be included in an append request.
        /// </summary>
        public int MaxLogEntriesPerAppendRequest { get; set; } = 10;

        /// <summary>
        /// Gets a value indicating whether or not committed operations should be applied to the state machine on a
        /// server which is currently a follower.
        /// </summary>
        public bool ApplyEntriesOnFollowers { get; set; } = false;
    }
}