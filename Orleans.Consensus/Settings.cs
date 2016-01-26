namespace Orleans.Consensus
{
    internal class Settings : ISettings
    {
        // TODO: Use less insanely high values.

        public int MinElectionTimeoutMilliseconds { get; } = 600;

        public int MaxElectionTimeoutMilliseconds => 2 * this.MinElectionTimeoutMilliseconds;

        public int HeartbeatTimeoutMilliseconds => this.MinElectionTimeoutMilliseconds / 3;

        /// <summary>
        /// The maximum number of log entries which will be included in an append request.
        /// </summary>
        public int MaxLogEntriesPerAppendRequest { get; } = 10;

        /// <summary>
        /// Gets a value indicating whether or not committed operations should be applied to the state machine on a
        /// server which is currently a follower.
        /// </summary>
        public bool ApplyEntriesOnFollowers { get; } = false;
    }
}