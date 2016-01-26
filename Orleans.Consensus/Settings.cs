namespace Orleans.Consensus.Actors
{
    internal static class Settings
    {
        // TODO: Use less insanely high values.

        public const int MinElectionTimeoutMilliseconds = 600;

        public const int MaxElectionTimeoutMilliseconds = 2 * MinElectionTimeoutMilliseconds;

        public const int HeartbeatTimeoutMilliseconds = MinElectionTimeoutMilliseconds / 3;

        /// <summary>
        /// The maximum number of log entries which will be included in an append request.
        /// </summary>
        public const int MaxLogEntriesPerAppendRequest = 10;

        /// <summary>
        /// Gets a value indicating whether or not committed operations should be applied to the state machine on a
        /// server which is currently a follower.
        /// </summary>
        public static bool ApplyEntriesOnFollowers { get; } = false;
    }
}