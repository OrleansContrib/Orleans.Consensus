namespace Orleans.Raft.Contract.Messages
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    using Orleans.Concurrency;
    using Orleans.Raft.Contract.Log;

    [Immutable]
    [Serializable]
    public class AppendRequest<TOperation> : IMessage
    {
        public long Term { get; set; }
        public string Leader { get; set; }
        public LogEntryId PreviousLogEntry { get; set; }

        /// <summary>
        /// Empty for heartbeat
        /// </summary>
        public List<LogEntry<TOperation>> Entries { get; set; }
        public long LeaderCommitIndex { get; set; }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        public override string ToString()
        {
            return $"Append(Leader: {this.Leader}, CommitIndex: {this.LeaderCommitIndex},"
                   + $" Previous: {this.PreviousLogEntry}, Term: {this.Term},"
                   + $" Entries: {string.Join(", ", this.Entries?.Select(_ => _.Id) ?? Enumerable.Empty<LogEntryId>())})";
        }
    }
}
