namespace OrleansRaft.Messages
{
    using System;
    using System.Collections.Generic;

    using OrleansRaft.Log;

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
    }
}
