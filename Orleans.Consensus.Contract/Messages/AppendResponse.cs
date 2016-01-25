namespace Orleans.Consensus.Contract.Messages
{
    using System;

    using Orleans.Concurrency;
    using Orleans.Consensus.Contract.Log;

    [Immutable]
    [Serializable]
    public class AppendResponse : IMessage
    {
        public long Term { get; set; }
        public bool Success { get; set; }

        public LogEntryId LastLogEntryId { get; set; }
    }
}