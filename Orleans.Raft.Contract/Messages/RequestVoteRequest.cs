namespace Orleans.Raft.Contract.Messages
{
    using System;

    using Orleans.Concurrency;
    using Orleans.Raft.Contract.Log;

    [Immutable]
    [Serializable]
    public class RequestVoteRequest : IMessage
    {
        public long Term { get; set; }
        public string Candidate { get; set; }
        public LogEntryId LastLogEntryId { get; set; }
    }
}