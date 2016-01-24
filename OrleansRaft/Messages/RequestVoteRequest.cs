namespace OrleansRaft.Messages
{
    using System;

    using OrleansRaft.Log;

    [Serializable]
    public class RequestVoteRequest : IMessage
    {
        public long Term { get; set; }
        public string Candidate { get; set; }
        public LogEntryId LastLogEntryId { get; set; }
    }
}