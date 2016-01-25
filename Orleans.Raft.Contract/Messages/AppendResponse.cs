namespace Orleans.Raft.Contract.Messages
{
    using System;

    using Orleans.Concurrency;

    [Immutable]
    [Serializable]
    public class AppendResponse : IMessage
    {
        public long Term { get; set; }
        public bool Success { get; set; }
    }
}