namespace Orleans.Consensus.Contract.Messages
{
    using System;

    using Orleans.Concurrency;

    [Immutable]
    [Serializable]
    public class RequestVoteResponse : IMessage
    {
        public long Term { get; set; }
        public bool VoteGranted { get; set; }
    }
}