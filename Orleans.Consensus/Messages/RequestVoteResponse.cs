namespace Orleans.Consensus.Contract.Messages
{
    using System;

    using Orleans.Concurrency;

    [Immutable]
    [Serializable]
    public class RequestVoteResponse : IMessage
    {
        public bool VoteGranted { get; set; }
        public long Term { get; set; }
    }
}