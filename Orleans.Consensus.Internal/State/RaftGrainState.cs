namespace Orleans.Consensus.State
{
    using System;

    using Orleans.Consensus.Log;

    [Serializable]
    public class RaftGrainState<TOperation>
    {
        public string VotedFor { get; set; }
        public long CurrentTerm { get; set; }
        public InMemoryLog<TOperation> Log { get; set; } = new InMemoryLog<TOperation>();
    }
}