using Orleans.CodeGeneration;
using Orleans.Consensus.Contract.Messages;

[assembly: KnownType(typeof(NotLeaderException))]
namespace Orleans.Consensus.Actors
{
    using System;

    using Orleans;
    using Orleans.Consensus.Contract.Log;
    using Orleans.Consensus.Log;

    [Serializable]
    public class RaftGrainState<TOperation> : GrainState
    {
        public string VotedFor { get; set; }
        public long CurrentTerm { get; set; }
        public InMemoryLog<TOperation> Log { get; set; } = new InMemoryLog<TOperation>();
    }
}
 