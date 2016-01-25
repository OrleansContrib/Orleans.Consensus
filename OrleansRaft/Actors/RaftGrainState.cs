using Orleans.CodeGeneration;
using Orleans.Raft.Contract.Messages;

[assembly: KnownType(typeof(NotLeaderException))]
namespace OrleansRaft.Actors
{
    using System;

    using Orleans;
    using Orleans.Raft.Contract.Log;

    [Serializable]
    public class RaftGrainState<TOperation> : GrainState
    {
        public string VotedFor { get; set; }
        public long CurrentTerm { get; set; }
        public InMemoryLog<TOperation> Log { get; set; } = new InMemoryLog<TOperation>();
    }
}
 