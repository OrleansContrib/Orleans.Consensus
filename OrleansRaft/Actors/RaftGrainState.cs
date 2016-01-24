using Orleans.CodeGeneration;
using Orleans.Raft.Contract.Messages;

[assembly: KnownType(typeof(NotLeaderException))]
namespace OrleansRaft.Actors
{
    using System;

    using Orleans;

    [Serializable]
    public class RaftGrainState : GrainState
    {
        public string VotedFor { get; set; }
        public long CurrentTerm { get; set; }
    }
}
 