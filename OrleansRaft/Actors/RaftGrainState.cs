using Orleans.CodeGeneration;

using OrleansRaft;
using OrleansRaft.Messages;

[assembly: KnownType(typeof(NotLeaderException))]
namespace OrleansRaft.Actors
{
    using System;

    using Orleans;

    using OrleansRaft.Log;

    [Serializable]
    public class RaftGrainState<TOperation> : GrainState
    {
        public string VotedFor { get; set; }
        public long CurrentTerm { get; set; }
        public Log<TOperation> Log { get; set; } = new Log<TOperation>();
    }
}
 