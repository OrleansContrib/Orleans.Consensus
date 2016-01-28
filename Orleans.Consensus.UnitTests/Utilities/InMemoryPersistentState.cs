using System.Threading.Tasks;

namespace Orleans.Consensus.UnitTests.Utilities
{
    using Orleans.Consensus.State;
    public class InMemoryPersistentState : IRaftPersistentState
    {
        public virtual string VotedFor { get; set; }
        public virtual long CurrentTerm { get; set; }

        public virtual Task UpdateTermAndVote(string votedFor, long currentTerm)
        {
            this.VotedFor = votedFor;
            this.CurrentTerm = currentTerm;
            return Task.FromResult(0);
        }
    }
}
