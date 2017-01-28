using Orleans.Consensus.Contract.Log;

namespace Orleans.Consensus.State
{
    using System.Threading.Tasks;

    public interface IRaftPersistentState
    {
        string VotedFor { get; }
        long CurrentTerm { get; }

        Task UpdateTermAndVote(string votedFor, long currentTerm);
    }
}