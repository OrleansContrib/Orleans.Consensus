namespace Orleans.Consensus
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    using Orleans.Consensus.Contract.Messages;

    public interface IRaftRole<TOperation>
    {
        string State { get; }
        Task Enter();

        Task Exit();

        Task<RequestVoteResponse> RequestVote(RequestVoteRequest request);
        Task<AppendResponse> Append(AppendRequest<TOperation> request);

        Task ReplicateAndApplyEntries(List<TOperation> entries);
    }
}