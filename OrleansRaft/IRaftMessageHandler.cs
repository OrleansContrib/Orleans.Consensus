namespace OrleansRaft
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    using OrleansRaft.Messages;

    public interface IRaftMessageHandler<TOperation>
    {
        string State { get; }
        Task Enter();

        Task Exit();

        Task<RequestVoteResponse> RequestVote(RequestVoteRequest request);
        Task<AppendResponse> Append(AppendRequest<TOperation> request);

        Task ReplicateAndApplyEntries(List<TOperation> entries);
    }
}