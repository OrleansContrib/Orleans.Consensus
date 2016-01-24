namespace Orleans.Raft.Contract
{
    using System.Threading.Tasks;

    using Orleans;
    using Orleans.Raft.Contract.Messages;

    public interface IRaftGrain<TOperation> : IGrainWithStringKey
    {
        Task<RequestVoteResponse> RequestVote(RequestVoteRequest request);
        Task<AppendResponse> Append(AppendRequest<TOperation> request);
    }
}
