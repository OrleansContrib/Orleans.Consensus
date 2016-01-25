namespace Orleans.Consensus.Contract
{
    using System.Threading.Tasks;

    using Orleans;
    using Orleans.Consensus.Contract.Messages;

    public interface IRaftGrain<TOperation> : IGrainWithStringKey
    {
        Task<RequestVoteResponse> RequestVote(RequestVoteRequest request);
        Task<AppendResponse> Append(AppendRequest<TOperation> request);
    }
}
