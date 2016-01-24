namespace OrleansRaft.Actors
{
    using System.Threading.Tasks;

    using Orleans;

    using OrleansRaft.Messages;

    public interface IRaftGrain<TOperation> : IGrainWithStringKey
    {
        Task<RequestVoteResponse> RequestVote(RequestVoteRequest request);
        Task<AppendResponse> Append(AppendRequest<TOperation> request);
    }
}
