using Orleans;
using Orleans.Placement;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrleansRaft
{
    public interface IRaftGrain : IGrainWithStringKey
    {
        Task<RequestVoteResponse> RequestVote(RequestVoteRequest request);
        Task<AppendResponse> Append(AppendRequest request);
    }
}
