using Orleans;
using Orleans.Concurrency;
using Orleans.Placement;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrleansRaft
{

    [Reentrant]
    [PreferLocalPlacement]
    public class RaftGrain : Grain, IRaftGrain
    {
        Random rand;

        IDisposable electionTimeoutCancellation;
        IDisposable leaderHeatbeatCancellation;
        NodeState state;
        int totalNodes;
        string nodeName;
        string[] otherNodes;
        int term;
        string votedFor = null;
        string leader = null;


        // this is based on the dodgey assumption that nodes will be names 1/3 2/3 & 3/3
        async Task CaptureClusterMembership()
        {
            nodeName = this.GetPrimaryKeyString();
            var managementGrain = this.GrainFactory.GetGrain<IManagementGrain>(0);
            var hosts = await managementGrain.GetHosts();
            totalNodes = hosts.Count;
            otherNodes = hosts.Select(x => x.Key.ToString()).Where(x => x != nodeName).ToArray();
        }


        public override async Task OnActivateAsync()
        {
            Console.WriteLine($"{this.GetPrimaryKeyString()} activating");

            rand = new Random();
            state = NodeState.Follower;

            if (this.GetPrimaryKeyString() != this.RuntimeIdentity)
            {
                Console.WriteLine($"{this.GetPrimaryKeyString()} deactivating, grain is not hosted in the correct silo");
                this.DeactivateOnIdle();
                await base.OnActivateAsync();
                return;
            }

            ResetElectionTimeout();
            await base.OnActivateAsync();
        }


        Task ElectionTimeoutElapsed(object _)
        {
            this.electionTimeoutCancellation.Dispose();
            this.electionTimeoutCancellation = null;
            if (this.state == NodeState.Follower) return StandForElection();
            return TaskDone.Done;
        }

        async Task StandForElection()
        {
            Console.WriteLine($"{nodeName} standing for election");
            await CaptureClusterMembership();
            if (this.totalNodes < 3)
            {
                // not enough nodes for a vote
                Console.WriteLine($"insufficient number of nodes to form a cluster ({totalNodes}). A minimum of 3 is required.");
                ResetElectionTimeout();
                return;
            }

            this.state = NodeState.Candidate;
            this.term++;
            var promises = new List<Task<RequestVoteResponse>>();
            this.votedFor = this.nodeName;
            foreach (var node in this.otherNodes)
            {
                var nodeRef = GrainFactory.GetGrain<IRaftGrain>(node);
                var task = nodeRef.RequestVote(new RequestVoteRequest
                {
                    Candidate = this.nodeName,
                    Term = this.term
                });
                promises.Add(task);
            }

            await Task.WhenAll(promises);

            if (promises.Where(x => x.Result != null).Any(x => x.Result.Term > this.term))
            {
                Demote(promises.Max(x => x.Result.Term));
                return;
            }

            if (this.state != NodeState.Candidate)
            {
                return;
            }

            if (promises.Where(x => x.Result != null).Count(x => x.Result.VoteGranted) + 1 > this.totalNodes / 2)
            {
                Console.WriteLine($"{nodeName} elected as leader with {promises.Where(x => x.Result != null).Count(x => x.Result.VoteGranted)} votes from {this.totalNodes}");
                this.state = NodeState.Leader;
                leaderHeatbeatCancellation = this.RegisterTimer(this.SendAppend, null, TimeSpan.Zero, TimeSpan.FromMilliseconds(30));
            }
            else
            {
                this.state = NodeState.Follower;
                ResetElectionTimeout();
            }

        }

        async Task SendAppend(object _)
        {
            var promises = new List<Task<AppendResponse>>();
            foreach (var node in this.otherNodes)
            {
                var nodeRef = GrainFactory.GetGrain<IRaftGrain>(node);
                var task = nodeRef.Append(new AppendRequest
                {
                    Leader = this.nodeName,
                    Term = this.term
                });
                promises.Add(task);
            }

            await Task.WhenAll(promises);

            if (promises.Where(x => x.Result != null).Any(x => x.Result.Term > this.term))
            {
                Demote(promises.Where(x => x.Result != null).Max(x => x.Result.Term));
            }

        }

        void ResetElectionTimeout()
        {
            if (this.electionTimeoutCancellation != null) this.electionTimeoutCancellation.Dispose();
            this.electionTimeoutCancellation = this.RegisterTimer(this.ElectionTimeoutElapsed, null, TimeSpan.FromMilliseconds(rand.Next(150, 300)), TimeSpan.MaxValue);
        }


        public Task<AppendResponse> Append(AppendRequest request)
        {
            if (request == null) return Task.FromResult(new AppendResponse
            {
                Success = false,
                Term = this.term
            });

            leader = request.Leader;
            ResetElectionTimeout();

            if (request.Term > this.term)
            {
                Demote(request.Term);
            }

            if (request.Term < this.term)
            {
                return Task.FromResult(new AppendResponse
                {
                    Success = false,
                    Term = this.term    
                });
            }

            return Task.FromResult(new AppendResponse
            {
                Success = true,
                Term = this.term
            });

        }

        void Demote(int newTerm)
        {
            this.term = newTerm;
            this.state = NodeState.Follower;
            this.votedFor = null;
            if (this.leaderHeatbeatCancellation != null) leaderHeatbeatCancellation.Dispose();
            leaderHeatbeatCancellation = null;
            ResetElectionTimeout();
        }

        public Task<RequestVoteResponse> RequestVote(RequestVoteRequest request)
        {
            Console.WriteLine($"recieved vote request from {request.Candidate}");

            if (request.Term > this.term)
            {
                Demote(request.Term);
            }

            if (request.Term < this.term || (this.votedFor != null && this.votedFor != request.Candidate) )
            {
                Console.WriteLine(" - voting no");
                return Task.FromResult(new RequestVoteResponse
                {
                    VoteGranted = false,
                    Term = this.term
                });
            }

            Console.WriteLine(" - voting yes");
            votedFor = request.Candidate;
            return Task.FromResult(new RequestVoteResponse
            {
                VoteGranted = true,
                Term = this.term
            });
        }
    }
}
