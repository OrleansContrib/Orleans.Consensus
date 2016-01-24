using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace OrleansRaft.Actors
{
    using Orleans;
    using Orleans.Raft.Contract;
    using Orleans.Raft.Contract.Messages;

    public abstract partial class RaftGrain<TOperation>
    {
        internal class CandidateBehavior : IRaftMessageHandler<TOperation>
        {
            private readonly RaftGrain<TOperation> self;

            private IDisposable electionTimer;

            private int votes;

            public CandidateBehavior(RaftGrain<TOperation> self)
            {
                this.self = self;
            }

            public string State => "Candidate";

            public async Task Enter()
            {
                this.self.LogInfo("Becoming candidate.");

                // There is currently no leader.
                this.self.LeaderId = null;

                // Increment currentTerm and vote for self.
                this.self.State.CurrentTerm = this.self.State.CurrentTerm + 1;
                this.self.State.VotedFor = this.self.Id;
                await this.self.WriteStateAsync();

                // Reset election timer.
                var randomTimeout =
                    TimeSpan.FromMilliseconds(
                        this.self.random.Next(Settings.MinElectionTimeoutMilliseconds, Settings.MaxElectionTimeoutMilliseconds));
                this.electionTimer?.Dispose();
                this.electionTimer = this.self.RegisterTimer(
                    _ => this.self.BecomeCandidate(),
                    null,
                    randomTimeout,
                    TimeSpan.MaxValue);

                // Send RequestVote RPCs to all other servers.
                var request = new RequestVoteRequest
                {
                    Candidate = this.self.Id,
                    LastLogEntryId = this.self.Log.LastLogEntryId,
                    Term = this.self.State.CurrentTerm
                };
                foreach (var server in this.self.servers)
                {
                    if (string.Equals(this.self.Id, server, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    var serverGrain = this.self.GrainFactory.GetGrain<IRaftGrain<TOperation>>(server);
                    serverGrain.RequestVote(request).ContinueWith(this.CountVotes).Ignore();
                }
            }

            private async Task CountVotes(Task<RequestVoteResponse> responseTask)
            {
                if (responseTask.Status == TaskStatus.RanToCompletion)
                {
                    var response = responseTask.GetAwaiter().GetResult();
                    if (response.VoteGranted)
                    {
                        // Safety check.
                        if (response.Term > this.self.State.CurrentTerm)
                        {
                            throw new InvalidOperationException(
                                $"Received vote from follower in a greater term, {response.Term}, to the current term, {this.self.State.CurrentTerm}");
                        }

                        this.votes++;
                        this.self.LogInfo($"Received {this.votes} votes as candidate for term {this.self.State.CurrentTerm}.");

                        // If votes received from majority of servers: become leader (§5.2)
                        if (this.votes > this.self.servers.Count / 2)
                        {
                            this.self.LogInfo(
                                $"Becoming leader for term {this.self.State.CurrentTerm} with {this.votes} votes from {this.self.servers.Count} total servers.");
                            await this.self.BecomeLeader();
                        }
                    }
                    else
                    {
                        await this.self.StepDownIfGreaterTerm(response);
                    }
                }
            }

            public Task Exit()
            {
                this.self.LogInfo("Leaving candidate state.");
                this.electionTimer?.Dispose();
                return Task.FromResult(0);
            }

            public async Task<RequestVoteResponse> RequestVote(RequestVoteRequest request)
            {
                // If the term of the requester is greater than the term of this instance, step down and handle the
                // message as a follower.
                if (await this.self.StepDownIfGreaterTerm(request))
                {
                    return await this.self.RequestVote(request);
                }

                // Candidates vote for themselves and no other.
                return new RequestVoteResponse { VoteGranted = false, Term = this.self.State.CurrentTerm };
            }

            public async Task<AppendResponse> Append(AppendRequest<TOperation> request)
            {
                // If AppendEntries RPC received from new leader: convert to follower.
                if (request.Term >= this.self.State.CurrentTerm)
                {
                    await this.self.BecomeFollowerForTerm(request.Term);
                    return await this.self.Append(request);
                }

                // The requester is from an older term.
                this.self.LogInfo($"Denying append from {request.Leader}.");
                return new AppendResponse { Success = false, Term = this.self.State.CurrentTerm };
            }

            public Task ReplicateAndApplyEntries(List<TOperation> entries)
            {
                throw new NotLeaderException(this.self.LeaderId);
            }
        }
    }
}
