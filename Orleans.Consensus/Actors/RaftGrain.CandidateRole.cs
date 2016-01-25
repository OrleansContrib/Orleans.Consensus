namespace Orleans.Consensus.Actors
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    using Orleans.Consensus.Contract;
    using Orleans.Consensus.Contract.Messages;

    public abstract partial class RaftGrain<TOperation>
    {
        internal class CandidateRole : IRaftRole<TOperation>
        {
            private readonly RaftGrain<TOperation> self;

            private readonly CancellationTokenSource cancellation = new CancellationTokenSource();

            private IDisposable electionTimer;

            private int votes;

            public CandidateRole(RaftGrain<TOperation> self)
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
                await this.self.UpdateTermAndVote(this.self.Id, this.self.CurrentTerm + 1);

                // In the event of a stalemate, re-declare candidacy.
                this.ResetElectionTimer();

                this.RequestVotes().Ignore();
            }

            private async Task RequestVotes()
            {
                // Send RequestVote RPCs to all other servers.
                var request = new RequestVoteRequest(
                    this.self.State.CurrentTerm,
                    this.self.Id,
                    this.self.Log.LastLogEntryId);
                var tasks = new List<Task>(this.self.OtherServers.Count + 1)
                {
                    this.cancellation.Token.WhenCanceled()
                };

                // Send vote requests to each server.
                foreach (var server in this.self.OtherServers)
                {
                    var serverGrain = this.self.GrainFactory.GetGrain<IRaftGrain<TOperation>>(server);
                    tasks.Add(serverGrain.RequestVote(request));
                }

                // Wait for each server to respond.
                while (!this.cancellation.IsCancellationRequested)
                {
                    // Wait for one task to complete and remove it from the list.
                    var task = await Task.WhenAny(tasks);
                    tasks.Remove(task);

                    var responseTask = task as Task<RequestVoteResponse>;
                    if (responseTask == null)
                    {
                        return;
                    }

                    var response = await responseTask;
                    
                    try
                    {
                        if (await this.self.StepDownIfGreaterTerm(response))
                        {
                            return;
                        }

                        if (!response.VoteGranted)
                        {
                            continue;
                        }

                        this.votes++;
                        this.self.LogInfo(
                            $"Received {this.votes} votes as candidate for term {this.self.State.CurrentTerm}.");

                        // If votes received from majority of servers: become leader (§5.2)
                        if (this.votes > this.self.OtherServers.Count / 2)
                        {
                            this.self.LogInfo(
                                $"Becoming leader for term {this.self.State.CurrentTerm} with {this.votes}/{this.self.OtherServers.Count + 1} votes.");
                            await this.self.BecomeLeader();
                            return;
                        }
                    }
                    catch (Exception exception)
                    {
                        this.self.LogWarn($"Exception from {nameof(this.RequestVote)}: {exception}");
                    }
                }
            }

            private void ResetElectionTimer()
            {
                var randomTimeout =
                    TimeSpan.FromMilliseconds(
                        this.self.GetNextRandom(
                            Settings.MinElectionTimeoutMilliseconds,
                            Settings.MaxElectionTimeoutMilliseconds));
                this.electionTimer?.Dispose();
                this.electionTimer = this.self.RegisterTimer(
                    _ => this.self.BecomeCandidate(),
                    null,
                    randomTimeout,
                    randomTimeout);
                this.self.LogInfo($"Election timer set to fire in {randomTimeout.TotalMilliseconds}ms");
            }

            public Task Exit()
            {
                this.self.LogInfo("Leaving candidate state.");
                this.electionTimer?.Dispose();
                this.cancellation.Cancel();
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
