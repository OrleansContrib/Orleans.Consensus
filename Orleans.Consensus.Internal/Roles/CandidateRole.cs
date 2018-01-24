using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Consensus.Roles
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    using Orleans.Consensus.Actors;
    using Orleans.Consensus.Contract;
    using Orleans.Consensus.Contract.Log;
    using Orleans.Consensus.Contract.Messages;
    using Orleans.Consensus.State;
    using Orleans.Consensus.Utilities;

    internal class CandidateRole<TOperation> : ICandidateRole<TOperation>
    {
        private readonly CancellationTokenSource cancellation = new CancellationTokenSource();

        private readonly IGrainFactory grainFactory;

        private readonly IServerIdentity identity;

        private readonly IPersistentLog<TOperation> journal;

        private readonly IRoleCoordinator<TOperation> local;

        private readonly ILogger logger;

        private readonly IMembershipProvider membershipProvider;

        private readonly ReplicaSetOptions replicaSetOptions;

        private readonly IRaftPersistentState persistentState;

        private readonly IRandom random;

        private readonly RegisterTimerDelegate registerTimer;

        private readonly IRaftVolatileState volatileState;

        private IDisposable electionTimer;

        private int votes;

        public CandidateRole(
            IRoleCoordinator<TOperation> local,
            ILogger<CandidateRole<TOperation>> logger,
            IPersistentLog<TOperation> journal,
            IRaftPersistentState persistentState,
            IRaftVolatileState volatileState,
            IRandom random,
            IGrainFactory grainFactory,
            RegisterTimerDelegate registerTimer,
            IServerIdentity identity,
            IMembershipProvider membershipProvider,
            IOptions<ReplicaSetOptions> replicaSetOptions)
        {
            this.local = local;
            this.logger = logger;
            this.journal = journal;
            this.persistentState = persistentState;
            this.volatileState = volatileState;
            this.random = random;
            this.grainFactory = grainFactory;
            this.registerTimer = registerTimer;
            this.identity = identity;
            this.membershipProvider = membershipProvider;
            this.replicaSetOptions = replicaSetOptions.Value;
        }

        public string RoleName => "Candidate";

        public async Task Enter()
        {
            this.logger.LogInformation("Becoming candidate.");

            // There is currently no leader.
            this.volatileState.LeaderId = null;

            // Increment currentTerm and vote for self.
            await this.persistentState.UpdateTermAndVote(this.identity.Id, this.persistentState.CurrentTerm + 1);

            // In the event of a stalemate, re-declare candidacy.
            this.ResetElectionTimer();

            this.RequestVotes().Ignore();
        }

        private int QuorumSize => this.membershipProvider.OtherServers.Count / 2;

        public Task Exit()
        {
            this.logger.LogInformation("Leaving candidate state.");
            this.electionTimer?.Dispose();
            this.cancellation.Cancel();
            return Task.FromResult(0);
        }

        public async Task<RequestVoteResponse> RequestVote(RequestVoteRequest request)
        {
            // If the term of the requester is greater than the term of this instance, step down and handle the
            // message as a follower.
            if (await this.local.StepDownIfGreaterTerm(request))
            {
                return await this.local.Role.RequestVote(request);
            }

            // Candidates vote for themselves and no other.
            return new RequestVoteResponse { VoteGranted = false, Term = this.persistentState.CurrentTerm };
        }

        public async Task<AppendResponse> Append(AppendRequest<TOperation> request)
        {
            // If AppendEntries RPC received from new leader: convert to follower.
            if (request.Term >= this.persistentState.CurrentTerm)
            {
                await this.local.BecomeFollowerForTerm(request.Term);
                return await this.local.Role.Append(request);
            }

            // The requester is from an older term.
            this.logger.LogInformation($"Denying append from {request.Leader}.");
            return new AppendResponse { Success = false, Term = this.persistentState.CurrentTerm };
        }

        public Task<LogEntry<TOperation>[]> ReplicateOperations(TOperation[] operations)
        {
            throw new NotLeaderException(this.volatileState.LeaderId);
        }

        private async Task RequestVotes()
        {
            // Send RequestVote RPCs to all other servers.
            var request = new RequestVoteRequest(
                this.persistentState.CurrentTerm,
                this.identity.Id,
                this.journal.LastLogEntryId);
            var tasks = new List<Task<RequestVoteResponse>>(this.membershipProvider.OtherServers.Count + 1)
            {
                this.cancellation.Token.WhenCanceled<RequestVoteResponse>()
            };

            // Send vote requests to each server.
            foreach (var server in this.membershipProvider.OtherServers)
            {
                var serverGrain = this.grainFactory.GetGrain<IRaftGrain<TOperation>>(server);
                tasks.Add(serverGrain.RequestVote(request));
            }

            // Wait for each server to respond.
            while (!this.cancellation.IsCancellationRequested)
            {
                // Wait for one task to complete and remove it from the list.
                var task = await Task.WhenAny(tasks);
                tasks.Remove(task);

                var response = await task;
                if (await this.local.StepDownIfGreaterTerm(response))
                {
                    return;
                }

                if (!response.VoteGranted)
                {
                    continue;
                }

                this.votes++;
                this.logger.LogInformation(
                    $"Received {this.votes} votes as candidate for term {this.persistentState.CurrentTerm}.");

                // If votes received from majority of servers: become leader (§5.2)
                if (this.votes >= this.QuorumSize)
                {
                    this.logger.LogInformation(
                        $"Becoming leader for term {this.persistentState.CurrentTerm} with {this.votes}/{this.membershipProvider.OtherServers.Count + 1} votes.");
                    await this.local.BecomeLeader();
                    return;
                }
            }
        }

        private void ResetElectionTimer()
        {
            var randomTimeout =
                TimeSpan.FromMilliseconds(
                    this.random.Next(
                        this.replicaSetOptions.MinElectionTimeoutMilliseconds,
                        this.replicaSetOptions.MaxElectionTimeoutMilliseconds));
            this.electionTimer?.Dispose();
            this.electionTimer = this.registerTimer(
                _ => this.local.BecomeCandidate(),
                null,
                randomTimeout,
                randomTimeout);
            this.logger.LogInformation($"Election timer set to fire in {randomTimeout.TotalMilliseconds}ms");
        }
    }
}