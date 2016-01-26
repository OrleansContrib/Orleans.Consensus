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

    internal class CandidateRole<TOperation> : IRaftRole<TOperation>
    {
        private readonly CancellationTokenSource cancellation = new CancellationTokenSource();

        private readonly IGrainFactory grainFactory;

        private readonly IServerIdentity identity;

        private readonly IPersistentLog<TOperation> journal;

        private readonly IRoleCoordinator<TOperation> local;

        private readonly ILogger logger;

        private readonly IMembershipProvider membershipProvider;

        private readonly ISettings settings;

        private readonly IRaftPersistentState persistentState;

        private readonly IRandom random;

        private readonly RegisterTimerDelegate registerTimer;

        private readonly IRaftVolatileState volatileState;

        private IDisposable electionTimer;

        private int votes;

        public CandidateRole(
            IRoleCoordinator<TOperation> local,
            ILogger logger,
            IPersistentLog<TOperation> journal,
            IRaftPersistentState persistentState,
            IRaftVolatileState volatileState,
            IRandom random,
            IGrainFactory grainFactory,
            RegisterTimerDelegate registerTimer,
            IServerIdentity identity,
            IMembershipProvider membershipProvider,
            ISettings settings)
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
            this.settings = settings;
        }

        public string RoleName => "Candidate";

        public async Task Enter()
        {
            this.logger.LogInfo("Becoming candidate.");

            // There is currently no leader.
            this.volatileState.LeaderId = null;

            // Increment currentTerm and vote for self.
            await this.persistentState.UpdateTermAndVote(this.identity.Id, this.persistentState.CurrentTerm + 1);

            // In the event of a stalemate, re-declare candidacy.
            this.ResetElectionTimer();

            this.RequestVotes().Ignore();
        }

        public Task Exit()
        {
            this.logger.LogInfo("Leaving candidate state.");
            this.electionTimer?.Dispose();
            this.cancellation.Cancel();
            return Task.FromResult(0);
        }

        public async Task<RequestVoteResponse> RequestVote(RequestVoteRequest request)
        {
            // If the term of the requester is greater than the term of this instance, step down and handle the
            // message as a follower.
            if (await this.local.StepDownIfGreaterTerm(request, this.persistentState))
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
            this.logger.LogInfo($"Denying append from {request.Leader}.");
            return new AppendResponse { Success = false, Term = this.persistentState.CurrentTerm };
        }

        public Task<ICollection<LogEntry<TOperation>>> ReplicateOperations(ICollection<TOperation> operations)
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
            var tasks = new List<Task>(this.membershipProvider.OtherServers.Count + 1)
            {
                this.cancellation.Token.WhenCanceled()
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

                var responseTask = task as Task<RequestVoteResponse>;
                if (responseTask == null)
                {
                    return;
                }

                var response = await responseTask;

                try
                {
                    if (await this.local.StepDownIfGreaterTerm(response, this.persistentState))
                    {
                        return;
                    }

                    if (!response.VoteGranted)
                    {
                        continue;
                    }

                    this.votes++;
                    this.logger.LogInfo(
                        $"Received {this.votes} votes as candidate for term {this.persistentState.CurrentTerm}.");

                    // If votes received from majority of servers: become leader (§5.2)
                    if (this.votes > this.membershipProvider.OtherServers.Count / 2)
                    {
                        this.logger.LogInfo(
                            $"Becoming leader for term {this.persistentState.CurrentTerm} with {this.votes}/{this.membershipProvider.OtherServers.Count + 1} votes.");
                        await this.local.BecomeLeader();
                        return;
                    }
                }
                catch (Exception exception)
                {
                    this.logger.LogWarn($"Exception from {nameof(this.RequestVote)}: {exception}");
                }
            }
        }

        private void ResetElectionTimer()
        {
            var randomTimeout =
                TimeSpan.FromMilliseconds(
                    this.random.Next(
                        this.settings.MinElectionTimeoutMilliseconds,
                        this.settings.MaxElectionTimeoutMilliseconds));
            this.electionTimer?.Dispose();
            this.electionTimer = this.registerTimer(
                _ => this.local.BecomeCandidate(),
                null,
                randomTimeout,
                randomTimeout);
            this.logger.LogInfo($"Election timer set to fire in {randomTimeout.TotalMilliseconds}ms");
        }
    }
}