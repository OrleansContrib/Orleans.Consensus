namespace Orleans.Consensus.Roles
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    using Orleans.Consensus.Actors;
    using Orleans.Consensus.Contract;
    using Orleans.Consensus.Contract.Log;
    using Orleans.Consensus.Contract.Messages;
    using Orleans.Consensus.Log;
    using Orleans.Consensus.State;
    using Orleans.Consensus.Utilities;

    internal class FollowerRole<TOperation> : IRaftRole<TOperation>
    {
        private readonly IPersistentLog<TOperation> journal;

        private readonly IRoleCoordinator<TOperation> coordinator;

        private readonly ILogger logger;

        private readonly IRaftPersistentState persistentState;

        private readonly IRandom random;

        private readonly IStateMachine<TOperation> stateMachine;

        private readonly IRaftVolatileState volatileState;

        private IDisposable electionTimer;

        private int messagesSinceLastElectionExpiry;

        private readonly RegisterTimerDelegate registerTimer;

        private readonly ISettings settings;

        public FollowerRole(
            ILogger logger,
            IRoleCoordinator<TOperation> coordinator,
            IPersistentLog<TOperation> journal,
            IStateMachine<TOperation> stateMachine,
            IRaftPersistentState persistentState,
            IRaftVolatileState volatileState,
            IRandom random,
            RegisterTimerDelegate registerTimer,
            ISettings settings)
        {
            this.logger = logger;
            this.coordinator = coordinator;
            this.journal = journal;
            this.stateMachine = stateMachine;
            this.persistentState = persistentState;
            this.volatileState = volatileState;
            this.random = random;
            this.registerTimer = registerTimer;
            this.settings = settings;
        }

        public string RoleName => "Follower";

        public async Task Enter()
        {
            this.logger.LogInfo("Becoming follower.");

            if (this.settings.ApplyEntriesOnFollowers && this.stateMachine != null)
            {
                await this.stateMachine.Reset();
            }

            this.ResetElectionTimer();
        }

        public Task<ICollection<LogEntry<TOperation>>> ReplicateOperations(ICollection<TOperation> operations)
        {
            throw new NotLeaderException(this.volatileState.LeaderId);
        }

        public Task Exit()
        {
            this.logger.LogInfo("Leaving follower state.");
            this.electionTimer?.Dispose();
            return Task.FromResult(0);
        }

        public async Task<RequestVoteResponse> RequestVote(RequestVoteRequest request)
        {
            bool voteGranted;

            this.logger.LogInfo($"RequestVote: {request}");

            // 1. Reply false if term < currentTerm(§5.1)
            if (request.Term < this.persistentState.CurrentTerm)
            {
                this.logger.LogWarn($"Denying vote {request}. Requested term is older than current term.");
                voteGranted = false;
            }
            // 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s
            // log, grant vote (§5.2, §5.4)
            else
            {
                // Check if this server has already voted for another server in the current term.
                var votedInCurrentTerm = request.Term == this.persistentState.CurrentTerm
                                         && !string.IsNullOrEmpty(this.persistentState.VotedFor);
                var votedForAnotherServerInCurrentTerm = votedInCurrentTerm
                                                         && !string.Equals(
                                                             this.persistentState.VotedFor,
                                                             request.Candidate,
                                                             StringComparison.Ordinal);

                if (votedForAnotherServerInCurrentTerm)
                {
                    this.logger.LogWarn($"Denying vote {request}: Already voted for {this.persistentState.VotedFor}");
                    voteGranted = false;
                }
                else if (this.journal.LastLogEntryId > request.LastLogEntryId)
                {
                    this.logger.LogWarn(
                        $"Denying vote {request}: Local log is more up-to-date than candidate's log. "
                        + $"{this.journal.LastLogEntryId} > {request.LastLogEntryId}");
                    voteGranted = false;
                }
                else
                {
                    this.logger.LogInfo(
                        $"Granting vote to {request.Candidate} with last log: {request.LastLogEntryId}.");
                    this.messagesSinceLastElectionExpiry++;

                    voteGranted = true;

                    // If a vote has not yet been granted for this candidate in the current term,
                    // record that the vote is being granted and update the term.
                    if (this.persistentState.VotedFor != request.Candidate
                        || this.persistentState.CurrentTerm != request.Term)
                    {
                        await this.persistentState.UpdateTermAndVote(request.Candidate, request.Term);
                    }
                }
            }

            return new RequestVoteResponse { VoteGranted = voteGranted, Term = this.persistentState.CurrentTerm };
        }

        public async Task<AppendResponse> Append(AppendRequest<TOperation> request)
        {
            bool success;

            // If the request has a higher term than this follower, persist the updated term.
            if (request.Term > this.persistentState.CurrentTerm)
            {
                await this.persistentState.UpdateTermAndVote(null, request.Term);
            }

            // 1. Reply false if term < currentTerm (§5.1)
            if (request.Term < this.persistentState.CurrentTerm)
            {
                this.logger.LogWarn(
                    $"Denying append {request}: Term is older than current term, {this.persistentState.CurrentTerm}.");
                success = false;
            }
            // 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
            else if (!this.journal.Contains(request.PreviousLogEntry))
            {
                this.messagesSinceLastElectionExpiry++;
                this.logger.LogWarn(
                    $"Denying append {request}: Local log does not contain previous entry. "
                    + $"Local: {this.journal.ProgressString()}");
                success = false;
            }
            else
            {
                this.messagesSinceLastElectionExpiry++;

                // Set the current leader, so that clients can be redirected.
                this.volatileState.LeaderId = request.Leader;

                if (request.Entries == null || request.Entries.Count == 0)
                {
                    //this.journalInfo($"heartbeat from {request.Leader}.");
                }
                else
                {
                    // 3. If an existing entry conflicts with a new one (same index but different terms),
                    // delete the existing entry and all that follow it (§5.3)
                    // 4. Append any new entries not already in the log.
                    await this.journal.AppendOrOverwrite(request.Entries);
                    this.logger.LogInfo($"Accepted append. Log is now: {this.journal.ProgressString()}");
                }

                // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
                if (request.LeaderCommitIndex > this.volatileState.CommitIndex)
                {
                    this.volatileState.CommitIndex = Math.Min(
                        request.LeaderCommitIndex,
                        this.journal.LastLogEntryId.Index);

                    if (this.settings.ApplyEntriesOnFollowers)
                    {
                        // If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine(§5.3)
                        await this.ApplyRemainingCommittedEntries();
                    }
                }

                success = true;
            }

            return new AppendResponse
            {
                Success = success,
                Term = this.persistentState.CurrentTerm,
                LastLogEntryId = this.journal.LastLogEntryId
            };
        }

        private Task ElectionTimerExpired()
        {
            // If a message has been received since the last election timeout, reset the timer.
            if (this.messagesSinceLastElectionExpiry == 0)
            {
                this.logger.LogInfo("Election timer expired with no recent messages, becoming candidate.");
                return this.coordinator.BecomeCandidate();
            }

            this.logger.LogVerbose($"Election timer expired. {this.messagesSinceLastElectionExpiry} recent messages. ");
            this.messagesSinceLastElectionExpiry = 0;
            this.ResetElectionTimer();
            return Task.FromResult(0);
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
                _ => this.ElectionTimerExpired(),
                null,
                randomTimeout,
                randomTimeout);

            //this.journalInfo($"Election timer will expire in {randomTimeout.TotalMilliseconds}ms");
        }

        private async Task ApplyRemainingCommittedEntries()
        {
            if (this.stateMachine != null)
            {
                foreach (var entry in
                    this.journal.GetCursor((int)this.volatileState.LastApplied)
                        .Take((int)(this.volatileState.CommitIndex - this.volatileState.LastApplied)))
                {
                    this.logger.LogInfo($"Applying {entry}.");
                    await this.stateMachine.Apply(entry);
                    this.volatileState.LastApplied = entry.Id.Index;
                }
            }
        }
    }
}