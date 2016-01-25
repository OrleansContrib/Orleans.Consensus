namespace Orleans.Consensus.Actors
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    using Orleans.Consensus.Contract.Messages;

    public abstract partial class RaftGrain<TOperation>
    {
        internal class FollowerRole : IRaftRole<TOperation>
        {
            private int messagesSinceLastElectionExpiry;

            private readonly RaftGrain<TOperation> self;

            private IDisposable electionTimer;

            private readonly long term;

            public FollowerRole(RaftGrain<TOperation> self, long term)
            {
                this.self = self;
                this.term = term;
            }

            public string State => "Follower";

            public async Task Enter()
            {
                this.self.LogInfo($"Becoming follower for term {this.term}.");
                await this.UpdateTerm();
                this.ResetElectionTimer();

                if (Settings.ApplyEntriesOnFollowers && this.self.StateMachine!=null)
                {
                    await this.self.StateMachine.Reset();
                }
            }

            public Task ReplicateAndApplyEntries(List<TOperation> entries)
            {
                throw new NotLeaderException(this.self.LeaderId);
            }

            private Task UpdateTerm()
            {
                if (this.self.State.CurrentTerm > this.term)
                {
                    throw new InvalidOperationException(
                        $"{nameof(this.UpdateTerm)} failed precondition check: (current) {this.self.State.CurrentTerm} > (new) {this.term}.");
                }

                if (this.self.State.CurrentTerm != this.term)
                {
                    return this.self.UpdateTermAndVote(null, this.term);
                }

                return Task.FromResult(0);
            }

            private Task ElectionTimerExpired()
            {
                // If a message has been received since the last election timeout, reset the timer.
                if (this.messagesSinceLastElectionExpiry == 0)
                {
                    this.self.LogInfo("Election timer expired with no recent messages, becoming candidate.");
                    return this.self.BecomeCandidate();
                }
                
                this.self.LogVerbose(
                    $"Election timer expired. {this.messagesSinceLastElectionExpiry} recent messages. ");
                this.messagesSinceLastElectionExpiry = 0;
                this.ResetElectionTimer();
                return Task.FromResult(0);
            }

            private void ResetElectionTimer()
            {
                var randomTimeout =
                    TimeSpan.FromMilliseconds(
                        this.self.GetNextRandom(Settings.MinElectionTimeoutMilliseconds, Settings.MaxElectionTimeoutMilliseconds));
                this.electionTimer?.Dispose();
                this.electionTimer = this.self.RegisterTimer(_ => this.ElectionTimerExpired(), null, randomTimeout, randomTimeout);

                //this.self.LogInfo($"Election timer will expire in {randomTimeout.TotalMilliseconds}ms");
            }

            public Task Exit()
            {
                this.self.LogInfo("Leaving follower state.");
                this.electionTimer?.Dispose();
                return Task.FromResult(0);
            }

            public async Task<RequestVoteResponse> RequestVote(RequestVoteRequest request)
            {
                bool voteGranted;

                this.self.LogInfo($"RequestVote: {request}");

                // 1. Reply false if term < currentTerm(§5.1)
                if (request.Term < this.self.State.CurrentTerm)
                {
                    this.self.LogWarn($"Denying vote {request}. Requested term is older than current term.");
                    voteGranted = false;
                }
                // 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s
                // log, grant vote (§5.2, §5.4)
                else
                {
                    // Check if this server has already voted for another server in the current term.
                    var votedInCurrentTerm = request.Term == this.self.State.CurrentTerm
                                             && !string.IsNullOrEmpty(this.self.State.VotedFor);
                    var votedForAnotherServerInCurrentTerm = votedInCurrentTerm
                                                             && !string.Equals(
                                                                 this.self.State.VotedFor,
                                                                 request.Candidate,
                                                                 StringComparison.Ordinal);

                    if (votedForAnotherServerInCurrentTerm)
                    {
                        this.self.LogWarn($"Denying vote {request}: Already voted for {this.self.State.VotedFor}");
                        voteGranted = false;
                    }
                    else if (this.self.Log.LastLogEntryId > request.LastLogEntryId)
                    {
                        this.self.LogWarn(
                            $"Denying vote {request}: Local log is more up-to-date than candidate's log. "
                            + $"{this.self.Log.LastLogEntryId} > {request.LastLogEntryId}");
                        voteGranted = false;
                    }
                    else
                    {
                        this.self.LogInfo(
                            $"Granting vote to {request.Candidate} with last log: {request.LastLogEntryId}.");
                        this.messagesSinceLastElectionExpiry++;

                        voteGranted = true;

                        // Record that the vote is being granted.
                        await this.self.UpdateTermAndVote(request.Candidate, request.Term);
                    }
                }

                return new RequestVoteResponse { VoteGranted = voteGranted, Term = this.self.CurrentTerm };
            }

            public async Task<AppendResponse> Append(AppendRequest<TOperation> request)
            {
                bool success;

                // If the request has a higher term than this follower, persist the updated term.
                if (request.Term > this.self.CurrentTerm)
                {
                    await this.self.UpdateTermAndVote(null, request.Term);
                }

                // 1. Reply false if term < currentTerm (§5.1)
                if (request.Term < this.self.CurrentTerm)
                {
                    this.self.LogWarn(
                        $"Denying append {request}: Term is older than current term, {this.self.CurrentTerm}.");
                    success = false;
                }
                // 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
                else if (!this.self.Log.Contains(request.PreviousLogEntry))
                {
                    this.messagesSinceLastElectionExpiry++;
                    this.self.LogWarn(
                        $"Denying append {request}: Local log does not contain previous entry. "
                        + $"Local: [{string.Join(", ", this.self.Log.Entries.Select(_ => _.Id))}]");
                    success = false;
                }
                // 3. If an existing entry conflicts with a new one (same index but different terms),
                // delete the existing entry and all that follow it (§5.3)
                else if (this.self.Log.ConflictsWith(request.PreviousLogEntry))
                {
                    this.messagesSinceLastElectionExpiry++;
                    this.self.LogWarn(
                        $"Denying append {request}: Previous log entry {request.PreviousLogEntry} conflicts with "
                        + $"local log: [{string.Join(", ", this.self.Log.Entries.Select(_ => _.Id))}]");
                    success = false;
                }
                else
                {
                    this.messagesSinceLastElectionExpiry++;

                    // Set the current leader, so that clients can be redirected.
                    this.self.LeaderId = request.Leader;

                    // 4. Append any new entries not already in the log.
                    if (request.Entries == null || request.Entries.Count == 0)
                    {
                        //this.self.LogInfo($"heartbeat from {request.Leader}.");
                    }
                    else
                    {
                        foreach (var entry in request.Entries)
                        {
                            // TODO: batch writes.
                            await this.self.Log.AppendOrOverwrite(entry);
                        }
                        this.self.LogInfo(
                            $"Accepted append. Log is now: [{string.Join(", ", this.self.Log.Entries.Select(_ => _.Id))}]");
                    }

                    // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
                    if (request.LeaderCommitIndex > this.self.CommitIndex)
                    {
                        this.self.CommitIndex = Math.Min(request.LeaderCommitIndex, this.self.Log.LastLogIndex);
                        
                        if (Settings.ApplyEntriesOnFollowers)
                        {
                            // If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine(§5.3)
                            await this.self.ApplyRemainingCommittedEntries();
                        }
                    }

                    success = true;
                }

                return new AppendResponse
                {
                    Success = success,
                    Term = this.self.CurrentTerm,
                    LastLogEntryId = this.self.Log.LastLogEntryId
                };
            }
        }
    }
}