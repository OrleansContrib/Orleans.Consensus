using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace OrleansRaft.Actors
{
    using Newtonsoft.Json;

    using Orleans.Raft.Contract.Messages;

    public abstract  partial class RaftGrain<TOperation>
    {
        internal class FollowerBehavior : IRaftMessageHandler<TOperation>
        {
            private int messagesSinceLastElectionExpiry;

            private readonly RaftGrain<TOperation> self;

            private IDisposable electionTimer;

            private readonly long term;

            public FollowerBehavior(RaftGrain<TOperation> self, long term)
            {
                this.self = self;
                this.term = term;
            }

            public string State => "Follower";

            public async Task Enter()
            {
                this.self.LogInfo($"Becoming follower for term {this.term}.");
                this.messagesSinceLastElectionExpiry = 1;
                await this.UpdateTerm();
                await this.ElectionTimerExpired();
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
                    this.self.State.CurrentTerm = this.term;
                    this.self.State.VotedFor = null;
                    return this.self.WriteStateAsync();
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
                    $"Election timer expired. Received {this.messagesSinceLastElectionExpiry} messages since last expiry.");
                this.messagesSinceLastElectionExpiry = 0;
                var randomTimeout =
                    TimeSpan.FromMilliseconds(
                        this.self.random.Next(Settings.MinElectionTimeoutMilliseconds, Settings.MaxElectionTimeoutMilliseconds));
                this.electionTimer?.Dispose();
                this.electionTimer = this.self.RegisterTimer(
                    _ => this.ElectionTimerExpired(),
                    null,
                    randomTimeout,
                    randomTimeout);

                return Task.FromResult(0);
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

                // 1. Reply false if term < currentTerm(§5.1)
                if (request.Term < this.self.State.CurrentTerm)
                {
                    this.self.LogWarn($"Denying vote request from {request.Candidate} in old term, {request.Term}, and last log {request.LastLogEntryId}.");
                    voteGranted = false;
                }
                // 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s
                // log, grant vote (§5.2, §5.4)
                else if ((string.IsNullOrEmpty(this.self.State.VotedFor)
                          || string.Equals(this.self.State.VotedFor, request.Candidate, StringComparison.Ordinal))
                         && request.LastLogEntryId >= this.self.Log.LastLogEntryId)
                {
                    this.self.LogInfo($"Granting vote to {request.Candidate} with last log: {request.LastLogEntryId}.");
                    this.messagesSinceLastElectionExpiry++;

                    voteGranted = true;

                    // Record that the vote is being granted.
                    this.self.State.CurrentTerm = request.Term;
                    this.self.State.VotedFor = request.Candidate;
                    await this.self.WriteStateAsync();
                }
                else
                {
                    this.self.LogWarn(
                        $"Denying vote request from {request.Candidate} in term {request.Term}, with last log {request.LastLogEntryId}.");
                    voteGranted = false;
                }

                return new RequestVoteResponse { VoteGranted = voteGranted, Term = this.self.State.CurrentTerm };
            }

            public async Task<AppendResponse> Append(AppendRequest<TOperation> request)
            {
                bool success;

                // 1. Reply false if term < currentTerm (§5.1)
                if (request.Term < this.self.State.CurrentTerm)
                {
                    this.self.LogWarn($"Denying append request from {request.Leader} in old term, {request.Term}, and last log {request.PreviousLogEntry}.");
                    success = false;
                }
                // 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
                else if (!this.self.Log.Contains(request.PreviousLogEntry))
                {
                    this.self.LogWarn(
                        $"Denying append from {request.Leader} since local log does not contain previous entry {request.PreviousLogEntry}: {JsonConvert.SerializeObject(this.self.Log.Entries, Formatting.Indented)}");
                    success = false;
                }
                // 3. If an existing entry conflicts with a new one (same index but different terms),
                // delete the existing entry and all that follow it (§5.3)
                else if (this.self.Log.ConflictsWith(request.PreviousLogEntry))
                {
                    this.self.LogWarn(
                        $"Denying append request from {request.Leader} because previous log entry {request.PreviousLogEntry} conflicts with local log: {JsonConvert.SerializeObject(this.self.Log.Entries, Formatting.Indented)}");
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
                        this.self.LogInfo($"Accepting append (prev entry: {request.PreviousLogEntry})");
                        foreach (var entry in request.Entries)
                        {
                            // TODO: batch writes.
                            await this.self.Log.AppendOrOverwrite(entry);
                        }
                    }

                    // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
                    if (request.LeaderCommitIndex > this.self.CommitIndex)
                    {
                        this.self.CommitIndex = Math.Min(request.LeaderCommitIndex, this.self.Log.LastLogIndex);

                        this.self.LogInfo($"Committed up to index {this.self.CommitIndex}.");

                        // If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine(§5.3)
                        await this.self.ApplyRemainingCommittedEntries();
                    }

                    success = true;
                }

                return new AppendResponse { Success = success, Term = this.self.State.CurrentTerm };
            }
        }
    }
}
