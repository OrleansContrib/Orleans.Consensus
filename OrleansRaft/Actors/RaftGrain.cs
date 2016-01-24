namespace OrleansRaft.Actors
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    using Newtonsoft.Json;

    using Orleans;
    using Orleans.Providers;
    using Orleans.Runtime;

    using OrleansRaft.Log;
    using OrleansRaft.Messages;

    [StorageProvider]
    public abstract class RaftGrain<TOperation> : Grain<RaftGrainState<TOperation>>, IRaftGrain<TOperation>
    {
        // TODO: Use less insanely high values.
        private const int MinElectionTimeoutMilliseconds = 1500;

        private const int MaxElectionTimeoutMilliseconds = 3000;

        private const int HeartbeatTimeoutMilliseconds = 500;

        private readonly Random random = new Random();

        private IRaftMessageHandler<TOperation> messageHandler;

        // TODO provide a state machine.
        private IStateMachine<TOperation> stateMachine;

        private long commitIndex;

        private long lastApplied;

        // TODO: Use a real list of servers...
        private readonly HashSet<string> servers = new HashSet<string> { "one", "two", "three" };

        private Logger log;

        // TODO: Ensure that this is kept up to date.
        private string leaderId;

        protected string Id => this.GetPrimaryKeyString();
        
        protected Task AppendEntry(TOperation entry)
        {
            return this.messageHandler.ReplicateAndApplyEntries(new List<TOperation> { entry });
        }

        /// <summary>
        /// This method is called at the end of the process of activating a grain.
        ///             It is called before any messages have been dispatched to the grain.
        ///             For grains with declared persistent state, this method is called after the State property has been populated.
        /// </summary>
        public override async Task OnActivateAsync()
        {
            this.log = this.GetLogger($"{this.GetPrimaryKeyString()}");
            this.servers.Remove(this.GetPrimaryKeyString());
            await this.BecomeFollowerForTerm(this.State.CurrentTerm);
            await base.OnActivateAsync();
        }

        private void LogInfo(string message)
        {
            this.log.Info($"[{this.messageHandler.State} CurrTerm: {this.State.CurrentTerm}, LastLog: ({this.State.Log.LastLogEntryId})] {message}");
        }

        private void LogWarn(string message)
        {
            this.log.Warn(-1, message);
        }

        private async Task<bool> StepDownIfGreaterTerm<TMessage>(TMessage message) where TMessage : IMessage
        {
            // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert
            // to follower (§5.1)
            if (message.Term > this.State.CurrentTerm)
            {
                this.log.Info(
                    $"Stepping down for term {message.Term}, which is greater than current term, {this.State.CurrentTerm}.");
                await this.BecomeFollowerForTerm(message.Term);
                return true;
            }

            return false;
        }

        private async Task BecomeFollowerForTerm(long term)
        {
            await this.Become(new FollowerBehavior(this, term));
        }

        private async Task Become(IRaftMessageHandler<TOperation> handler)
        {
            if (this.messageHandler != null)
            {
                await this.messageHandler.Exit();
            }

            this.messageHandler = handler;
            await this.messageHandler.Enter();
        }

        private Task BecomeCandidate() => this.Become(new CandidateBehavior(this));

        private Task BecomeLeader() => this.Become(new LeaderBehavior(this, this.servers));

        public Task<RequestVoteResponse> RequestVote(RequestVoteRequest request) => this.messageHandler.RequestVote(request);

        public Task<AppendResponse> Append(AppendRequest<TOperation> request) => this.messageHandler.Append(request);

        internal class LeaderBehavior : IRaftMessageHandler<TOperation>
        {
            private readonly TimeSpan heartbeatTimeout = TimeSpan.FromMilliseconds(HeartbeatTimeoutMilliseconds);

            private readonly RaftGrain<TOperation> self;

            private readonly Dictionary<string, ServerState> servers = new Dictionary<string, ServerState>();

            private IDisposable heartbeatTimer;

            private DateTime lastMessageSentTime;

            public LeaderBehavior(RaftGrain<TOperation> self, IEnumerable<string> servers)
            {
                this.self = self;

                // Initialize volatile state on leader.
                foreach (var server in servers)
                {
                    this.servers[server] = new ServerState { NextIndex = self.State.Log.LastLogIndex + 1, MatchIndex = 0 };
                }
            }

            public string State => "Leader";

            public Task Enter()
            {
                this.self.LogInfo("Becoming leader.");

                // This node is the leader.
                this.self.leaderId = this.self.Id;

                // Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle
                // periods to prevent election timeouts (§5.2)
                this.SendHeartBeats().Ignore();
                this.heartbeatTimer?.Dispose();
                this.heartbeatTimer = this.self.RegisterTimer(
                    _ => this.SendHeartBeats(),
                    null,
                    this.heartbeatTimeout,
                    this.heartbeatTimeout);

                return Task.FromResult(0);
            }

            public Task SendHeartBeats()
            {
                if (this.lastMessageSentTime + this.heartbeatTimeout > DateTime.UtcNow)
                {
                    // Only send heartbeat if idle.
                    return Task.FromResult(0);
                }

                foreach (var server in this.servers.Keys)
                {
                    if (string.Equals(this.self.Id, server, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    // Send a heartbeat to the server.
                    this.ReplicateAndApplyEntries(null).Ignore();
                }

                return Task.FromResult(0);
            }

            public Task Exit()
            {
                this.self.LogWarn("Leaving leader state.");
                this.heartbeatTimer?.Dispose();
                return Task.FromResult(0);
            }

            public async Task<RequestVoteResponse> RequestVote(RequestVoteRequest request)
            {
                // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
                if (await this.self.StepDownIfGreaterTerm(request))
                {
                    return await this.self.RequestVote(request);
                }

                return new RequestVoteResponse { VoteGranted = false, Term = this.self.State.CurrentTerm };
            }

            public async Task<AppendResponse> Append(AppendRequest<TOperation> request)
            {
                // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
                if (await this.self.StepDownIfGreaterTerm(request))
                {
                    return await this.self.Append(request);
                }

                this.self.LogWarn($"Denying append from {request.Leader}.");
                return new AppendResponse { Success = false, Term = this.self.State.CurrentTerm };
            }
            

            public Task ReplicateAndApplyEntries(List<TOperation> entries)
            {
                if (entries?.Count > 0)
                {
                    this.self.LogInfo($"Sending {entries.Count} entries to {this.servers.Count} servers");
                }
                else
                {
                    //this.self.LogInfo("heartbeat");
                }

                var log = this.self.State.Log;
                if (entries != null)
                {
                    foreach (var entry in entries)
                    {
                        log.AppendOrOverwrite(
                            new LogEntry<TOperation>(
                                new LogEntryId(this.self.State.CurrentTerm, log.LastLogIndex + 1),
                                entry));
                    }
                }

                foreach (var server in this.servers)
                {
                    if (string.Equals(this.self.Id, server.Key, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    var nextIndex = server.Value.NextIndex;
                    var request = new AppendRequest<TOperation>
                    {
                        Leader = this.self.Id,
                        LeaderCommitIndex = this.self.commitIndex,
                        Term = this.self.State.CurrentTerm,
                        Entries = log.Entries.Skip((int)Math.Max(0, nextIndex - 1)).ToList()
                    };

                    if (nextIndex >= 2 && log.Entries.Count > nextIndex - 2)
                    {
                        request.PreviousLogEntry = log.Entries[(int)nextIndex - 2].Id;
                    }

                    this.lastMessageSentTime = DateTime.UtcNow;
                    if (entries?.Count > 0)
                    {
                        this.self.LogInfo(
                            $"Replicating to '{server.Key}': {JsonConvert.SerializeObject(request, Formatting.Indented)}");
                    }
                    
                    this.self.GrainFactory.GetGrain<IRaftGrain<TOperation>>(server.Key)
                        .Append(request)
                        .ContinueWith(
                            async responseTask =>
                            {
                                // Only process messages which ran to completion.
                                if (responseTask.Status == TaskStatus.RanToCompletion)
                                {
                                    var response = responseTask.GetAwaiter().GetResult();
                                    if (response.Success)
                                    {
                                        // The follower's log matches the included logs.
                                        var newMatchIndex = Math.Max(
                                            server.Value.MatchIndex,
                                            request.PreviousLogEntry.Index + (request.Entries?.Count ?? 0));
                                        if (newMatchIndex != server.Value.MatchIndex)
                                        {
                                            this.self.LogInfo(
                                                $"Successfully appended entries {request.PreviousLogEntry.Index + 1} to {newMatchIndex} on {server.Key}");
                                            /*this.self.LogInfo(
                                                $"Updating MatchIndex of {server.Key} from {server.Value.MatchIndex} to {newMatchIndex}");*/
                                            server.Value.MatchIndex = newMatchIndex;

                                            // The send was successful, so the next entry to send is the subsequent entry in the leader's log.
                                            server.Value.NextIndex = newMatchIndex + 1;

                                            await this.UpdateCommittedIndex();
                                        }
                                    }
                                    else
                                    {
                                        this.self.LogWarn($"Received failure response for append call with term of {response.Term}");
                                        if (await this.self.StepDownIfGreaterTerm(response))
                                        {
                                            // This node is no longer a leader, so retire.
                                            return;
                                        }

                                        // Log mismatch, decrement and try again later.
                                        // TODO: This should try again immediately.
                                        var next = server.Value.NextIndex--;
                                        this.self.LogWarn($"Log mismatch must have occured on '{server.Key}', decrementing nextIndex to {next}.");
                                    }
                                }
                            }).Ignore();
                }

                // TODO: return a task which completes when the operation is committed.
                return Task.FromResult(0);
            }

            private Task UpdateCommittedIndex()
            {
                for (var index = this.self.State.Log.LastLogIndex; index > this.self.commitIndex; index--)
                {
                    var votes = 0;
                    foreach (var server in this.servers)
                    {
                        if (server.Value.MatchIndex >= index)
                        {
                            votes++;
                        }
                    }

                    if (votes >= this.QuorumSize)
                    {
                        this.self.LogInfo($"Recently committed entries from {this.self.commitIndex + 1} to {index}.");
                        this.self.commitIndex = index;
                        return this.self.ApplyRemainingCommittedEntries();
                    }
                }

                return Task.FromResult(0);
            }

            private int QuorumSize => (this.servers.Count + 1) / 2;

            internal class ServerState
            {
                public long NextIndex { get; set; }
                public long MatchIndex { get; set; }
            }
        }

        private async Task ApplyRemainingCommittedEntries()
        {
            if (this.stateMachine != null)
            {
                foreach (var entry in
                    this.State.Log.Entries.Skip((int)this.lastApplied - 1)
                        .Take((int)(this.commitIndex - this.lastApplied)))
                {
                    this.LogInfo($"Applying {entry}.");
                    await this.stateMachine.Apply(entry);
                    this.lastApplied = entry.Id.Index;
                }
            }
        }

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
                this.self.leaderId = null;

                // Increment currentTerm and vote for self.
                this.self.State.CurrentTerm = this.self.State.CurrentTerm + 1;
                this.self.State.VotedFor = this.self.Id;
                await this.self.WriteStateAsync();

                // Reset election timer.
                var randomTimeout =
                    TimeSpan.FromMilliseconds(
                        this.self.random.Next(MinElectionTimeoutMilliseconds, MaxElectionTimeoutMilliseconds));
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
                    LastLogEntryId = this.self.State.Log.LastLogEntryId,
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
                throw new NotLeaderException(this.self.leaderId);
            }
        }

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
                throw new NotLeaderException(this.self.leaderId);
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

                this.self.LogInfo(
                    $"Election timer expired. Received {this.messagesSinceLastElectionExpiry} messages since last expiry.");
                this.messagesSinceLastElectionExpiry = 0;
                var randomTimeout =
                    TimeSpan.FromMilliseconds(
                        this.self.random.Next(MinElectionTimeoutMilliseconds, MaxElectionTimeoutMilliseconds));
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
                    this.self.LogInfo($"Denying vote request from {request.Candidate} in old term, {request.Term}, and last log {request.LastLogEntryId}.");
                    voteGranted = false;
                }
                // 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s
                // log, grant vote (§5.2, §5.4)
                else if ((string.IsNullOrEmpty(this.self.State.VotedFor)
                          || string.Equals(this.self.State.VotedFor, request.Candidate, StringComparison.Ordinal))
                         && request.LastLogEntryId >= this.self.State.Log.LastLogEntryId)
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
                    this.self.LogInfo(
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
                    this.self.LogInfo($"Denying append request from {request.Leader} in old term, {request.Term}, and last log {request.PreviousLogEntry}.");
                    success = false;
                }
                // 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
                else if (!this.self.State.Log.Contains(request.PreviousLogEntry))
                {
                    this.self.LogInfo(
                        $"Denying append from {request.Leader} since local log does not contain previous entry {request.PreviousLogEntry}: {JsonConvert.SerializeObject(this.self.State.Log.Entries, Formatting.Indented)}");
                    success = false;
                }
                // 3. If an existing entry conflicts with a new one (same index but different terms),
                // delete the existing entry and all that follow it (§5.3)
                else if (this.self.State.Log.ConflictsWith(request.PreviousLogEntry))
                {
                    this.self.LogInfo(
                        $"Denying append request from {request.Leader} because previous log entry {request.PreviousLogEntry} conflicts with local log: {JsonConvert.SerializeObject(this.self.State.Log.Entries, Formatting.Indented)}");
                    success = false;
                }
                else
                {
                    this.messagesSinceLastElectionExpiry++;

                    // Set the current leader, so that clients can be redirected.
                    this.self.leaderId = request.Leader;

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
                            await this.self.State.Log.AppendOrOverwrite(entry);
                        }
                    }

                    // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
                    if (request.LeaderCommitIndex > this.self.commitIndex)
                    {
                        this.self.commitIndex = Math.Min(request.LeaderCommitIndex, this.self.State.Log.LastLogIndex);

                        this.self.LogInfo($"Committed up to index {this.self.commitIndex}.");

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