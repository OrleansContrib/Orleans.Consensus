namespace Orleans.Consensus.Actors
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using Orleans.Consensus.Contract;
    using Orleans.Consensus.Contract.Log;
    using Orleans.Consensus.Contract.Messages;

    public abstract partial class RaftGrain<TOperation>
    {
        internal class LeaderRole : IRaftRole<TOperation>
        {
            private readonly CancellationTokenSource cancellation = new CancellationTokenSource();
            private readonly Stopwatch callTimer = new Stopwatch();
            private readonly TimeSpan heartbeatTimeout = TimeSpan.FromMilliseconds(Settings.HeartbeatTimeoutMilliseconds);

            private readonly RaftGrain<TOperation> self;

            private readonly Dictionary<string, FollowerProgress> servers = new Dictionary<string, FollowerProgress>();

            private IDisposable heartbeatTimer;

            private DateTime lastMessageSentTime;

            private bool madeProgress;

            public LeaderRole(RaftGrain<TOperation> self)
            {
                this.self = self;

                // Initialize volatile state on leader.
                foreach (var server in this.self.OtherServers)
                {
                    this.servers[server] = new FollowerProgress { NextIndex = self.Log.LastLogIndex + 1, MatchIndex = 0 };
                }
            }

            public string State => "Leader";

            public async Task Enter()
            {
                this.self.LogInfo("Becoming leader.");

                // This node is the leader.
                this.self.LeaderId = this.self.Id;

                // Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle
                // periods to prevent election timeouts (§5.2)
                this.heartbeatTimer?.Dispose();
                this.heartbeatTimer = this.self.RegisterTimer(
                    _ => this.SendHeartBeats(),
                    null,
                    TimeSpan.Zero,
                    this.heartbeatTimeout);

                if (this.self.StateMachine != null)
                {
                    await this.self.StateMachine.Reset();
                }
            }

            public Task SendHeartBeats()
            {
                this.self.LogVerbose("heartbeat");
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
                this.cancellation.Cancel();
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

            public async Task ReplicateAndApplyEntries(List<TOperation> entries)
            {
                if (entries?.Count > 0)
                {
                    this.self.LogInfo($"Replicating {entries.Count} entries to {this.servers.Count} servers");
                }

                var log = this.self.Log;
                if (entries != null && entries.Count > 0)
                {
                    foreach (var entry in entries)
                    {
                        await log.AppendOrOverwrite(
                            new LogEntry<TOperation>(
                                new LogEntryId(this.self.State.CurrentTerm, log.LastLogIndex + 1),
                                entry));
                    }

                    this.self.LogInfo($"Leader log is: [{string.Join(", ", this.self.Log.Entries.Select(_ => _.Id))}]");
                }

                var tasks = new List<Task>(this.servers.Count);
                foreach (var server in this.servers)
                {
                    if (string.Equals(this.self.Id, server.Key, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    tasks.Add(this.AppendEntriesOnServer(server.Key, server.Value));
                }

                // TODO: return a task which completes when the operation is committed.
                await Task.WhenAll(tasks);
                if (this.madeProgress)
                {
                    await this.UpdateCommittedIndex();
                    this.madeProgress = false;
                }
            }

            private async Task AppendEntriesOnServer(string serverId, FollowerProgress followerProgress)
            {
                this.callTimer.Restart();
                var serverGrain = this.self.GrainFactory.GetGrain<IRaftGrain<TOperation>>(serverId);
                var log = this.self.Log;
                while (!this.cancellation.IsCancellationRequested)
                {
                    var nextIndex = followerProgress.NextIndex;
                    var request = new AppendRequest<TOperation>
                    {
                        Leader = this.self.Id,
                        LeaderCommitIndex = this.self.CommitIndex,
                        Term = this.self.State.CurrentTerm,
                        Entries =
                            log.Entries.Skip((int)Math.Max(0, nextIndex - 1))
                                .Take(Settings.MaxLogEntriesPerAppendRequest)
                                .ToList()
                    };

                    if (nextIndex >= 2 && log.Entries.Count > nextIndex - 2)
                    {
                        request.PreviousLogEntry = log.Entries[(int)nextIndex - 2].Id;
                    }

                    this.lastMessageSentTime = DateTime.UtcNow;
                    if (request.Entries.Count > 0)
                    {
                        this.self.LogInfo(
                            $"Replicating to '{serverId}': prev: {request.PreviousLogEntry},"
                            + $" entries: [{string.Join(", ", request.Entries.Select(_ => _.Id))}]"
                            + $", next: {nextIndex}, match: {followerProgress.MatchIndex}");
                    }

                    var response = await serverGrain.Append(request);
                    if (response.Success)
                    {
                        // The follower's log matches the included logs.
                        var newMatchIndex = Math.Max(
                            followerProgress.MatchIndex,
                            (long)request.Entries?.LastOrDefault().Id.Index);

                        // The send was successful, so the next entry to send is the subsequent entry in the leader's log.
                        followerProgress.NextIndex = newMatchIndex + 1;

                        // For efficiency, only consider updating the committedIndex if matchIndex has changed.
                        if (newMatchIndex != followerProgress.MatchIndex)
                        {
                            this.self.LogInfo(
                                $"Successfully appended entries {request.Entries?.FirstOrDefault().Id} to {newMatchIndex} on {serverId}");
                            followerProgress.MatchIndex = newMatchIndex;

                            this.madeProgress = true;
                        }

                        return;
                    }

                    this.self.LogWarn($"Received failure response for append call with term of {response.Term}");
                    if (await this.self.StepDownIfGreaterTerm(response))
                    {
                        // This node is no longer a leader, so retire.
                        return;
                    }

                    if (followerProgress.NextIndex > response.LastLogEntryId.Index)
                    {
                        // If the follower is lagging, jump immediately to its last log entry.
                        followerProgress.NextIndex = response.LastLogEntryId.Index;
                        this.self.LogInfo($"Follower's last log is {response.LastLogEntryId}, jumping nextIndex to that index.");
                    }
                    else if (followerProgress.NextIndex > 0)
                    {
                        // Log mismatch, decrement and try again later.
                        --followerProgress.NextIndex;
                        this.self.LogWarn(
                            $"Log mismatch must have occured on '{serverId}', decrementing nextIndex to {followerProgress.NextIndex}.");
                    }
                    
                    // In an attempt to maintain fairness and retain leader status, bail out of catchup if the call has taken
                    // more than the allotted time.
                    if (this.callTimer.ElapsedMilliseconds
                        >= Settings.HeartbeatTimeoutMilliseconds / this.self.OtherServers.Count)
                    {
                        this.self.LogInfo(
                            $"Bailing on '{serverId}' catch-up due to fairness. Elapsed: {this.callTimer.ElapsedMilliseconds}.");
                        break;
                    }
                }
            }

            private Task UpdateCommittedIndex()
            {
                foreach (var entry in this.self.Log.Reverse())
                {
                    if (entry.Id.Term != this.self.CurrentTerm)
                    {
                        // Only log entries from the leader’s current term are committed by counting replicas. See §5.4.2.
                        return Task.FromResult(0);
                    }

                    // Return if the log entry is already committed.
                    if (entry.Id.Index == this.self.CommitIndex)
                    {
                        return Task.FromResult(0);
                    }
                    
                    var index = entry.Id.Index;
                    var replicas = 0;
                    foreach (var server in this.servers)
                    {
                        // If the server is not known to have this log entry, continue to the next server.
                        if (server.Value.MatchIndex < index)
                        {
                            continue;
                        }

                        replicas++;

                        // If a quorum has not yet been reached, continue to the next server.
                        if (replicas < this.QuorumSize)
                        {
                            continue;
                        }

                        // This entry is committed.
                        this.self.LogInfo(
                            $"Recently committed entries from {this.self.CommitIndex} to {index} with {replicas + 1}/{this.servers.Count + 1} replicas.");
                        this.self.CommitIndex = index;
                        return this.self.ApplyRemainingCommittedEntries();
                    }
                }

                return Task.FromResult(0);
            }

            private int QuorumSize => (this.servers.Count + 1) / 2;

            internal class FollowerProgress
            {
                public long NextIndex { get; set; }
                public long MatchIndex { get; set; }
            }
        }

    }
}
