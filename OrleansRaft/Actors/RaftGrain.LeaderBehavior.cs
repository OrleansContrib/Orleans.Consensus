using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace OrleansRaft.Actors
{
    using Newtonsoft.Json;

    using Orleans;
    using Orleans.Raft.Contract;
    using Orleans.Raft.Contract.Log;
    using Orleans.Raft.Contract.Messages;

    public abstract partial class RaftGrain<TOperation>
    {
        internal class LeaderBehavior : IRaftMessageHandler<TOperation>
        {
            private readonly TimeSpan heartbeatTimeout = TimeSpan.FromMilliseconds(Settings.HeartbeatTimeoutMilliseconds);

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
                    this.servers[server] = new ServerState { NextIndex = self.Log.LastLogIndex + 1, MatchIndex = 0 };
                }
            }

            public string State => "Leader";

            public Task Enter()
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

                return Task.FromResult(0);
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

                var log = this.self.Log;
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
                        LeaderCommitIndex = this.self.CommitIndex,
                        Term = this.self.State.CurrentTerm,
                        Entries = log.Entries.Skip((int)Math.Max(0, nextIndex - 1)).Take(Settings.MaxLogEntriesPerAppendRequest).ToList()
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
                for (var index = this.self.Log.LastLogIndex; index > this.self.CommitIndex; index--)
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
                        this.self.LogInfo($"Recently committed entries from {this.self.CommitIndex + 1} to {index}.");
                        this.self.CommitIndex = index;
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

    }
}
