namespace OrleansRaft.Actors
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    using Orleans;
    using Orleans.Providers;
    using Orleans.Raft.Contract;
    using Orleans.Raft.Contract.Log;
    using Orleans.Raft.Contract.Messages;
    using Orleans.Runtime;

    public interface IRaftVolatileState
    {
        long CommitIndex { get; set; }
        long LastApplied { get; set; }
        string LeaderId { get; set; }

        ICollection<string> servers { get; }

        int GetNextRandom(int minValue, int maxValue);
    }

    public interface IRaftPersistentState
    {
        string VotedFor { get; }
        long CurrentTerm { get; }

        Task Update(string votedFor, long currentTerm);
        }

    public interface IHasLog<TOperation>
    {
        Log<TOperation> Log { get; }
    }

    public interface IRaftServerState<TOperation> : IRaftVolatileState, IRaftPersistentState, IHasLog<TOperation>
    {
        IStateMachine<TOperation> StateMachine { get; }
    }


    internal static class Settings
    {
        // TODO: Use less insanely high values.

        public const int MinElectionTimeoutMilliseconds = 1500;

        public const int MaxElectionTimeoutMilliseconds = 3000;

        public const int HeartbeatTimeoutMilliseconds = 500;

        /// <summary>
        /// The maximum number of log entries which will be included in an append request.
        /// </summary>
        public const int MaxLogEntriesPerAppendRequest = 10;
    }

    [StorageProvider]
    public abstract partial class RaftGrain<TOperation> : Grain<RaftGrainState>, IRaftGrain<TOperation>, IRaftServerState<TOperation>
    {
        private readonly Random random = new Random();

        private IRaftMessageHandler<TOperation> messageHandler;

        // TODO provide a state machine.
        public IStateMachine<TOperation> StateMachine { get; protected set; }
        
        private Logger log;

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

        private async Task ApplyRemainingCommittedEntries()
        {
            if (this.StateMachine != null)
            {
                foreach (var entry in
                    this.Log.Entries.Skip((int)this.LastApplied)
                        .Take((int)(this.CommitIndex - this.LastApplied)))
                {
                    this.LogInfo($"Applying {entry}.");
                    await this.StateMachine.Apply(entry);
                    this.LastApplied = entry.Id.Index;
                }
            }
        }

        public long CommitIndex { get; set; }
        public long LastApplied { get; set; }
        public string LeaderId { get; set; }
        public ICollection<string> servers { get; } = new HashSet<string> { "one", "two", "three" };

        public int GetNextRandom(int minValue, int maxValue) => this.random.Next(minValue, maxValue);

        public string VotedFor => this.State.VotedFor;
        public long CurrentTerm => this.State.CurrentTerm;

        public Task Update(string votedFor, long currentTerm)
        {
            this.State.VotedFor = votedFor;
            this.State.CurrentTerm = currentTerm;
            return this.WriteStateAsync();
        }

        public Log<TOperation> Log { get; } = new Log<TOperation>();


        private void LogInfo(string message)
        {
            this.log.Info(this.GetLogMessage(message));
        }

        private void LogWarn(string message)
        {
            this.log.Warn(-1, this.GetLogMessage(message));
        }

        private void LogVerbose(string message)
        {
            this.log.Verbose(this.GetLogMessage(message));
        }

        private string GetLogMessage(string message)
        {
            return $"[{this.messageHandler.State} in term {this.State.CurrentTerm}, LastLog: ({this.Log.LastLogEntryId})] {message}";
        }
    }
}