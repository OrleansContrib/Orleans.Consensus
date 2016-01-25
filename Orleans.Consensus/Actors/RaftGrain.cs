namespace Orleans.Consensus.Actors
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Security.Cryptography;
    using System.Threading.Tasks;

    using Orleans;
    using Orleans.Consensus.Contract;
    using Orleans.Consensus.Contract.Log;
    using Orleans.Consensus.Contract.Messages;
    using Orleans.Providers;
    using Orleans.Runtime;

    public interface IRaftVolatileState
    {
        long CommitIndex { get; set; }
        long LastApplied { get; set; }
        string LeaderId { get; set; }

        ICollection<string> OtherServers { get; }

        int GetNextRandom(int minValue, int maxValue);
    }

    public interface IRaftPersistentState
    {
        string VotedFor { get; }
        long CurrentTerm { get; }

        Task UpdateTermAndVote(string votedFor, long currentTerm);
    }

    public interface IHasLog<TOperation>
    {
        InMemoryLog<TOperation> Log { get; }
    }

    public interface IRaftServerState<TOperation> : IRaftVolatileState, IRaftPersistentState, IHasLog<TOperation>
    {
        IStateMachine<TOperation> StateMachine { get; }
    }


    internal static class Settings
    {
        // TODO: Use less insanely high values.

        public const int MinElectionTimeoutMilliseconds = 600;

        public const int MaxElectionTimeoutMilliseconds = 2 * MinElectionTimeoutMilliseconds;

        public const int HeartbeatTimeoutMilliseconds = MinElectionTimeoutMilliseconds / 3;

        /// <summary>
        /// The maximum number of log entries which will be included in an append request.
        /// </summary>
        public const int MaxLogEntriesPerAppendRequest = 10;

        public static bool ApplyEntriesOnFollowers { get; }= false;
    }

    public interface IRandom
    {
        int Next(int minValue, int maxValue);
    }

    public class ConcurrentRandom : IRandom
    {
        private static readonly RNGCryptoServiceProvider GlobalRandom = new RNGCryptoServiceProvider();

        [ThreadStatic]
        private static Random local;

        public static ConcurrentRandom Instance { get; } = new ConcurrentRandom();

        public int Next(int minValue, int maxValue)
        {
            var inst = local;
            if (inst == null)
            {
                var buffer = new byte[4];
                GlobalRandom.GetBytes(buffer);
                local = inst = new Random(BitConverter.ToInt32(buffer, 0));
            }

            return inst.Next(minValue, maxValue);
        }
    }
    
    [StorageProvider]
    public abstract partial class RaftGrain<TOperation> : Grain<RaftGrainState<TOperation>>,
                                                          IRaftGrain<TOperation>,
                                                          IRaftServerState<TOperation>
    {
        private IRaftRole<TOperation> role;

        // TODO provide a state machine.
        public IStateMachine<TOperation> StateMachine { get; protected set; }

        private Logger log;

        protected string Id => this.GetPrimaryKeyString();

        protected Task AppendEntry(TOperation entry)
        {
            return this.role.ReplicateAndApplyEntries(new List<TOperation> { entry });
        }

        /// <summary>
        /// This method is called at the end of the process of activating a grain.
        ///             It is called before any messages have been dispatched to the grain.
        ///             For grains with declared persistent state, this method is called after the State property has been populated.
        /// </summary>
        public override async Task OnActivateAsync()
        {
            this.log = this.GetLogger($"{this.GetPrimaryKeyString()}");
            this.log.Info("Activating");

            // TODO: Get servers from Orleans' memberhsip provider.
            var allServers = new HashSet<string> { "one", "two", "three" };
            this.OtherServers = new HashSet<string>(allServers);

            this.State.Log.WriteCallback = this.LogAndWriteState;

            if (this.State.CurrentTerm == 0 && this.Id == allServers.Min())
            {
                // As an attempted optimization, immediately become a candidate for the first term if this server has
                // just been initialized.
                // The candidacy will fail quickly if this server is being added to an existing cluster and the server
                // will revert to follower in the updated term.
                await this.BecomeCandidate();
            }
            else
            {
                // When servers start up, they begin as followers. (§5.2)
                await this.BecomeFollowerForTerm(this.State.CurrentTerm);
            }

            this.OtherServers.Remove(this.GetPrimaryKeyString());
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
            await this.TransitionRole(new FollowerRole(this, term));
        }

        private async Task TransitionRole(IRaftRole<TOperation> handler)
        {
            if (this.role != null)
            {
                await this.role.Exit();
            }

            this.role = handler;
            await this.role.Enter();
        }

        /// <summary>
        /// Transitions into the candidate state.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        private Task BecomeCandidate() => this.TransitionRole(new CandidateRole(this));

        /// <summary>
        /// Transitions into the leader state.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        private Task BecomeLeader() => this.TransitionRole(new LeaderRole(this));

        public Task<RequestVoteResponse> RequestVote(RequestVoteRequest request)
            => this.role.RequestVote(request);

        public Task<AppendResponse> Append(AppendRequest<TOperation> request) => this.role.Append(request);

        private async Task ApplyRemainingCommittedEntries()
        {
            if (this.StateMachine != null)
            {
                foreach (var entry in
                    this.Log.Entries.Skip((int)this.LastApplied).Take((int)(this.CommitIndex - this.LastApplied)))
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

        /// <summary>
        /// The collection of servers other than the current server.
        /// </summary>
        public ICollection<string> OtherServers { get; private set; }

        public InMemoryLog<TOperation> Log => this.State.Log;

        public int GetNextRandom(int minValue, int maxValue) => ConcurrentRandom.Instance.Next(minValue, maxValue);

        public string VotedFor => this.State.VotedFor;
        public long CurrentTerm => this.State.CurrentTerm;

        public Task LogAndWriteState()
        {
            var s = this.State;
            this.LogWarn(
                $"Writing state: votedFor {s.VotedFor}, term: {s.CurrentTerm}, log: [{string.Join(", ", s.Log.Entries.Select(_ => _.Id))}]");
            return this.WriteStateAsync();
        }

        public Task UpdateTermAndVote(string votedFor, long currentTerm)
        {
            this.State.VotedFor = votedFor;
            this.State.CurrentTerm = currentTerm;
            return this.LogAndWriteState();
        }

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
            return
                $"[{this.role.State} in term {this.State.CurrentTerm}, LastLog: ({this.Log.LastLogEntryId})] {message}";
        }
    }
}