using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace Orleans.Consensus.Actors
{
    using System;
    using System.Threading.Tasks;
    
    using Orleans.Consensus.Contract;
    using Orleans.Consensus.Contract.Log;
    using Orleans.Consensus.Contract.Messages;
    using Orleans.Consensus.Roles;
    using Orleans.Consensus.State;
    using Orleans.Consensus.Utilities;
    using Orleans.Providers;
    using Orleans.Runtime;

    public delegate IDisposable RegisterTimerDelegate(
        Func<object, Task> callback,
        object state,
        TimeSpan dueTime,
        TimeSpan period);

    [StorageProvider]
    public abstract class RaftGrain<TOperation> : Grain<RaftGrainState<TOperation>>,
        IRaftGrain<TOperation>
    {
        private IRoleCoordinator<TOperation> coordinator;

        private IPersistentLog<TOperation> journal;

        private Logger log;

        private ILogger logger;

        private IRaftPersistentState persistentState;
        private IServiceProvider container;

        public Task<RequestVoteResponse> RequestVote(RequestVoteRequest request)
            => this.coordinator.Role.RequestVote(request);

        public Task<AppendResponse> Append(AppendRequest<TOperation> request) => this.coordinator.Role.Append(request);

        protected abstract IStateMachine<TOperation> GetStateMachine(IServiceProvider context);

        protected Task AppendEntry(TOperation entry)
        {
            return this.coordinator.Role.ReplicateOperations(new[] {entry});
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
            this.State.Log.WriteCallback = this.LogAndWriteJournal;

            // TODO: Get servers from Orleans' membership provider.
            var allServers = new[] {"one", "two", "three"};

            var serviceCollection = new ServiceCollection();

            // TODO: Move these registrations into a module.
            serviceCollection.TryAddSingleton<ReplicaSetOptions, ReplicaSetOptions>();
            serviceCollection.TryAddSingleton<IMembershipProvider>(
                sp =>
                {
                    var result = ActivatorUtilities.CreateInstance<StaticMembershipProvider>(sp);
                    result.SetServers(allServers);
                    return result;
                });
            serviceCollection.TryAddSingleton<IRaftVolatileState, VolatileState>();
            serviceCollection.TryAddSingleton<IRoleCoordinator<TOperation>, RoleCoordinator<TOperation>>();
            serviceCollection.TryAddSingleton<IRandom>(_ => ConcurrentRandom.Instance);

            // Register roles.
            serviceCollection.TryAddTransient<IFollowerRole<TOperation>, FollowerRole<TOperation>>();
            serviceCollection.TryAddTransient<ICandidateRole<TOperation>, CandidateRole<TOperation>>();
            serviceCollection.TryAddTransient<ILeaderRole<TOperation>, LeaderRole<TOperation>>();


            serviceCollection.TryAddSingleton(this.GrainFactory);
            serviceCollection.TryAddSingleton<IServerIdentity>(
                _ => new ServerIdentity
                {
                    Id = this.GetPrimaryKeyString()
                });

            serviceCollection.AddLogging();
            serviceCollection.TryAddSingleton<RegisterTimerDelegate>(this.RegisterTimer);

            // By default, all persistent state is stored using the configured grain state storage provider.
            serviceCollection.TryAddSingleton<IRaftPersistentState>(
                _ => new OrleansStorageRaftPersistentState<TOperation>(this.State, this.LogAndWriteState));
            serviceCollection.TryAddSingleton<IPersistentLog<TOperation>>(_ => this.State.Log);
            ;

            // The consumer typically provides their own state machine, so register the method used to retrieve
            // it.
            serviceCollection.TryAddSingleton(this.GetStateMachine);

            this.container = serviceCollection.BuildServiceProvider();

            // Resolve services.
            this.persistentState = this.container.GetRequiredService<IRaftPersistentState>();
            this.coordinator = this.container.GetRequiredService<IRoleCoordinator<TOperation>>();
            this.journal = this.container.GetRequiredService<IPersistentLog<TOperation>>();
            this.logger = this.container.GetRequiredService<ILogger>();

            await this.coordinator.Initialize();

            await base.OnActivateAsync();
        }

        /// <summary>
        /// This method is called at the beginning of the process of deactivating a grain.
        /// </summary>
        public override async Task OnDeactivateAsync()
        {
            if (this.coordinator != null)
            {
                await this.coordinator.Shutdown();
            }
            
            await base.OnDeactivateAsync();
        }

        private Task LogAndWriteState()
        {
            this.logger.LogInformation(
                $"Writing state: votedFor {this.persistentState.VotedFor}, term: {this.persistentState.CurrentTerm}");
            return this.WriteStateAsync();
        }

        private Task LogAndWriteJournal()
        {
            this.logger.LogInformation($"Writing log: {this.journal.ProgressString()}");
            return this.WriteStateAsync();
        }
    }
}