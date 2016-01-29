namespace Orleans.Consensus.Actors
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    using Autofac;

    using Orleans.Consensus.Contract;
    using Orleans.Consensus.Contract.Log;
    using Orleans.Consensus.Contract.Messages;
    using Orleans.Consensus.Log;
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
        private ILifetimeScope container;

        private IRoleCoordinator<TOperation> coordinator;

        private IPersistentLog<TOperation> journal;

        private Logger log;

        private ILogger logger;

        private IRaftPersistentState persistentState;

        public Task<RequestVoteResponse> RequestVote(RequestVoteRequest request)
            => this.coordinator.Role.RequestVote(request);

        public Task<AppendResponse> Append(AppendRequest<TOperation> request) => this.coordinator.Role.Append(request);
        
        // TODO provide a state machine.
        protected abstract IStateMachine<TOperation> GetStateMachine(IComponentContext context);

        protected Task AppendEntry(TOperation entry)
        {
            return this.coordinator.Role.ReplicateOperations(new List<TOperation> { entry });
        }

        private string GetLogMessage(string message)
        {
            return
                $"[{this.coordinator.Role.RoleName} in term {this.persistentState.CurrentTerm}, LastLog: ({this.journal.LastLogEntryId})] {message}";
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
            var allServers = new[] { "one", "two", "three" };

            var applicationContainerBuilder = new ContainerBuilder();

            // TODO: Move these registrations into a module.
            applicationContainerBuilder.RegisterType<Settings>().As<ISettings>().SingleInstance().PreserveExistingDefaults();
            applicationContainerBuilder.RegisterType<StaticMembershipProvider>()
                .OnActivated(_ => _.Instance.SetServers(allServers))
                .InstancePerLifetimeScope()
                .AsImplementedInterfaces()
                .PreserveExistingDefaults();
            applicationContainerBuilder.RegisterType<VolatileState>()
                .SingleInstance()
                .AsImplementedInterfaces()
                .PreserveExistingDefaults();
            applicationContainerBuilder.RegisterType<RoleCoordinator<TOperation>>()
                .InstancePerLifetimeScope()
                .AsImplementedInterfaces()
                .PreserveExistingDefaults();
            applicationContainerBuilder.Register<IRandom>(_ => ConcurrentRandom.Instance)
                .SingleInstance()
                .PreserveExistingDefaults();

            // Register roles.
            applicationContainerBuilder.RegisterType<FollowerRole<TOperation>>().PreserveExistingDefaults();
            applicationContainerBuilder.RegisterType<CandidateRole<TOperation>>().PreserveExistingDefaults();
            applicationContainerBuilder.RegisterType<LeaderRole<TOperation>>().PreserveExistingDefaults();

            var applicationContainer = applicationContainerBuilder.Build();

            // Build the container for this grain's scope.
            this.container = applicationContainer.BeginLifetimeScope(
                builder =>
                {
                    builder.RegisterInstance(this.GrainFactory).SingleInstance().PreserveExistingDefaults();
                    builder.Register<IServerIdentity>(_ => new ServerIdentity { Id = this.GetPrimaryKeyString() })
                        .SingleInstance()
                        .PreserveExistingDefaults();

                    builder.Register(_ => new OrleansLogger(this.log) { FormatMessage = this.GetLogMessage })
                        .SingleInstance()
                        .AsImplementedInterfaces()
                        .PreserveExistingDefaults();

                    builder.RegisterInstance<RegisterTimerDelegate>(this.RegisterTimer)
                        .SingleInstance()
                        .PreserveExistingDefaults();

                    // By default, all persistent state is stored using the configured grain state storage provider.
                    builder.Register<IRaftPersistentState>(
                        _ => new OrleansStorageRaftPersistentState<TOperation>(this.State, this.LogAndWriteState))
                        .SingleInstance()
                        .PreserveExistingDefaults();
                    builder.Register(_ => this.State.Log)
                        .OnActivated(_ => _.Instance.WriteCallback = this.LogAndWriteJournal)
                        .SingleInstance()
                        .As<IPersistentLog<TOperation>>()
                        .PreserveExistingDefaults();
                    
                    // The consumer typically provides their own state machine, so register the method used to retrieve
                    // it.
                    builder.Register(this.GetStateMachine);
                });

            // Resolve services.
            this.persistentState = this.container.Resolve<IRaftPersistentState>();
            this.coordinator = this.container.Resolve<IRoleCoordinator<TOperation>>();
            this.journal = this.container.Resolve<IPersistentLog<TOperation>>();
            this.logger = this.container.Resolve<ILogger>();

            await this.coordinator.Initialize();

            await base.OnActivateAsync();
        }

        /// <summary>
        /// This method is called at the begining of the process of deactivating a grain.
        /// </summary>
        public override async Task OnDeactivateAsync()
        {
            if (this.coordinator != null)
            {
                await this.coordinator.Shutdown();
            }

            this.container.Dispose();
            await base.OnDeactivateAsync();
        }

        private Task LogAndWriteState()
        {
            this.logger.LogInfo(
                $"Writing state: votedFor {this.persistentState.VotedFor}, term: {this.persistentState.CurrentTerm}");
            return this.WriteStateAsync();
        }

        private Task LogAndWriteJournal()
        {
            this.logger.LogInfo($"Writing log: {this.journal.ProgressString()}");
            return this.WriteStateAsync();
        }
    }
}