namespace Orleans.Consensus.Roles
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;

    using Autofac;

    using Orleans.Consensus.Actors;
    using Orleans.Consensus.Contract.Messages;
    using Orleans.Consensus.State;

    public class RoleCoordinator<TOperation> : IRoleCoordinator<TOperation>
    {
        private readonly ILifetimeScope container;

        private readonly IServerIdentity identity;

        private readonly ILogger logger;

        private readonly IMembershipProvider membershipProvider;

        private readonly IRaftPersistentState persistentState;

        public RoleCoordinator(
            ILifetimeScope container,
            IRaftPersistentState persistentState,
            ILogger logger,
            IServerIdentity identity,
            IMembershipProvider membershipProvider)
        {
            this.container = container;
            this.persistentState = persistentState;
            this.logger = logger;
            this.identity = identity;
            this.membershipProvider = membershipProvider;
        }

        public IRaftRole<TOperation> Role { get; private set; }

        async Task<bool> IRoleCoordinator<TOperation>.StepDownIfGreaterTerm(
            IMessage message,
            IRaftPersistentState state)
        {
            // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert
            // to follower (§5.1)
            if (message.Term > state.CurrentTerm)
            {
                this.logger.LogInfo(
                    $"Stepping down for term {message.Term}, which is greater than current term, {state.CurrentTerm}.");
                await ((IRoleCoordinator<TOperation>)this).BecomeFollowerForTerm(message.Term);
                return true;
            }

            return false;
        }

        public async Task Initialize()
        {
            if (this.persistentState.CurrentTerm == 0 && this.identity.Id == this.membershipProvider.AllServers.Min())
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
                await this.BecomeFollowerForTerm(this.persistentState.CurrentTerm);
            }
        }

        public Task Shutdown() => this.TransitionRole(null);

        public async Task BecomeFollowerForTerm(long term)
        {
            if (this.persistentState.CurrentTerm > term)
            {
                throw new InvalidOperationException(
                    $"Failed precondition check: (current) {this.persistentState.CurrentTerm} > (new) {term}.");
            }

            if (this.persistentState.CurrentTerm != term)
            {
                await this.persistentState.UpdateTermAndVote(null, term);
            }

            await this.TransitionRole(this.container.Resolve<FollowerRole<TOperation>>());
        }

        /// <summary>
        /// Transitions into the candidate state.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        public Task BecomeCandidate() => this.TransitionRole(this.container.Resolve<CandidateRole<TOperation>>());

        /// <summary>
        /// Transitions into the leader state.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        public Task BecomeLeader() => this.TransitionRole(this.container.Resolve<LeaderRole<TOperation>>());

        private async Task TransitionRole(IRaftRole<TOperation> handler)
        {
            if (this.Role != null)
            {
                await this.Role.Exit();
            }

            this.Role = handler;

            if (this.Role != null)
            {
                await this.Role.Enter();
            }
        }
    }
}