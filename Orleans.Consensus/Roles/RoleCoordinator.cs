namespace Orleans.Consensus.Roles
{
    using System;
    using System.Threading.Tasks;

    using Orleans.Consensus.Contract;
    using Orleans.Consensus.Contract.Messages;
    using Orleans.Consensus.State;

    internal class RoleCoordinator<TOperation> : IRoleCoordinator<TOperation>
    {
        private readonly ILogger logger;

        private readonly Func<IFollowerRole<TOperation>> createFollowerRole;

        private readonly Func<ICandidateRole<TOperation>> createCandidateRole;

        private readonly Func<ILeaderRole<TOperation>> createLeaderRole;

        private readonly IRaftPersistentState persistentState;

        public RoleCoordinator(
            IRaftPersistentState persistentState,
            ILogger logger,
            Func<IFollowerRole<TOperation>> createFollowerRole,
            Func<ICandidateRole<TOperation>> createCandidateRole,
            Func<ILeaderRole<TOperation>> createLeaderRole)
        {
            this.persistentState = persistentState;
            this.logger = logger;
            this.createFollowerRole = createFollowerRole;
            this.createCandidateRole = createCandidateRole;
            this.createLeaderRole = createLeaderRole;
        }

        public IRaftRole<TOperation> Role { get; private set; }

        public async Task<bool> StepDownIfGreaterTerm<T>(T message) where T : IMessage
        {
            // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert
            // to follower (§5.1)
            if (message.Term > this.persistentState.CurrentTerm)
            {
                this.logger.LogInfo(
                    $"Stepping down for term {message.Term}, which is greater than current term, {this.persistentState.CurrentTerm}.");
                await ((IRoleCoordinator<TOperation>)this).BecomeFollowerForTerm(message.Term);
                return true;
            }

            return false;
        }

        public Task Initialize()
        {
            // When servers start up, they begin as followers. (§5.2)
            return this.BecomeFollowerForTerm(this.persistentState.CurrentTerm);
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

            await this.TransitionRole(this.createFollowerRole());
        }

        /// <summary>
        /// Transitions into the candidate state.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        public Task BecomeCandidate() => this.TransitionRole(this.createCandidateRole());

        /// <summary>
        /// Transitions into the leader state.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        public Task BecomeLeader() => this.TransitionRole(this.createLeaderRole());

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