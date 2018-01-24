namespace Orleans.Consensus.Roles
{
    using System.Threading.Tasks;

    using Orleans.Consensus.Contract.Messages;

    public interface IRoleCoordinator<TOperation>
    {
        /// <summary>
        /// Gets the current role.
        /// </summary>
        IRaftRole<TOperation> Role { get; }

        /// <summary>
        /// Transitions into the <see cref="ICandidateRole{TOperation}"/> role.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        Task BecomeCandidate();

        /// <summary>
        /// Transitions into the <see cref="IFollowerRole{TOperation}"/> role for the provided <paramref name="term"/>.
        /// </summary>
        /// <param name="term">
        /// The term.
        /// </param>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        Task BecomeFollowerForTerm(long term);

        /// <summary>
        /// Transitions into the <see cref="ILeaderRole{TOperation}"/> role.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        Task BecomeLeader();

        /// <summary>
        /// Steps down to the <see cref="IFollowerRole{TOperation}"/> if the provided <paramref name="message"/> has
        /// a greater term.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        Task<bool> StepDownIfGreaterTerm<T>(T message) where T : IMessage;

        /// <summary>
        /// Initializes this instance.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        Task Initialize();

        /// <summary>
        /// Transitions out of the current role.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        Task Shutdown();
    }
}