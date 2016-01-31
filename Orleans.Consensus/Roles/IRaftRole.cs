namespace Orleans.Consensus.Roles
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    using Orleans.Consensus.Contract.Log;
    using Orleans.Consensus.Contract.Messages;

    /// <summary>
    /// Defines the operations for a Raft role.
    /// </summary>
    /// <typeparam name="TOperation">The underlying log entry type.</typeparam>
    public interface IRaftRole<TOperation>
    {
        /// <summary>
        /// Gets the name of this role.
        /// </summary>
        string RoleName { get; }

        /// <summary>
        /// Transitions into this role.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        Task Enter();

        /// <summary>
        /// Transitions away from this role.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        Task Exit();

        /// <summary>
        /// Request a vote from this server.
        /// </summary>
        /// <param name="request">The request.</param>
        /// <returns>The response.</returns>
        Task<RequestVoteResponse> RequestVote(RequestVoteRequest request);

        /// <summary>
        /// Append entries to this server's log.
        /// </summary>
        /// <param name="request">The request.</param>
        /// <returns>The response.</returns>
        Task<AppendResponse> Append(AppendRequest<TOperation> request);

        /// <summary>
        /// Replicates the provided operations.
        /// </summary>
        /// <param name="operations">The operations.</param>
        /// <returns>
        /// A collection of <see cref="LogEntry{TOperation}"/> matching the provided <paramref name="operations"/>,
        /// containing the identifier of the entries which will be replicated to followers.
        /// </returns>
        Task<ICollection<LogEntry<TOperation>>> ReplicateOperations(ICollection<TOperation> operations);
    }
}