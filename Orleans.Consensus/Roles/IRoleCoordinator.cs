namespace Orleans.Consensus.Roles
{
    using System.Threading.Tasks;

    using Orleans.Consensus.Contract.Messages;
    using Orleans.Consensus.State;

    public interface IRoleCoordinator<TOperation>
    {
        IRaftRole<TOperation> Role { get; }

        Task BecomeCandidate();

        Task BecomeFollowerForTerm(long term);

        Task BecomeLeader();

        Task<bool> StepDownIfGreaterTerm(IMessage message, IRaftPersistentState state);

        Task Initialize();

        Task Shutdown();
    }
}