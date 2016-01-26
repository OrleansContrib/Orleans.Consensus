namespace Orleans.Consensus.Actors
{
    using System.Threading.Tasks;

    using Orleans.Consensus.Contract.Messages;

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