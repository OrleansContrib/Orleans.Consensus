namespace OrleansRaft
{
    using System.Threading.Tasks;

    using Orleans.Raft.Contract.Log;

    public interface IStateMachine<TOperationType>
    {
        Task Reset();

        Task Apply(LogEntry<TOperationType> entry);
    }
}