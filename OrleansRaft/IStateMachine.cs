namespace OrleansRaft
{
    using System.Threading.Tasks;

    using OrleansRaft.Log;

    public interface IStateMachine<TOperationType>
    {
        Task Reset();

        Task Apply(LogEntry<TOperationType> entry);
    }
}