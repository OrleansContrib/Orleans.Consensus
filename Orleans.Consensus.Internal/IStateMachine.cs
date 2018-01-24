namespace Orleans.Consensus
{
    using System.Threading.Tasks;

    using Orleans.Consensus.Contract.Log;

    public interface IStateMachine<TOperationType>
    {
        Task Reset();

        Task Apply(LogEntry<TOperationType> entry);
    }
}