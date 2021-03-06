namespace Orleans.Consensus.Contract.Log
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public interface IPersistentLog<TOperation>
    {
        LogEntryId LastLogEntryId { get; }

        IEnumerable<LogEntry<TOperation>> GetReverseCursor();

        LogEntry<TOperation> Get(long index);

        IEnumerable<LogEntry<TOperation>> GetCursor(long fromIndex);

        bool Contains(LogEntryId entryId);

        Task AppendOrOverwrite(IEnumerable<LogEntry<TOperation>> entries);
    }
}