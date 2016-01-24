namespace OrleansRaft.Log
{
    using System;

    [Serializable]
    public struct LogEntry<TOperation>
    {
        public LogEntry(LogEntryId entryId, TOperation operation)
        {
            this.Id = entryId;
            this.Operation = operation;
        }

        public TOperation Operation { get; }

        public LogEntryId Id { get; }
    }
}