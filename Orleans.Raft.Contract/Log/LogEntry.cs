namespace Orleans.Raft.Contract.Log
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

        /// <summary>
        /// Returns the fully qualified type name of this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> containing a fully qualified type name.
        /// </returns>
        public override string ToString()
        {
            return $"Id: {this.Id}, Operation: {this.Operation}";
        }
    }
}