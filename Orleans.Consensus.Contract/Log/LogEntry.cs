namespace Orleans.Consensus.Contract.Log
{
    using System;

    using Orleans.Concurrency;

    [Immutable]
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
            return $"Entry({this.Id}, Op: {this.Operation})";
        }
    }

    /// <summary>
    /// A mutable version of <see cref="MutableLogEntry{TOperation}"/>, for use by the ProtoBuf serializer.
    /// </summary>
    public struct MutableLogEntry<TOperation>
    {
        public static implicit operator LogEntry<TOperation>(MutableLogEntry<TOperation> surrogate)
        {
            return new LogEntry<TOperation>(surrogate.Id, surrogate.Operation);
        }

        public static implicit operator MutableLogEntry<TOperation>(LogEntry<TOperation> surrogate)
        {
            return new MutableLogEntry<TOperation> { Id = surrogate.Id, Operation = surrogate.Operation };
        }

        public TOperation Operation { get; set; }

        public LogEntryId Id { get; set; }
    }
}