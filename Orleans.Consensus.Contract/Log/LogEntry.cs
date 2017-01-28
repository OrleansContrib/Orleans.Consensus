namespace Orleans.Consensus.Contract.Log
{
    using System;

    using Orleans.Concurrency;

    [Serializable]
    [Immutable]
    public class ServiceConfiguration
    {
        public ServiceConfiguration(string[] members)
        {
            this.Members = members;
        }

        public string[] Members { get; set;  }
    }

    [Immutable]
    [Serializable]
    public struct LogEntry<TOperation>
    {
        private readonly TOperation operation;
        private readonly ServiceConfiguration configuration;

        public LogEntry(LogEntryId entryId, TOperation operation)
        {
            this.Id = entryId;
            this.operation = operation;
            this.Kind = LogEntryKind.Operation;
            this.configuration = null;
        }

        public LogEntry(LogEntryId entryId, ServiceConfiguration configuration)
        {
            this.Id = entryId;
            this.operation = default(TOperation);
            this.Kind = LogEntryKind.Configuration;
            this.configuration = configuration;
        }

        /// <summary>
        /// The kind of this log entry.
        /// </summary>
        public LogEntryKind Kind { get; }

        /// <summary>
        /// If <see cref="Kind"/> is <see cref="LogEntryKind.Operation"/>, then the operation.
        /// </summary>
        public TOperation Operation
        {
            get
            {
                this.ThrowIfIncorrectKind(LogEntryKind.Operation);
                return this.operation;
            }
        }

        /// <summary>
        /// If <see cref="Kind"/> is <see cref="LogEntryKind.Configuration"/>, then the configuration.
        /// </summary>
        public ServiceConfiguration Configuration
        {
            get
            {
                this.ThrowIfIncorrectKind(LogEntryKind.Configuration);
                return this.configuration;
            }
        }

        public LogEntryId Id { get; }

        private void ThrowIfIncorrectKind(LogEntryKind expectedKind)
        {
            if (this.Kind != expectedKind)
            {
                throw new InvalidOperationException(
                    $"Accessed {this} as though it were of incorrect kind, {expectedKind}.");
            }
        }

        /// <summary>
        /// Returns the fully qualified type name of this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> containing a fully qualified type name.
        /// </returns>
        public override string ToString()
        {
            return $"Entry({this.Id}, Kind: {this.Kind}, Op: {this.operation}, Config: {this.configuration}";
        }

        public bool IsConfiguration => this.Kind == LogEntryKind.Configuration;

        public bool IsOperation => this.Kind == LogEntryKind.Operation;
    }

    /// <summary>
    /// The kind of log entry.
    /// </summary>
    public enum LogEntryKind : byte
    {
        Invalid,
        Operation,
        Configuration
    }

    /// <summary>
    /// A mutable version of <see cref="MutableLogEntry{TOperation}"/>, for use by the ProtoBuf serializer.
    /// </summary>
    public class MutableLogEntry<TOperation>
    {
        public static implicit operator LogEntry<TOperation>(MutableLogEntry<TOperation> surrogate)
        {
            switch (surrogate.Kind)
            {
                case LogEntryKind.Operation:
                    return new LogEntry<TOperation>(surrogate.Id, surrogate.Operation);
                case LogEntryKind.Configuration:
                    return new LogEntry<TOperation>(surrogate.Id, surrogate.Configuration);
                default:
                    throw new ArgumentOutOfRangeException(
                        $"Unsupported kind, {surrogate.Kind} for type {nameof(MutableLogEntry<TOperation>)}.");
            }
        }

        public static implicit operator MutableLogEntry<TOperation>(LogEntry<TOperation> surrogate)
        {
            var result = new MutableLogEntry<TOperation>
            {
                Id = surrogate.Id,
                Kind = surrogate.Kind,
            };

            if (surrogate.IsOperation) result.Operation = surrogate.Operation;
            if (surrogate.IsConfiguration) result.Configuration = surrogate.Configuration;
            return result;
        }

        public TOperation Operation { get; set; }

        public LogEntryId Id { get; set; }

        public LogEntryKind Kind { get; set; }

        public ServiceConfiguration Configuration { get; set; }
    }
}