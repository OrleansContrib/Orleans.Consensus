using Bond;
using Bond.Expressions;
using Bond.Tag;

namespace Orleans.Consensus.Contract.Log
{
    using System;



    // TODO: Port to Hagar


    [Serializable]
    [Schema]
    public class ServiceConfiguration
    {
        [Id(0)]
        public string[] Members { get; set;  }
    }
    
    [Serializable]
    [Schema]
    public struct LogEntry<TOperation>
    {
        public LogEntry(LogEntryId entryId, TOperation operation)
        {
            this.Id = entryId;
            this.Operation = new Bonded<TOperation>(operation);
            this.Configuration = null;
            this.Kind = LogEntryKind.Operation;
        }

        public LogEntry(LogEntryId entryId, ServiceConfiguration configuration)
        {
            this.Id = entryId;
            this.Operation = null;
            this.Configuration = new Bonded<ServiceConfiguration>(configuration);
            this.Kind = LogEntryKind.Configuration;
        }
        
        /// <summary>
        /// The id.
        /// </summary>
        [Id(0)]
        public LogEntryId Id { get; set; }

        /// <summary>
        /// The kind of this log entry, which informs deserialization.
        /// </summary>
        [Id(1)]
        public LogEntryKind Kind { get; set; }

        [Id(2)]
        public IBonded<TOperation> Operation { get; set; }
        [Id(3)]
        public IBonded<ServiceConfiguration> Configuration { get; set; }
        /*
                public TOperation Operation
                {
                    get
                    {
                        this.ThrowIfIncorrectKind(LogEntryKind.Operation);
                        return this.Payload.Deserialize<TOperation>();
                    }
                }*/
/*        public ServiceConfiguration Configuration
        {
            get
            {
                this.ThrowIfIncorrectKind(LogEntryKind.Configuration);
                return this.Payload.Deserialize<ServiceConfiguration>();
            }
        }*/

        private void ThrowIfIncorrectKind(LogEntryKind requiredKind)
        {
            if (this.Kind != requiredKind)
                throw new InvalidOperationException($"Cannot deserialize entry of kind {this.Kind} as {requiredKind}.");
        }

        /// <summary>
        /// Returns the fully qualified type name of this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> containing a fully qualified type name.
        /// </returns>
        public override string ToString()
        {
            return
                $"Entry({this.Id}, {this.Kind})";
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
}