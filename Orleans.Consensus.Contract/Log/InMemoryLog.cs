namespace Orleans.Consensus.Contract.Log
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    [Serializable]
    public class InMemoryLog<TOperation>
    {
        [NonSerialized]
        private Func<Task> writeCallback;

        public List<LogEntry<TOperation>> Entries { get; set; } = new List<LogEntry<TOperation>>();

        public IEnumerable<LogEntry<TOperation>> Reverse() => Enumerable.Reverse(this.Entries);

        public long LastLogIndex => this.Entries.Count;

        public LogEntryId LastLogEntryId
        {
            get
            {
                if (this.Entries.Count > 0)
                {
                    return this.Entries[(int)this.LastLogIndex - 1].Id;
                }

                return default(LogEntryId);
            }
        }

        public bool Contains(LogEntryId entryId)
        {
            // The log starts at index 1, index 0 is implicitly included.
            if (entryId.Index == 0)
            {
                return true;
            }

            if (this.Entries.Count < entryId.Index)
            {
                return false;
            }

            if (this.Entries[(int)entryId.Index - 1].Id != entryId)
            {
                return false;
            }

            return true;
        }

        public bool ConflictsWith(LogEntryId entryId)
        {
            if (this.LastLogEntryId == entryId)
            {
                return false;
            }

            // If the entry is after all current entries, the log does not conflict.
            if (this.LastLogIndex < entryId.Index || entryId.Index == 0)
            {
                return false;
            }

            // If the term for the specified entry index differs, the log conflicts.
            if (this.Entries[(int)entryId.Index - 1].Id.Term != entryId.Term)
            {
                return true;
            }

            return false;
        }

        public Task AppendOrOverwrite(LogEntry<TOperation> logEntry)
        {
            if (logEntry.Id.Index > this.LastLogIndex + 1)
            {
                throw new InvalidOperationException(
                    $"Cannot append entry {logEntry.Id} because it is greater than the next index, {this.LastLogIndex + 1}.");
            }

            if (logEntry.Id.Index == this.LastLogIndex + 1)
            {
                this.Entries.Add(logEntry);
            }
            else
            {
                this.Entries[(int)logEntry.Id.Index - 1] = logEntry;
            }

            return this.WriteCallback?.Invoke() ?? Task.FromResult(0);
        }

        public Func<Task> WriteCallback
        {
            get
            {
                return this.writeCallback;
            }
            set
            {
                this.writeCallback = value;
            }
        }
    }
}