namespace Orleans.Consensus.Log
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    using Orleans.Consensus.Contract.Log;

    public static class PersistentLogExtensions
    {
        public static string ProgressString<TOperation>(this IPersistentLog<TOperation> log)
        {
            return $"[{string.Join(", ", log.GetCursor(0).Select(_ => _.Id))}]";
        }
    }

    [Serializable]
    public class InMemoryLog<TOperation> : IPersistentLog<TOperation>
    {
        [NonSerialized]
        private Func<Task> writeCallback;

        internal List<LogEntry<TOperation>> Entries { get; } = new List<LogEntry<TOperation>>();

        public IEnumerable<LogEntry<TOperation>> GetReverseCursor() => Enumerable.Reverse(this.Entries);

        public LogEntryId LastLogEntryId
        {
            get
            {
                if (this.Entries.Count > 0)
                {
                    return this.Entries[this.Entries.Count - 1].Id;
                }

                return default(LogEntryId);
            }
        }

        public LogEntry<TOperation> Get(long index)
        {
            return this.Entries[(int)index - 1];
        }

        public IEnumerable<LogEntry<TOperation>> GetCursor(long fromIndex)
        {
            return this.Entries.Skip((int)fromIndex);
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
            if (this.LastLogEntryId.Index < entryId.Index || entryId.Index == 0)
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
            if (logEntry.Id.Index > this.LastLogEntryId.Index + 1)
            {
                throw new InvalidOperationException(
                    $"Cannot append entry {logEntry.Id} because it is greater than the next index, {this.LastLogEntryId.Index + 1}.");
            }

            if (logEntry.Id.Index == this.LastLogEntryId.Index + 1)
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