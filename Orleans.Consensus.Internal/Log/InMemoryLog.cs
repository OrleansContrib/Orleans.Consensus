namespace Orleans.Consensus.Log
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading.Tasks;

    using Orleans.Consensus.Contract.Log;

    [Serializable]
    [SuppressMessage("ReSharper", "ClassWithVirtualMembersNeverInherited.Global",
        Justification = "Test classes override virtual methods.")]
    public class InMemoryLog<TOperation> : IPersistentLog<TOperation>
    {
        [NonSerialized]
        private Func<Task> writeCallback;

        internal List<LogEntry<TOperation>> Entries { get; } = new List<LogEntry<TOperation>>();

        public virtual Func<Task> WriteCallback
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

        public virtual IEnumerable<LogEntry<TOperation>> GetReverseCursor() => Enumerable.Reverse(this.Entries);

        public virtual LogEntryId LastLogEntryId
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

        public virtual LogEntry<TOperation> Get(long index)
        {
            return this.Entries[(int)index - 1];
        }

        public virtual IEnumerable<LogEntry<TOperation>> GetCursor(long fromIndex)
        {
            return this.Entries.Skip((int)fromIndex);
        }

        public virtual bool Contains(LogEntryId entryId)
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

        public virtual Task AppendOrOverwrite(LogEntry<TOperation>[] entries)
        {
            foreach (var entry in entries)
            {
                this.AppendOrOverwrite(entry);
            }

            return this.WriteCallback?.Invoke() ?? Task.FromResult(0);
        }

        private void AppendOrOverwrite(LogEntry<TOperation> logEntry)
        {
            if (logEntry.Id.Index > this.LastLogEntryId.Index + 1)
            {
                throw new InvalidOperationException(
                    $"Cannot append entry {logEntry.Id} because it is greater than the next index, {this.LastLogEntryId.Index + 1}.");
            }


            // 3. If an existing entry conflicts with a new one (same index but different terms),
            // delete the existing entry and all that follow it (§5.3)
            if (this.Entries.Count >= logEntry.Id.Index)
            {
                this.Entries.RemoveRange((int)logEntry.Id.Index - 1, this.Entries.Count - (int)logEntry.Id.Index + 1);
            }

            this.Entries.Add(logEntry);
        }
    }
}