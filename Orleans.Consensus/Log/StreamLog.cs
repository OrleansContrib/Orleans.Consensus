namespace Orleans.Consensus.Log
{
    using Orleans.Consensus.Contract.Log;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;

    public class StreamLog<TOperation> : IPersistentLog<TOperation>, IDisposable
    {
        const int CACHE_SIZE = 1000;

        Stream stream;
        ISerializer<LogEntry<TOperation>> serializer;
        BTree<long, long> bTreeIndex;
        IDictionary<long, LogEntry<TOperation>> cache;

        public StreamLog(Stream stream, ISerializer<LogEntry<TOperation>> serializer)
        {
            if (null == stream) throw new ArgumentNullException(nameof(stream));
            if (null == serializer) throw new ArgumentNullException(nameof(serializer));

            this.stream = stream;
            this.serializer = serializer;
            this.bTreeIndex = new BTree<long, long>();
            this.cache = new Dictionary<long, LogEntry<TOperation>>(CACHE_SIZE);
            BuildIndex();
        }

        void BuildIndex()
        {
            stream.Position = 0;
            var first = true;
            while (stream.Position < stream.Length)
            {
                var position = stream.Position;
                var entry = serializer.Deserialize(stream);
                this.bTreeIndex.Insert(entry.Id.Index, position);
                AddToCache(entry);
                this.LastLogEntryId = entry.Id;
                if (first)
                {
                    this.FirstLogEntryId = entry.Id;
                    first = false;
                }
            }
        }

        void AddToCache(LogEntry<TOperation> entry)
        {
            this.cache.Add(entry.Id.Index, entry);
            while (cache.Count > CACHE_SIZE)
            {
                // remove keys with lower indexes
                cache.Remove(cache.Keys.Min());
            }
        }

        public LogEntryId LastLogEntryId { get; private set; }
        public LogEntryId FirstLogEntryId { get; private set; }

        public async Task AppendOrOverwrite(IEnumerable<LogEntry<TOperation>> entries)
        {
            if (null == entries) throw new ArgumentNullException(nameof(entries));

            var sortedEntries = entries.OrderBy(x => x.Id.Index).ToArray();
            if (sortedEntries.Length == 0) return;

            if (this.LastLogEntryId.Index + 1 < sortedEntries.First().Id.Index) throw new ArgumentOutOfRangeException("entries are missing");

            if (this.LastLogEntryId >= sortedEntries.First().Id)
            {
                // rewind the stream
                Rewind(sortedEntries.First().Id.Index);
            }

            if (stream.Length == 0)
            {
                // stream is empty, so this is the first entry in the stream
                this.FirstLogEntryId = sortedEntries.First().Id;
            }

            foreach (var entry in sortedEntries)
            {
                var position = stream.Position;
                serializer.Serialize(entry, stream);
                await stream.FlushAsync();
                this.bTreeIndex.Insert(entry.Id.Index, position);
                this.AddToCache(entry);
            }

            if (stream.Length > stream.Position)
            {
                // we have reduced the length of the stream by deleting entries, so reclaim the space.
                stream.SetLength(stream.Position);
            }

            this.LastLogEntryId = sortedEntries.Last().Id;
        }

        void Rewind(long index)
        {
            var result = SetStreamPosition(index);
            if (!result) throw new ArgumentOutOfRangeException("cannot rewind stream to earlier position");
            
            // purge entries from the btree
            for (var i = index; i <= this.LastLogEntryId.Index; i++)
            {
                bTreeIndex.Delete(i);
                if (this.cache.ContainsKey(i)) this.cache.Remove(i);
            }

            // warning: this method does not update the LastLogEntryId
        }

        bool SetStreamPosition(long index)
        {
            var indexEntry = this.bTreeIndex.Search(index);
            if (indexEntry != null)
            {
                stream.Position = indexEntry.Pointer;
                return true;
            }
            return false;
        }

        /// <summary>
        /// does an existing entry have a different term?
        /// </summary>
        /// <param name="entryId"></param>
        /// <returns></returns>
        public bool ConflictsWith(LogEntryId entryId)
        {
            if (null == entryId) throw new ArgumentNullException(nameof(entryId));

            // entry doesn't exist yet, so it doesn't conflict
            if (entryId.Index > this.LastLogEntryId.Index || entryId.Index < this.FirstLogEntryId.Index) return false;

            var entry = Get(entryId.Index);
            
            if (entry.Id.Term != entryId.Term) return true;
            return false;
        }

        public bool Contains(LogEntryId entryId)
        {
            if (null == entryId) throw new ArgumentNullException(nameof(entryId));

            if (entryId > this.LastLogEntryId || entryId < this.FirstLogEntryId) return false;

            return this.Get(entryId.Index).Id == entryId;
        }

        public LogEntry<TOperation> Get(long index)
        {
            if (index > this.LastLogEntryId.Index || index < this.FirstLogEntryId.Index) throw new ArgumentOutOfRangeException("outside bounds of index");

            if (this.cache.ContainsKey(index)) return this.cache[index];

            var indexEntry = this.bTreeIndex.Search(index);
            if (null == indexEntry) throw new ArgumentOutOfRangeException("cannot find index");

            stream.Position = indexEntry.Pointer;
            return serializer.Deserialize(stream);
        }

        public IEnumerable<LogEntry<TOperation>> GetCursor(long fromIndex)
        {
            for (var i = fromIndex; i <= this.LastLogEntryId.Index; i++)
            {
                yield return Get(i);
            }
        }

        public IEnumerable<LogEntry<TOperation>> GetReverseCursor()
        {
            if (this.stream.Length == 0) yield break;

            for (var i = this.LastLogEntryId.Index; i >= this.FirstLogEntryId.Index; i--)
            {
                yield return Get(i);
            }
        }

        public void Dispose()
        {
            stream.Dispose();
        }
    }
}
