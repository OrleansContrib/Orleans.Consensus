using Orleans.Consensus.Contract.Log;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Dapper;

namespace Orleans.Consensus.Log
{
    internal class LogTable
    {
        public long Index { get; set; }
        public long Term { get; set; }
        public byte[] Value { get; set; }

        public LogEntryId ToLogEntryId()
        {
            return new LogEntryId(this.Term, this.Index);
        }

        public LogEntry<TOperation> ToLogEntry<TOperation>(ISerializer<TOperation> serializer)
        {
            return new LogEntry<TOperation>(this.ToLogEntryId(), FromByteArray(this.Value, serializer));
        }

        public static LogTable FromLogEntry<TOperation>(LogEntry<TOperation> entry, ISerializer<TOperation> serializer)
        {
            return new LogTable
            {
                Index = entry.Id.Index,
                Term = entry.Id.Term,
                Value = ToByteArray(entry.Operation, serializer)
            };
        }

        static byte[] ToByteArray<T>(T value, ISerializer<T> serializer)
        {
            using (var memoryStream = new MemoryStream())
            {
                serializer.Serialize(value, memoryStream);
                memoryStream.Position = 0;
                var buffer = new byte[memoryStream.Length];
                memoryStream.Read(buffer, 0, buffer.Length);
                return buffer;
            }
        }

        static T FromByteArray<T>(byte[] value, ISerializer<T> serializer)
        {
            using (var memoryStream = new MemoryStream())
            {
                memoryStream.Write(value, 0, value.Length);
                memoryStream.Position = 0;
                return serializer.Deserialize(memoryStream);
            }

        }

    }


    public class SqliteLog<TOperation> : IPersistentLog<TOperation>, IDisposable
    {
        ISerializer<TOperation> serializer;
        SQLiteConnection connection;

        public SqliteLog(string databaseFilename, ISerializer<TOperation> serializer)
        {
            this.serializer = serializer;
            var createSchema = false;
            if (!File.Exists(databaseFilename))
            {
                SQLiteConnection.CreateFile(databaseFilename);
                createSchema = true;

            }
            connection = new SQLiteConnection($"Data Source={databaseFilename};Version=3;");
            connection.Open();
            if (createSchema)
            {
                foreach (var line in Schema())
                {
                    connection.Execute(line);
                }
            }
        }

        IEnumerable<string> Schema()
        {
            yield return "CREATE TABLE log ([index] INTEGER PRIMARY KEY ASC, [term] INTEGER, [value] BLOB);";
        }

        public LogEntryId LastLogEntryId
        {
            get
            {
                var record = this.connection.Query<LogTable>("select top 1 * from log order by index desc").FirstOrDefault();
                if (null == record) return new LogEntryId(0, 0);
                return record.ToLogEntryId();
            }
        }

        public Task AppendOrOverwrite(IEnumerable<LogEntry<TOperation>> entries)
        {
            if (null == entries) throw new ArgumentNullException(nameof(entries));

            var sortedEntries = entries.OrderBy(x => x.Id.Index).ToArray();
            if (sortedEntries.Length == 0) return Task.FromResult(0);
            var transaction = this.connection.BeginTransaction();
            try
            {
                this.connection.Execute("delete from [log] where [index] >= @MinIndex", new { MinIndex = sortedEntries[0].Id.Index });
                this.connection.Execute("insert into [log] ([index], [term], [value]) values (@Index, @Term, @Value)", entries.Select(x => LogTable.FromLogEntry(x, serializer)));
            }
            catch
            {
                transaction.Rollback();
                throw;                
            }
            finally
            {
                transaction.Commit();
                transaction.Dispose();
            }
            return Task.FromResult(0);
        }

        public bool Contains(LogEntryId entryId)
        {
            return this.connection.Query<int>("select count(*) from [log] where [index] = @Index and [term] = @Term", entryId).First() > 0;
        }

        public void Dispose()
        {
            if (connection!=null) connection.Close();
            connection = null;
        }

        public LogEntry<TOperation> Get(long index)
        {
            var record = this.connection.Query<LogTable>("select * from [log] where [index] = @Index", new { Index = index }).FirstOrDefault();
            if (null == record) throw new ArgumentOutOfRangeException();
            return record.ToLogEntry(this.serializer);
        }

        public IEnumerable<LogEntry<TOperation>> GetCursor(long fromIndex)
        {
            foreach (var record in this.connection.Query<LogTable>("select * from [log] where [index] >= @FromIndex order by [index]", new { FromIndex = fromIndex }))
            {
                yield return record.ToLogEntry(this.serializer);
            }
        }

        public IEnumerable<LogEntry<TOperation>> GetReverseCursor()
        {
            // danger, we're going to load the whole table into memory
            // think about paging this
            foreach (var record in this.connection.Query<LogTable>("select * from [log] order by [index] desc"))
            {
                yield return record.ToLogEntry(this.serializer);
            }
        }
    }
}
