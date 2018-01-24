using Orleans.Consensus.Contract;

namespace Orleans.Consensus.UnitTests
{
    using Contract.Log;
    using Log;
    using System.IO;
    using Xunit;
    using FluentAssertions;
    using System.Linq;
    using System;
    public class PersistentLogTests
    {
        private static ISerializer<LogEntry<TestOperation>> CreateSerializer()
        {
            return new BondSerializer<LogEntry<TestOperation>>();
        }
        
        [Fact]
        public void StreamLogStoresAndRetrievesLogEntries()
        {
            var serializer = CreateSerializer();

            using (var memoryStream = new MemoryStream())
            {
                var log = new StreamLog<TestOperation>(memoryStream, serializer);
                TestEmptyLog(log);
                TestLog(log);
                TestLog(log); // test again, as it will overwrite the existing entries
            }
        }/*

        [Fact]
        public void SqliteLogStoresAndRetrievesLogEntries()
        {
            var serializer = new BondSerializer<TestOperation>();
            if (File.Exists("test.db")) File.Delete("test.db");

            var log = new SqliteLog<TestOperation>("test.db", serializer);
            TestEmptyLog(log);
            TestLog(log);
            TestLog(log); // test again, as it will overwrite the existing entries
        }*/

        private void TestLog(IPersistentLog<TestOperation> log)
        {
            var operations = new[]
            {
                new LogEntry<TestOperation>(new LogEntryId(1, 1), new TestOperation { StringValue = "operation1" }),
                new LogEntry<TestOperation>(new LogEntryId(1, 2), new TestOperation { StringValue = "operation2" }),
                new LogEntry<TestOperation>(new LogEntryId(1, 3), new TestOperation { StringValue = "operation3" })
            };

            log.AppendOrOverwrite(operations).Wait();

            log.Contains(operations[0].Id).Should().BeTrue();
            log.Contains(operations[1].Id).Should().BeTrue();
            log.Contains(operations[2].Id).Should().BeTrue();

            log.Contains(new LogEntryId(1,4)).Should().BeFalse();
            log.Contains(new LogEntryId(2, 1)).Should().BeFalse();
            
            var entry2 = log.Get(2);
            entry2.Id.Should().Be(new LogEntryId(1, 2));
            entry2.Operation.Deserialize().StringValue.Should().Be("operation2");

            var entries = log.GetCursor(2).ToArray();
            entries.Should().HaveCount(2);
            entries[0].Id.Should().Be(new LogEntryId(1, 2));
            entries[1].Id.Should().Be(new LogEntryId(1, 3));

            var reverseEntries = log.GetReverseCursor().ToArray();
            reverseEntries.Should().HaveCount(3);
            reverseEntries[0].Id.Should().Be(new LogEntryId(1, 3));
            reverseEntries[1].Id.Should().Be(new LogEntryId(1, 2));
            reverseEntries[2].Id.Should().Be(new LogEntryId(1, 1));
        }

        private void TestEmptyLog(IPersistentLog<TestOperation> log)
        {

            log.Contains(new LogEntryId(1, 4)).Should().BeFalse();

            Assert.Throws<ArgumentOutOfRangeException>(() => log.Get(2));

            var entries = log.GetCursor(2).ToArray();
            entries.Should().HaveCount(0);

            var reverseEntries = log.GetReverseCursor().ToArray();
            reverseEntries.Should().HaveCount(0);
        }

        [Fact]
        private void StreamLogCanOpenAnExistingStream()
        {
            var serializer = CreateSerializer();
            using (var memoryStream = new MemoryStream())
            {
                var log1 = new StreamLog<TestOperation>(memoryStream, serializer);
                TestLog(log1);

                var log2 = new StreamLog<TestOperation>(memoryStream, serializer);

                var entries = log2.GetCursor(1).ToArray();
                entries.Should().HaveCount(3);
                entries[0].Id.Should().Be(new LogEntryId(1, 1));
                entries[1].Id.Should().Be(new LogEntryId(1, 2));
                entries[2].Id.Should().Be(new LogEntryId(1, 3));
            }
        }
    }
}
