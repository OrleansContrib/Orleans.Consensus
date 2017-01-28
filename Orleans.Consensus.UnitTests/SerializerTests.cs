using Bond;
using Orleans.Consensus.Contract;

namespace Orleans.Consensus.UnitTests
{
    using Orleans.Consensus.Contract.Log;
    using Orleans.Consensus.Log;
    using System.IO;
    using Xunit;
    using FluentAssertions;

    [Schema]
    public class TestOperation
    {
        [Id(0)]
        public string StringValue { get; set; }

        [Id(1)]
        public int IntValue { get; set; }

        [Id(2)]
        public bool BoolValue { get; set; }

        [Id(3)]
        public int[] ArrayValue { get; set; }
    }

    public class SerializerTests
    {
        [Fact]
        public void BondSerializerCanSerializeAndDeserialize()
        {
            var serializer = new BondSerializer<LogEntry<TestOperation>>();
            TestSerializer(serializer);
        }

        // generic test for any serializer implementation
        private void TestSerializer(ISerializer<LogEntry<TestOperation>> serializer)
        {
            var testMessage = new TestOperation
            {
                StringValue = "STRING",
                BoolValue = true,
                IntValue = 42,
                ArrayValue = new int[] { 2, 4, 6, 8 }
            };
            var testEntry = new LogEntry<TestOperation>(new LogEntryId(1, 2), testMessage);
            using (var stream = new MemoryStream())
            {
                stream.Position.Should().Be(0);
                stream.Length.Should().Be(0);

                // write the object to the stream
                serializer.Serialize(testEntry, stream);

                stream.Position.Should().NotBe(0);
                stream.Length.Should().NotBe(0);

                // read the object from the stream
                stream.Position = 0;
                var clone = serializer.Deserialize(stream);

                stream.Position.Should().NotBe(0);
                clone.Should().NotBeNull();
                clone.Id.Term.Should().Be(1);
                clone.Id.Index.Should().Be(2);
                clone.Operation.Should().NotBeNull();
                var op = clone.Operation.Deserialize();
                op.StringValue.Should().Be("STRING");
                op.BoolValue.Should().BeTrue();
                op.IntValue.Should().Be(42);
                op.ArrayValue.Should().HaveCount(4);

                // write the clone to the stream (as a second entry)
                op.StringValue = "MODIFIED";
                serializer.Serialize(clone, stream);
                serializer.Serialize(clone, stream);

                // read the stream back, we expect to find 2 entries
                // reading beyond the end of the stream will result in nulls
                stream.Position = 0;
                var clone1 = serializer.Deserialize(stream);
                var clone2 = serializer.Deserialize(stream);
                clone1.Operation.Should().NotBeNull();
                clone2.Operation.Should().NotBeNull();

                clone1.Operation.Deserialize().StringValue.Should().Be("STRING");
                clone2.Operation.Deserialize().StringValue.Should().Be("MODIFIED");
            }

        }

    }
}
