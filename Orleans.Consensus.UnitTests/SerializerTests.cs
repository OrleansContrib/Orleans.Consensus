namespace Orleans.Consensus.UnitTests
{
    using Orleans.Consensus.Contract.Log;
    using Orleans.Consensus.Log;
    using System.IO;
    using Xunit;
    using FluentAssertions;
    using System;
    using ProtoBuf.Meta;

    
    public class TestOperation
    {
        public string StringValue { get; set; }
        public int IntValue { get; set; }
        public bool BoolValue { get; set; }
        public int[] ArrayValue { get; set; }
    }

    public class SerializerTests
    {
        [Fact]
        public void ProtobufSerializerCanSerializeAndDeserialize()
        {
            var model = TypeModel.Create();
            model.Add(typeof(MutableLogEntry<TestOperation>), false).Add(Array.ConvertAll(typeof(MutableLogEntry<TestOperation>).GetProperties(), prop => prop.Name));
            model.Add(typeof(LogEntry<TestOperation>), false).SetSurrogate(typeof(MutableLogEntry<TestOperation>));
            model.Add(typeof(MutableLogEntryId), false).Add(Array.ConvertAll(typeof(LogEntryId).GetProperties(), prop => prop.Name));
            model.Add(typeof(LogEntryId), false).SetSurrogate(typeof(MutableLogEntryId));
            model.Add(typeof(TestOperation), false).Add(Array.ConvertAll(typeof(TestOperation).GetProperties(), prop => prop.Name));

            model.Add(typeof(ServiceConfiguration), false).Add(Array.ConvertAll(typeof(ServiceConfiguration).GetProperties(), prop => prop.Name));

            var serializer = new ProtobufSerializer<LogEntry<TestOperation>>(model);
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
                clone.Operation.StringValue.Should().Be("STRING");
                clone.Operation.BoolValue.Should().BeTrue();
                clone.Operation.IntValue.Should().Be(42);
                clone.Operation.ArrayValue.Should().HaveCount(4);

                // write the clone to the stream (as a second entry)
                clone.Operation.StringValue = "MODIFIED";
                serializer.Serialize(clone, stream);

                // read the stream back, we expect to find 2 entries
                // reading beyond the end of the stream will result in nulls
                stream.Position = 0;
                var clone1 = serializer.Deserialize(stream);
                var clone2 = serializer.Deserialize(stream);
                var clone3 = serializer.Deserialize(stream);
                clone1.Operation.Should().NotBeNull();
                clone2.Operation.Should().NotBeNull();
                clone3.IsOperation.Should().BeFalse();

                clone1.Operation.StringValue.Should().Be("STRING");
                clone2.Operation.StringValue.Should().Be("MODIFIED");
            }

        }

    }
}
