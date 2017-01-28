using System.Collections.Concurrent;
using Bond;
using Bond.IO.Unsafe;
using Bond.Protocols;
using Orleans.Consensus.Contract;
using OutputBuffer = Bond.IO.Unsafe.OutputBuffer;

namespace Orleans.Consensus.Log
{
    using System;
    using System.IO;

    public class BondSerializer<T> : ISerializer<T>
    {
        private readonly Serializer<CompactBinaryWriter<OutputBuffer>> serializer;
        private readonly Deserializer<CompactBinaryReader<InputStream>> deserializer;

        private readonly ObjectPool<Tuple<CompactBinaryWriter<OutputBuffer>, OutputBuffer>> writers = new ObjectPool<Tuple<CompactBinaryWriter<OutputBuffer>, OutputBuffer>>(
            () =>
            {
                var output = new OutputBuffer();
                var writer = new CompactBinaryWriter<OutputBuffer>(output);
                return Tuple.Create(writer, output);
            });

        public BondSerializer()
        {
            this.serializer = new Serializer<CompactBinaryWriter<OutputBuffer>>(typeof(T));
            this.deserializer = new Deserializer<CompactBinaryReader<InputStream>>(typeof(T));
        }

        void ISerializer<T>.Serialize(T value, Stream stream)
        {
            var pooled = this.writers.GetObject();
            this.serializer.Serialize(value, pooled.Item1);
            var buffer = pooled.Item2.Data;
            stream.Write(buffer.Array, buffer.Offset, buffer.Count);
            pooled.Item2.Position = 0;
            this.writers.PutObject(pooled);
        }

        T ISerializer<T>.Deserialize(Stream stream)
        {
            var reader = new InputStream(stream);
            var result = this.deserializer.Deserialize<T>(new CompactBinaryReader<InputStream>(reader));
            stream.Position = reader.Position;
            return result;
        }
    }

    public class ObjectPool<T>
    {
        private readonly ConcurrentBag<T> objects;
        private readonly Func<T> objectGenerator;

        public ObjectPool(Func<T> objectGenerator)
        {
            if (objectGenerator == null) throw new ArgumentNullException(nameof(objectGenerator));

            this.objects = new ConcurrentBag<T>();
            this.objectGenerator = objectGenerator;
        }

        public T GetObject()
        {
            T item;
            return this.objects.TryTake(out item) ? item : this.objectGenerator();
        }

        public void PutObject(T item)
        {
            this.objects.Add(item);
        }
    }
}
