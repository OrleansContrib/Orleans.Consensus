namespace Orleans.Consensus.Log
{
    using ProtoBuf.Meta;
    using System;
    using System.IO;
    using System.Threading.Tasks;

    public class ProtobufSerializer<T> : ISerializer<T> where T:new()
    {
        RuntimeTypeModel model;

        public ProtobufSerializer(params Type[] subTypes)
        {
            model = TypeModel.Create();
            foreach (var type in subTypes)
            {
                model.Add(type, false).Add(Array.ConvertAll(type.GetProperties(), prop => prop.Name));
            }
            model.Add(typeof(T), false).Add(Array.ConvertAll(typeof(T).GetProperties(), prop => prop.Name));
            model.Compile();
        }

        public Task<T> Deserialize(Stream stream)
        {
            var value = default(T);
            var result = model.DeserializeWithLengthPrefix(stream, value, typeof(T), ProtoBuf.PrefixStyle.Fixed32, 0);
            return Task.FromResult((T) result);
        }

        public Task Serialize(T value, Stream stream)
        {
            model.SerializeWithLengthPrefix(stream, value, typeof(T), ProtoBuf.PrefixStyle.Fixed32, 0);
            return Task.FromResult(0);
        }
    }
}
