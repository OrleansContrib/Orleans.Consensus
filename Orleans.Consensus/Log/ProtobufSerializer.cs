namespace Orleans.Consensus.Log
{
    using ProtoBuf.Meta;
    using System;
    using System.IO;
    using System.Threading.Tasks;

    public class ProtobufSerializer<T> : ISerializer<T> where T:new()
    {
        TypeModel model;

        public ProtobufSerializer(params Type[] subTypes)
        {
            var runtimeModel = TypeModel.Create();
            foreach (var type in subTypes)
            {
                runtimeModel.Add(type, false).Add(Array.ConvertAll(type.GetProperties(), prop => prop.Name));
            }
            runtimeModel.Add(typeof(T), false).Add(Array.ConvertAll(typeof(T).GetProperties(), prop => prop.Name));
            runtimeModel.Compile();
            this.model = runtimeModel;
        }

        public ProtobufSerializer(TypeModel typeModel)
        {
            this.model = typeModel;
        }

        public T Deserialize(Stream stream)
        {
            var value = default(T);
            var result = model.DeserializeWithLengthPrefix(stream, value, typeof(T), ProtoBuf.PrefixStyle.Fixed32, 0);
            return (T) result;
        }

        public void Serialize(T value, Stream stream)
        {
            model.SerializeWithLengthPrefix(stream, value, typeof(T), ProtoBuf.PrefixStyle.Fixed32, 0);
        }
    }
}
