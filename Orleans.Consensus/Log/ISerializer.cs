namespace Orleans.Consensus.Log
{
    using System.IO;

    public interface ISerializer<T>
    {
        void Serialize(T value, Stream stream);
        T Deserialize(Stream stream);
    }
}
