using System.IO;

namespace Orleans.Consensus.Contract
{
    public interface ISerializer<T>
    {
        void Serialize(T value, Stream stream);
        T Deserialize(Stream stream);
    }
}
