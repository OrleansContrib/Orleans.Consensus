namespace Orleans.Consensus.Log
{
    using System.IO;
    using System.Threading.Tasks;

    public interface ISerializer<T>
    {
        void Serialize(T value, Stream stream);
        T Deserialize(Stream stream);
    }
}
