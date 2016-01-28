namespace Orleans.Consensus.Log
{
    using System.IO;
    using System.Threading.Tasks;

    public interface ISerializer<T>
    {
        Task Serialize(T value, Stream stream);
        Task<T> Deserialize(Stream stream);
    }
}
