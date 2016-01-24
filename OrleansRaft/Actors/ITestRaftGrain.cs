namespace OrleansRaft.Actors
{
    using System.Threading.Tasks;

    public interface ITestRaftGrain : IRaftGrain<string>
    {
        Task AddValue(string value);
    }
}