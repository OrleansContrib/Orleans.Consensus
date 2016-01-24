namespace OrleansRaft.Actors
{
    using System.Threading.Tasks;

    public class TestRaftGrain : RaftGrain<string>, ITestRaftGrain
    {
        public Task AddValue(string value)
        {
            if (string.IsNullOrEmpty(value)) return Task.FromResult(0);
            return this.AppendEntry(value);
        }
    }
}