namespace Orleans.Consensus.Contract
{
    using System;
    using System.Threading.Tasks;

    public interface ITestRaftGrain : IRaftGrain<string>
    {
        Task AddValue(string value);

        Task Crash();

        Task Delay(TimeSpan delay);

        Task<string> GetState();
    }
}